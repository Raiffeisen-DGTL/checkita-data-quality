package ru.raiffeisen.checkita.readers

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Sources._
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.connections.jdbc.JdbcConnection
import ru.raiffeisen.checkita.connections.kafka.KafkaConnection
import ru.raiffeisen.checkita.core.Source
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema
import ru.raiffeisen.checkita.utils.Common.paramsSeqToMap
import ru.raiffeisen.checkita.utils.ResultUtils._

import java.io.FileNotFoundException
import scala.annotation.tailrec
import scala.util.Try

object SourceReaders {


  /**
   * Base source reader trait
   * @tparam T Type of source configuration
   */
  sealed trait SourceReader[T <: SourceConfig] {

    /**
     * Wraps spark dataframe into Source instance.
     * 
     * @param config Source configuration
     * @param df Spark Dataframe
     * @return Source
     */
    protected def toSource(config: T, df: DataFrame): Source = 
      Source(config.id.value, df, config.keyFields.map(_.value))
    
    /**
     * Tries to read source given the source configuration.
     * @param config Source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: T)(implicit settings: AppSettings,
                             spark: SparkSession,
                             fs: FileSystem,
                             schemas: Map[String, SourceSchema],
                             connections: Map[String, DQConnection]): Source

    /**
     * Safely reads source given source configuration.
     * @param config Source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Either a valid Source or a list of source reading errors.
     */
    def read(config: T)(implicit settings: AppSettings,
                        spark: SparkSession,
                        fs: FileSystem,
                        schemas: Map[String, SourceSchema],
                        connections: Map[String, DQConnection]): Result[Source] =
      Try(tryToRead(config)).toResult(
        preMsg = s"Unable to read source '${config.id.value}' due to following error: "
      )
          
  }
  
  /**
   * Table source reader: reads source from JDBC Connection (Postgres, Oracle, etc)
   * @note In order to read table source it is required to provided map of valid connections
   */
  implicit object TableSourceReader extends SourceReader[TableSourceConfig] {

    /**
     * Tries to read table source given the source configuration.
     * @param config Table source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: TableSourceConfig)(implicit settings: AppSettings,
                                             spark: SparkSession,
                                             fs: FileSystem,
                                             schemas: Map[String, SourceSchema],
                                             connections: Map[String, DQConnection]): Source = {
      val conn = connections.getOrElse(config.connection.value, throw new NoSuchElementException(
        s"JDBC connection with id = '${config.connection.value}' not found."
      ))
      
      require(conn.isInstanceOf[JdbcConnection[_]], s"Table source '${config.id.value}' refers to non-Jdbc connection.")
      
      val df = conn.asInstanceOf[JdbcConnection[_]].loadDataframe(config)
      toSource(config, df)
    }
  }

  /**
   * Kafka source reader: reads topic from Kafka broker
   * @note In order to read kafka source it is required to provided map of valid connections
   */
  implicit object KafkaSourceReader extends SourceReader[KafkaSourceConfig] {

    /**
     * Tries to read kafka source given the source configuration.
     * @param config Kafka source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: KafkaSourceConfig)(implicit settings: AppSettings,
                                             spark: SparkSession,
                                             fs: FileSystem,
                                             schemas: Map[String, SourceSchema],
                                             connections: Map[String, DQConnection]): Source = {

      val conn = connections.getOrElse(config.connection.value, throw new NoSuchElementException(
        s"Kafka connection with id = '${config.connection.value}' not found."
      ))

      require(conn.isInstanceOf[KafkaConnection], 
        s"Kafka source '${config.id.value}' refers to not a Kafka connection.")

      val df = conn.asInstanceOf[KafkaConnection].loadDataframe(config)
      toSource(config, df)
    }
  }

  /**
   * Hive source reader: reads table from Hive.
   * Can also read only required partitions from table given the list partition columns and their values to read.
   */
  implicit object HiveSourceReader extends SourceReader[HiveSourceConfig] {

    /**
     * Tries to read hive source given the source configuration.
     * @param config Hive source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: HiveSourceConfig)(implicit settings: AppSettings,
                                            spark: SparkSession,
                                            fs: FileSystem,
                                            schemas: Map[String, SourceSchema],
                                            connections: Map[String, DQConnection]): Source = {
      val tableName = s"${config.schema.value}.${config.table.value}"
      val preDf = spark.read.table(tableName)
      val df = if (config.partitions.nonEmpty) {
        preDf.filter(
          config.partitions.map(p => col(p.name.value).isin(p.values.map(_.value))).reduce(_ && _)
        )
      } else preDf
      toSource(config, df)
    }
  }

  /**
   * Fixed file source reader: reads fixed-width file provided with schema ID corresponding to this file contents.
   * @note In order to read fixed file source it is required to provide map of source schemas.
   */  
  implicit object FixedFileSourceReader extends SourceReader[FixedFileSourceConfig] {

    /**
     * Transform sequence of column widths into sequence of substring indices to extract column from row-string.
     * @param widths Column widths
     * @param positions Column substring indices accumulator
     * @return Column substring indices
     */
    @tailrec
    private def widthsToPositions(widths: Seq[Int], positions: Seq[(Int, Int)] = Seq.empty): Seq[(Int, Int)] =
      if (widths.isEmpty) positions.reverse else {
        val curWidth = widths.head
        val (_, prevEnd) = Try(positions.head).getOrElse((0, 0))
        widthsToPositions(widths.tail, (prevEnd, prevEnd + curWidth) +: positions)
      }

    /**
     * Parses row string into Spark Row
     * @param x Row String
     * @param widths Sequence of column widths in order
     * @return Parsed row
     */
    private def getRow(x: String, widths: Seq[Int]) = Row.fromSeq(
      widthsToPositions(widths).map{
        case (p1, p2) => Try(x.substring(p1, p2)).getOrElse(null)
      }
    )

    /**
     * Tries to read fixed file source given the source configuration.
     * @param config Fixed file source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: FixedFileSourceConfig)(implicit settings: AppSettings,
                                                 spark: SparkSession,
                                                 fs: FileSystem,
                                                 schemas: Map[String, SourceSchema],
                                                 connections: Map[String, DQConnection]): Source = {

      val sourceSchema = schemas.getOrElse(config.schema.value, throw new NoSuchElementException(
        s"Schema with id = '${config.schema.value}' not found."
      ))
      val allStringSchema = StructType(sourceSchema.schema.map(
        col => StructField(col.name, StringType, nullable = true)
      ))
      
      val rdd = if (fs.exists(new Path(config.path.value)))
          spark.sparkContext.textFile(config.path.value).map(r => getRow(r, sourceSchema.columnWidths))
        else throw new FileNotFoundException(s"Fixed-width text file not found: ${config.path.value}")
      val df = spark.createDataFrame(rdd, allStringSchema).select(
        sourceSchema.schema.map(f => col(f.name).cast(f.dataType)) :_*
      )
      
      toSource(config, df)
    }
  }

  /**
   * Delimited file source reader: reads delimited (csv, tsv, etc.) file.
   * Schema may be inferred from file header or provided explicitly in job configuration file and
   * referenced in this source by its ID.
   * @note In order to read delimited file source it is required to provide map of source schemas
   */
  implicit object DelimitedFileSourceReader extends SourceReader[DelimitedFileSourceConfig] {

    /**
     * Tries to read delimited file source given the source configuration.
     * @param config Delimited file source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: DelimitedFileSourceConfig)(implicit settings: AppSettings,
                                                     spark: SparkSession,
                                                     fs: FileSystem,
                                                     schemas: Map[String, SourceSchema],
                                                     connections: Map[String, DQConnection]): Source = {
      val preDf = spark.read.format("csv")
        .option("sep", config.delimiter.value)
        .option("quote", config.quote.value)
        .option("escape", config.escape.value)
        .option("mode", "FAILFAST")
      
      val df = if (fs.exists(new Path(config.path.value))) {
        (config.header, config.schema.map(_.value)) match {
          case (true, None) => preDf.option("header", value = true).load(config.path.value)
          case (false, Some(schema)) =>
            val sourceSchema = schemas.getOrElse(schema, throw new NoSuchElementException(
              s"Schema with id = '$schema' not found."
            ))
            preDf.schema(sourceSchema.schema).load(config.path.value)
          case _ => throw new IllegalArgumentException(
            "For delimited file sources schema must either be read from header or from explicit schema but not from both."
          )
        }
      } else throw new FileNotFoundException(s"Delimited text file not found: ${config.path.value}")
      
      toSource(config, df)
    }
  }

  /**
   * Avro file source reader: reads avro file with optional explicit schema.
   * @note In order to read avro file source it is required to provide map of source schemas.
   */
  implicit object AvroFileSourceReader extends SourceReader[AvroFileSourceConfig] {

    /**
     * Tries to read avro file source given the source configuration.
     * @param config Avro file source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: AvroFileSourceConfig)(implicit settings: AppSettings,
                                                spark: SparkSession,
                                                fs: FileSystem,
                                                schemas: Map[String, SourceSchema],
                                                connections: Map[String, DQConnection]): Source = {
      val preDf = spark.read.format("avro")
      
      val df =  if (fs.exists(new Path(config.path.value))) {
        config.schema.map(_.value) match {
          case Some(schema) =>
            val sourceSchema = schemas.getOrElse(schema, throw new NoSuchElementException(
              s"Schema with id = '$schema' not found."
            ))
            preDf.option("avroSchema", sourceSchema.toAvroSchema.toString).load(config.path.value)
          case None => preDf.load(config.path.value)
        }
      } else throw new FileNotFoundException(s"Avro file not found: ${config.path.value}")
      
      toSource(config, df)
    }
  }

  /**
   * Parquet file source reader: reads parquet files.
   */
  implicit object ParquetFileSourceReader extends SourceReader[ParquetFileSourceConfig] {

    /**
     * Tries to read parquet file source given the source configuration.
     * @param config Parquet file source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: ParquetFileSourceConfig)(implicit settings: AppSettings,
                                                   spark: SparkSession,
                                                   fs: FileSystem,
                                                   schemas: Map[String, SourceSchema],
                                                   connections: Map[String, DQConnection]): Source = 
      if (fs.exists(new Path(config.path.value)))
        toSource(config, spark.read.parquet(config.path.value))
      else throw new FileNotFoundException(s"Parquet file not found: ${config.path.value}")
  }

  /**
   * Orc file source reader: reads orc files.
   */
  implicit object OrcFileSourceReader extends SourceReader[OrcFileSourceConfig] {

    /**
     * Tries to read orc file source given the source configuration.
     * @param config Orc file source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: OrcFileSourceConfig)(implicit settings: AppSettings,
                                               spark: SparkSession,
                                               fs: FileSystem,
                                               schemas: Map[String, SourceSchema],
                                               connections: Map[String, DQConnection]): Source =
      if (fs.exists(new Path(config.path.value)))
        toSource(config, spark.read.orc(config.path.value))
      else throw new FileNotFoundException(s"ORC file not found: ${config.path.value}")
  }

  implicit object CustomSourceReader extends SourceReader[CustomSource] {
    /**
     * Tries to read source given the source configuration.
     *
     * @param config      Source configuration
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: CustomSource)(implicit settings: AppSettings,
                                        spark: SparkSession,
                                        fs: FileSystem,
                                        schemas: Map[String, SourceSchema],
                                        connections: Map[String, DQConnection]): Source = {
      val readOptions = paramsSeqToMap(config.options.map(_.value))
      val sparkReaderInit = spark.read.format(config.format.value).options(readOptions)
      // if schema is provided use it explicitly in DataFrame reader:
      val sparkReader = config.schema.map(_.value) match {
        case Some(schema) =>
          val sourceSchema = schemas.getOrElse(schema, throw new NoSuchElementException(
            s"Schema with id = '$schema' not found."
          ))
          sparkReaderInit.schema(sourceSchema.schema)
        case None => sparkReaderInit
      }
      // if path to load from is provided then load source from that path:
      val df = config.path.map(_.value) match {
        case Some(path) => sparkReader.load(path)
        case None => sparkReader.load()
      }
      toSource(config, df)
    }
  }

  /**
   * Generic regular source reader that calls specific reader depending on the source configuration type.
   */
  implicit object AnySourceReader extends SourceReader[SourceConfig] {
    /**
     * Tries to read any regular source given the source configuration.
     * @param config Regular source configuration
     * @param settings Implicit application settings object
     * @param spark    Implicit spark session object
     * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: SourceConfig)(implicit settings: AppSettings,
                                        spark: SparkSession,
                                        fs: FileSystem,
                                        schemas: Map[String, SourceSchema],
                                        connections: Map[String, DQConnection]): Source =  
    config match {
      case table: TableSourceConfig => TableSourceReader.tryToRead(table)
      case kafka: KafkaSourceConfig => KafkaSourceReader.tryToRead(kafka)
      case hive: HiveSourceConfig => HiveSourceReader.tryToRead(hive)
      case fixed: FixedFileSourceConfig => FixedFileSourceReader.tryToRead(fixed)
      case delimited: DelimitedFileSourceConfig => DelimitedFileSourceReader.tryToRead(delimited)
      case avro: AvroFileSourceConfig => AvroFileSourceReader.tryToRead(avro)
      case parquet: ParquetFileSourceConfig => ParquetFileSourceReader.tryToRead(parquet)
      case orc: OrcFileSourceConfig => OrcFileSourceReader.tryToRead(orc)
      case custom: CustomSource => CustomSourceReader.tryToRead(custom)
      case other => throw new IllegalArgumentException(s"Unsupported source type: '${other.getClass.getTypeName}'")
    }
  }


  /**
   * Implicit conversion for source configurations to enable read method for them.
   * @param config Source configuration
   * @param reader Implicit reader for given source configuration
   * @param settings Implicit application settings object
   * @param spark Implicit spark session object
   * @param schemas Map of explicitly defined schemas (schemaId -> SourceSchema)
   * @param connections Map of existing connection (connectionID -> DQConnection)
   * @tparam T Type of source configuration
   */
  implicit class SourceReaderOps[T <: SourceConfig](config: T)
                                                   (implicit reader: SourceReader[T],
                                                    settings: AppSettings,
                                                    spark: SparkSession,
                                                    fs: FileSystem,
                                                    schemas: Map[String, SourceSchema],
                                                    connections: Map[String, DQConnection]) {
    def read: Result[Source] = reader.read(config)
  }
}
