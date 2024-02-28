package ru.raiffeisen.checkita.readers

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.Enums.StreamWindowing
import ru.raiffeisen.checkita.config.jobconf.Sources._
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.connections.jdbc.JdbcConnection
import ru.raiffeisen.checkita.connections.kafka.KafkaConnection
import ru.raiffeisen.checkita.connections.greenplum.PivotalConnection
import ru.raiffeisen.checkita.core.Source
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema
import ru.raiffeisen.checkita.utils.Common.paramsSeqToMap
import ru.raiffeisen.checkita.utils.ResultUtils._
import ru.raiffeisen.checkita.utils.SparkUtils.{DataFrameOps, getRowEncoder}

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
     *
     * @param config      Source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     *
     * @note Safeguard against reading non-streamable source as a stream is implemented in the higher-level
     *       method that uses this one. Therefore, current method implementation may just ignore 'readMode'
     *       argument for non-streamable sources.
     */
    def tryToRead(config: T,
                  readMode: ReadMode)(implicit settings: AppSettings,
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
      Try(tryToRead(config, ReadMode.Batch)).toResult(
        preMsg = s"Unable to read source '${config.id.value}' due to following error: "
      )

    /**
     * Safely reads streaming source given source configuration.
     *
     * @param config      Source configuration (source must be streamable)
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Either a valid Source or a list of source reading errors.
     */
    def readStream(config: T)(implicit settings: AppSettings,
                              spark: SparkSession,
                              fs: FileSystem,
                              schemas: Map[String, SourceSchema],
                              connections: Map[String, DQConnection]): Result[Source] = Try {
      if (config.streamable) tryToRead(config, ReadMode.Stream)
      else throw new UnsupportedOperationException(
        s"Source ${config.id} is not streamable and, therefore, cannot be read as a stream."
      )
    }.toResult(
      preMsg = s"Unable to read streaming source '${config.id.value}' due to following error: "
    )
  }

  sealed trait SimpleFileReader { this: SourceReader[_] =>

    /**
     * Basic file source reader that reads file source either
     * as a static dataframe or as a streaming dataframe.
     *
     * @param readMode Mode in which source is read. Either 'batch' or 'stream'
     * @param path     Path to read source from
     * @param format   File format
     * @param schemaId   Schema ID to apply while reading data
     * @param spark    Implicit spark session object
     * @return Spark DataFrame
     */
    protected def fileReader(readMode: ReadMode,
                             path: String,
                             format: String,
                             schemaId: Option[String],
                             windowBy: StreamWindowing)
                            (implicit settings: AppSettings,
                             spark: SparkSession,
                             fs: FileSystem,
                             schemas: Map[String, SourceSchema]): DataFrame = {

      val reader = (schema: Option[SourceSchema]) => readMode match {
        case ReadMode.Batch =>
          val batchReader = spark.read.format(format.toLowerCase)
          if (format.toLowerCase == "avro")
            schema.map(sch => batchReader.option("avroSchema", sch.toAvroSchema.toString))
              .getOrElse(batchReader).load(path)
          else schema.map(sch => batchReader.schema(sch.schema)).getOrElse(batchReader).load(path)
        case ReadMode.Stream =>
          val sch = schema.getOrElse(throw new IllegalArgumentException(
            s"Schema is missing but it must be provided to read $format files as a stream."
          ))
          spark.readStream.format(format.toLowerCase).schema(sch.schema).load(path).prepareStream(windowBy)
      }

      if (fs.exists(new Path(path))) {
        val sourceSchema = schemaId.map(sId =>
          schemas.getOrElse(sId, throw new NoSuchElementException(s"Schema with id = '$sId' not found."))
        ) // we want to throw exception if schemaId is provided but not found.
        reader(sourceSchema)
      } else throw new FileNotFoundException(s"$format file or directory not found: $path")
    }
  }

  /**
   * Table source reader: reads source from JDBC Connection (Postgres, Oracle, etc)
   * @note In order to read table source it is required to provided map of valid connections
   */
  implicit object TableSourceReader extends SourceReader[TableSourceConfig] {

    /**
     * Tries to read table source given the source configuration.
     *
     * @param config      Table source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     * @note TableSource is not streamable, therefore, 'readMode' argument is ignored
     *       and source is always read as static DataFrame.
     */
    def tryToRead(config: TableSourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                      spark: SparkSession,
                                      fs: FileSystem,
                                      schemas: Map[String, SourceSchema],
                                      connections: Map[String, DQConnection]): Source = {
      val conn = connections.getOrElse(config.connection.value, throw new NoSuchElementException(
        s"JDBC connection with id = '${config.connection.value}' not found."
      ))
      
      require(conn.isInstanceOf[JdbcConnection[_]], s"Table source '${config.id.value}' refers to non-Jdbc connection.")
      
      val df = conn.asInstanceOf[JdbcConnection[_]].loadDataFrame(config)
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
     *
     * @param config      Kafka source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: KafkaSourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                      spark: SparkSession,
                                      fs: FileSystem,
                                      schemas: Map[String, SourceSchema],
                                      connections: Map[String, DQConnection]): Source = {

      val conn = connections.getOrElse(config.connection.value, throw new NoSuchElementException(
        s"Kafka connection with id = '${config.connection.value}' not found."
      ))

      require(conn.isInstanceOf[KafkaConnection], 
        s"Kafka source '${config.id.value}' refers to not a Kafka connection.")

      val df = readMode match {
        case ReadMode.Batch => conn.asInstanceOf[KafkaConnection].loadDataFrame(config)
        case ReadMode.Stream => conn.asInstanceOf[KafkaConnection].loadDataStream(config)
      }
      toSource(config, df)
    }
  }

  /**
   * Greenplum source reader: reads source from pivotal Connection
   *
   * @note In order to read greenplum source it is required to provided map of valid connections
   */
  implicit object GreenplumSourceReader extends SourceReader[GreenplumSourceConfig] {

    /**
     * Tries to read greenplum source given the source configuration.
     *
     * @param config      Greenplum source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: GreenplumSourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                      spark: SparkSession,
                                      fs: FileSystem,
                                      schemas: Map[String, SourceSchema],
                                      connections: Map[String, DQConnection]): Source = {

      val conn = connections.getOrElse(config.connection.value, throw new NoSuchElementException(
        s"Pivotal greenplum connection with id = '${config.connection.value}' not found."
      ))

      require(conn.isInstanceOf[PivotalConnection],
        s"Table source '${config.id.value}' refers to not pivotal greenplum connection.")

      val df = conn.asInstanceOf[PivotalConnection].loadDataFrame(config)
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
     *
     * @param config      Hive source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     * @note HiveSource is not streamable, therefore, 'readMode' argument is ignored
     *       and source is always read as static DataFrame.
     */
    def tryToRead(config: HiveSourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                      spark: SparkSession,
                                      fs: FileSystem,
                                      schemas: Map[String, SourceSchema],
                                      connections: Map[String, DQConnection]): Source = {
      val tableName = s"${config.schema.value}.${config.table.value}"
      val preDf = spark.read.table(tableName)
      val df = if (config.partitions.nonEmpty) {
        preDf.filter(
          config.partitions.map { p =>
            if (p.expr.nonEmpty) p.expr.get
            else col(p.name.value).isin(p.values.map(_.value))
          }.reduce(_ && _)
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
      widthsToPositions(widths).map {
        case (p1, p2) => Try(x.substring(p1, p2)).getOrElse(null)
      }
    )

    /**
     * Tries to read fixed file source given the source configuration.
     *
     * @param config      Fixed file source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     * @note When read in stream mode, Spark will stream newly added files only.
     */
    def tryToRead(config: FixedFileSourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                      spark: SparkSession,
                                      fs: FileSystem,
                                      schemas: Map[String, SourceSchema],
                                      connections: Map[String, DQConnection]): Source = {

      val schemaId = config.schema.map(_.value).getOrElse(
        throw new IllegalArgumentException("Schema must always be provided to read fixed-width file.")
      )
      val sourceSchema = schemas.getOrElse(schemaId,
        throw new NoSuchElementException(s"Schema with id = '$schemaId' not found.")
      )
      val allStringSchema = StructType(sourceSchema.schema.map(
        col => StructField(col.name, StringType, nullable = true)
      ))

      implicit val encoder: ExpressionEncoder[Row] = getRowEncoder(allStringSchema)

      val rawDf = if (fs.exists(new Path(config.path.value))) readMode match {
        case ReadMode.Batch => spark.read.text(config.path.value)
        case ReadMode.Stream => spark.readStream.text(config.path.value)
      } else throw new FileNotFoundException(s"Fixed-width text file or directory not found: ${config.path.value}")

      val df = rawDf.map(c => getRow(c.getString(0), sourceSchema.columnWidths)).select(
        sourceSchema.schema.map(f => col(f.name).cast(f.dataType)): _*
      )
      toSource(config, if (readMode == ReadMode.Batch) df else df.prepareStream(config.windowBy))
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
     *
     * @param config      Delimited file source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     * @note When read in stream mode, Spark will stream newly added files only.
     */
    def tryToRead(config: DelimitedFileSourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                      spark: SparkSession,
                                      fs: FileSystem,
                                      schemas: Map[String, SourceSchema],
                                      connections: Map[String, DQConnection]): Source = {

      val reader = (opts: Map[String, String], schema: Option[StructType]) => readMode match {
        case ReadMode.Batch =>
          val batchReader = spark.read.format("csv").options(opts)
          schema.map(s => batchReader.schema(s)).getOrElse(batchReader).load(config.path.value)
        case ReadMode.Stream =>
          val streamReader = spark.readStream.format("csv").options(opts)
          schema.map(s => streamReader.schema(s)).getOrElse(streamReader).load(config.path.value)
      }

      val readOptions = Map(
        "sep" -> config.delimiter.value,
        "quote" -> config.quote.value,
        "escape" -> config.escape.value,
        "mode" -> (if (readMode == ReadMode.Batch) "FAILFAST" else "PERMISSIVE")
      )

      val df = if (fs.exists(new Path(config.path.value))) {
        (config.header, config.schema.map(_.value)) match {
          case (true, None) => reader(readOptions + ("header" -> "true"), None)
          case (false, Some(schema)) =>
            val sourceSchema = schemas.getOrElse(schema, throw new NoSuchElementException(
              s"Schema with id = '$schema' not found."
            ))
            reader(readOptions, Some(sourceSchema.schema))
          case _ => throw new IllegalArgumentException(
            "For delimited file sources schema must either be read from header or from explicit schema but not from both."
          )
        }
      } else throw new FileNotFoundException(s"Delimited text file or directory not found: ${config.path.value}")
      
      toSource(config, if (readMode == ReadMode.Batch) df else df.prepareStream(config.windowBy))
    }
  }

  /**
   * Avro file source reader: reads avro file with optional explicit schema.
   * @note In order to read avro file source it is required to provide map of source schemas.
   */
  implicit object AvroFileSourceReader extends SourceReader[AvroFileSourceConfig] with SimpleFileReader {

    /**
     * Tries to read avro file source given the source configuration.
     *
     * @param config      Avro file source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     * @note When read in stream mode, Spark will stream newly added files only.
     */
    def tryToRead(config: AvroFileSourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                      spark: SparkSession,
                                      fs: FileSystem,
                                      schemas: Map[String, SourceSchema],
                                      connections: Map[String, DQConnection]): Source =
      toSource(config, fileReader(readMode, config.path.value, "Avro", config.schema.map(_.value), config.windowBy))
  }

  /**
   * Parquet file source reader: reads parquet files.
   */
  implicit object ParquetFileSourceReader extends SourceReader[ParquetFileSourceConfig] with SimpleFileReader {

    /**
     * Tries to read parquet file source given the source configuration.
     *
     * @param config      Parquet file source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     * @note When read in stream mode, Spark will stream newly added files only.
     */
    def tryToRead(config: ParquetFileSourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                      spark: SparkSession,
                                      fs: FileSystem,
                                      schemas: Map[String, SourceSchema],
                                      connections: Map[String, DQConnection]): Source =
      toSource(config, fileReader(readMode, config.path.value, "Parquet", config.schema.map(_.value), config.windowBy))
  }

  /**
   * Orc file source reader: reads orc files.
   */
  implicit object OrcFileSourceReader extends SourceReader[OrcFileSourceConfig] with SimpleFileReader {

    /**
     * Tries to read orc file source given the source configuration.
     *
     * @param config      Orc file source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: OrcFileSourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                      spark: SparkSession,
                                      fs: FileSystem,
                                      schemas: Map[String, SourceSchema],
                                      connections: Map[String, DQConnection]): Source =
      toSource(config, fileReader(readMode, config.path.value, "ORC", config.schema.map(_.value), config.windowBy))
  }

  implicit object CustomSourceReader extends SourceReader[CustomSource] {

    /**
     * Tries to read source given the source configuration.
     *
     * @param config      Source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: CustomSource,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                        spark: SparkSession,
                                        fs: FileSystem,
                                        schemas: Map[String, SourceSchema],
                                        connections: Map[String, DQConnection]): Source = {
      val readOptions = paramsSeqToMap(config.options.map(_.value))
      val sourceSchema = config.schema.map(_.value).map(sId =>
        schemas.getOrElse(sId, throw new NoSuchElementException(s"Schema with id = '$sId' not found."))
      ) // we want to throw exception if schemaId is provided but not found.

      val df = readMode match {
        case ReadMode.Batch =>
          val readerInit = spark.read.format(config.format.value).options(readOptions)
          val reader = sourceSchema.map(s => readerInit.schema(s.schema)).getOrElse(readerInit)
          config.path.map(_.value).map(p => reader.load(p)).getOrElse(reader.load())
        case ReadMode.Stream =>
          val readerInit = spark.readStream.format(config.format.value).options(readOptions)
          val reader = sourceSchema.map(s => readerInit.schema(s.schema)).getOrElse(readerInit)
          config.path.map(_.value).map(p => reader.load(p)).getOrElse(reader.load())
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
     *
     * @param config      Regular source configuration
     * @param readMode    Mode in which source is read. Either 'batch' or 'stream'
     * @param settings    Implicit application settings object
     * @param spark       Implicit spark session object
     * @param schemas     Map of explicitly defined schemas (schemaId -> SourceSchema)
     * @param connections Map of existing connection (connectionID -> DQConnection)
     * @return Source
     */
    def tryToRead(config: SourceConfig,
                  readMode: ReadMode)(implicit settings: AppSettings,
                                        spark: SparkSession,
                                        fs: FileSystem,
                                        schemas: Map[String, SourceSchema],
                                        connections: Map[String, DQConnection]): Source =  
    config match {
      case table: TableSourceConfig => TableSourceReader.tryToRead(table, readMode)
      case kafka: KafkaSourceConfig => KafkaSourceReader.tryToRead(kafka, readMode)
      case greenplum: GreenplumSourceConfig => GreenplumSourceReader.tryToRead(greenplum, readMode)
      case hive: HiveSourceConfig => HiveSourceReader.tryToRead(hive, readMode)
      case fixed: FixedFileSourceConfig => FixedFileSourceReader.tryToRead(fixed, readMode)
      case delimited: DelimitedFileSourceConfig => DelimitedFileSourceReader.tryToRead(delimited, readMode)
      case avro: AvroFileSourceConfig => AvroFileSourceReader.tryToRead(avro, readMode)
      case parquet: ParquetFileSourceConfig => ParquetFileSourceReader.tryToRead(parquet, readMode)
      case orc: OrcFileSourceConfig => OrcFileSourceReader.tryToRead(orc, readMode)
      case custom: CustomSource => CustomSourceReader.tryToRead(custom, readMode)
      case other => throw new IllegalArgumentException(s"Unsupported source type: '${other.getClass.getTypeName}'")
    }
  }


  /**
   * Implicit conversion for source configurations to enable read and readStream methods for them.
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
    def readStream: Result[Source] = Try(if (!config.streamable) throw new UnsupportedOperationException(
      s"Source '${config.id.value}' is not streamable and, therefore, cannot be read as a stream."
    )).toResult(preMsg = s"Unable to read source as a stream")
      .flatMap(_ => reader.readStream(config))
  }
}
