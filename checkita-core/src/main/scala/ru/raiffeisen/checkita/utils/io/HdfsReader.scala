package ru.raiffeisen.checkita.utils.io

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import org.apache.avro.Schema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import ru.raiffeisen.checkita.configs.{GenStructColumn, StructColumn, StructFixedColumn}
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.sources.HdfsFile
import ru.raiffeisen.checkita.utils.{DQSettings, Logging}

import java.io.{File, InputStreamReader}
import scala.util.Try
import scala.collection.JavaConversions._


/**
 * HDFS reading manager
 */
object HdfsReader extends Logging {

  /**
   * Function-aggregator to read from HDFS
   * @param inputConf input configuration
   * @param fs file system
   * @param spark spark session
   * @param settings dataquality configuration
   * @return sequency of dataframes
   */
  def load(inputConf: HdfsFile)(implicit fs: FileSystem, spark: SparkSession, settings: DQSettings): Seq[DataFrame] = {

    log.info(s"Loading ${inputConf.fileType.toUpperCase} file -> ${inputConf.id}:${inputConf.path}")
    val dfs = inputConf.fileType.toUpperCase match {
      case "DELIMITED" => loadCsv(inputConf, inputConf.path)
      case "FIXED"     => loadWithSchema(inputConf, inputConf.path)
      case "PARQUET"   => loadParquet(inputConf, inputConf.path)
      case "AVRO"      => loadAvro(inputConf, inputConf.path)
      case "ORC"       => loadOrc(inputConf, inputConf.path)
      case "DELTA"     => loadDelta(inputConf, inputConf.path)
      case _           => throw new Exception(inputConf.toString)
    }
    dfs.map(df => df.select(df.columns.map(c => col(c).as(c.toLowerCase)) : _*))
  }

  /**
   * Loads fixed length file (separation based on length of the fields) from HDFS
   * @param inputConf input configuration (subtype = FIXED)
   * @param filePath path to the file
   * @param fs file system
   * @param spark spark session
   * @return sequence of dataframes
   */
  private def loadWithSchema(inputConf: HdfsFile, filePath: String)(implicit fs: FileSystem,
                                                                    spark: SparkSession): Seq[DataFrame] = {
    log.info(s"Starting load ${inputConf.fileType.toUpperCase} file -> ${filePath}")

    val result: Option[DataFrame] = if (!fs.exists(new Path(filePath))) {
      log.warn("fixed input file: " + filePath + " not found!")
      None
    } else {
      log.warn("loading fixed input file: " + inputConf.id)

      val fieldSeq: List[GenStructColumn] = inputConf.schema.get match {
        case xs: List[_] => xs.filter{
          case _: GenStructColumn => true
          case _ => false
        }.asInstanceOf[List[GenStructColumn]]
        case s: String => tryToLoadSchema(s)
        case e         => throw IllegalParameterException(e.toString)
      }

      val ff: RDD[Row] = spark.sparkContext.textFile(filePath).map { x =>
        getRow(x, fieldSeq)
      }

      // getRow function reads string into Row of string values.
      // Thus, the dataframe is created with all columns having StringType.
      // Then columns are casted to required types using Spark DataFrame API
      val allStringSchema = StructType(fieldSeq.map(x => StructField(x.colName, StringType, nullable = true)))

      Option(spark.createDataFrame(ff, allStringSchema)).map{ df =>
        fieldSeq.filter(_.colType != StringType).foldLeft(df) { (acc, c) =>
          acc.withColumn(c.colName, col(c.colName).cast(c.colType))
        }
      }
    }

    result.map(Seq(_)).getOrElse(Nil)
  }

  /**
   * Loads avro file from HDFS
   * @param inputConf input configuration
   * @param filePath path to file
   * @param fs file system
   * @param spark spark session
   * @param settings dataquality configuration
   * @return sequence of dataframes
   */
  private def loadAvro(inputConf: HdfsFile, filePath: String)(implicit fs: FileSystem,
                                                              spark: SparkSession,
                                                              settings: DQSettings): Seq[DataFrame] = {

    val result: Option[DataFrame] =
      if (!fs.exists(new Path(filePath))) { None } else {

        // It's possible to provide a scheme, so the following code splits the workflow
        val schema = Try {
          inputConf.schema.get match {
            case str: String =>
              Try {
                new Schema.Parser().parse(new File(str))
              }.getOrElse(
                new Schema.Parser().parse(fs.open(new Path(str)))
              )
            case e => IllegalParameterException(e.toString)
          }
        }.toOption

        Try {
          val res: DataFrame = schema match {
            case Some(sc) =>
              spark.read
                .format("avro")
                .option("avroSchema", sc.toString)
                .load(filePath)
            case None =>
              if (inputConf.schema.isDefined)
                log.warn("Failed to load the schema from file")
              spark.read
                .format("avro")
                .load(filePath)
          }

          if (settings.repartition) res.repartition(spark.sparkContext.defaultParallelism)
          else res
        }.toOption
      }
    result.map(Seq(_)).getOrElse(Nil)
  }

  /**
   * Loads Parquet file from HDFS
   * @param inputConf input configuration
   * @param filePath hdfs path to file
   * @param fs file system
   * @param spark spark session
   * @return sequence of dataframes
   */
  private def loadParquet(inputConf: HdfsFile, filePath: String)(implicit fs: FileSystem,
                                                                 spark: SparkSession): Seq[DataFrame] = {

    val result: Option[DataFrame] =
      if (!fs.exists(new Path(filePath))) {
        log.warn("parquet input file: " + inputConf.id + " not found!")
        None
      } else {
        log.warn("loading parquet input file: " + inputConf.id)

        val res = spark.read
          .parquet(filePath)
        Some(res)
      }
    result.map(Seq(_)).getOrElse(Nil)
  }

  /**
   * Loads ORC file from HDFS
   * @param inputConf input configuration
   * @param filePath hdfs path to file
   * @param fs file system
   * @param spark spark session
   * @return sequence of dataframes
   */
  private def loadOrc(inputConf: HdfsFile, filePath: String)(implicit fs: FileSystem,
                                                             spark: SparkSession): Seq[DataFrame] = {

    val result: Option[DataFrame] =
      if (!fs.exists(new Path(filePath))) {
        log.warn("orc input file: " + inputConf.id + " not found!")
        None
      } else {
        log.warn("loading orc input file: " + inputConf.id)

        val res = spark.read.orc(filePath)
        Some(res)
      }
    result.map(Seq(_)).getOrElse(Nil)
  }

  /**
   * Loads Delta table file from Databricks
   * @param inputConf input configuration
   * @param filePath hdfs path to file
   * @param fs file system
   * @param spark spark session
   * @return sequence of dataframes
   */
  private def loadDelta(inputConf: HdfsFile, filePath: String)(implicit fs: FileSystem,
                                                               spark: SparkSession): Seq[DataFrame] = {

    val result: Option[DataFrame] =
      if (!fs.exists(new Path(filePath))) {
        log.warn("Input file: " + inputConf.id + " not found!")
        None
      } else {
        log.warn("loading delta-table input file: " + inputConf.id)

        val res = spark.read.format("delta").load(filePath)
        Some(res)
      }
    result.map(Seq(_)).getOrElse(Nil)
  }

  /**
   * Reads CSV file from HDFS
   * @param inputConf input configuration
   * @param filePath hdfs path to file
   * @param fs file system
   * @param spark Spark Session
   * @param settings dataquality configuration
   * @return sequence of dataframes
   */
  private def loadCsv(inputConf: HdfsFile, filePath: String)(implicit fs: FileSystem,
                                                             spark: SparkSession,
                                                             settings: DQSettings): Seq[DataFrame] = {

    val schema: Option[List[StructField]] = Try {
      inputConf.schema.get match {
        case list: List[_] =>
          list.map {
            case col: StructColumn => StructField(col.colName, col.colType, true)
            case x => throw IllegalParameterException(x.toString)
          }
        case str: String =>
          val colList: List[GenStructColumn] = tryToLoadSchema(str)
          colList.map(col => StructField(col.colName, col.colType, true))
        case e => throw IllegalParameterException(e.toString)
      }
    }.toOption

    log.info(s"File header: ${inputConf.header}")
    log.info("Entered schema: " + schema)
    if (inputConf.header && schema.isDefined)
      throw new IllegalArgumentException(
        "Source can't have schema and header at the same time. Please, check the configuration file...")

    val resultReader: DataFrameReader = spark.read
      .format("csv")
      .option("header", inputConf.header.toString)
      .option("delimiter", inputConf.delimiter.getOrElse(","))
      .option("quote", inputConf.quote.getOrElse("\""))
      .option("escape", inputConf.escape.getOrElse("\\"))

    val result = Try {
      val res: DataFrame = schema match {
        case Some(sc) => resultReader.schema(StructType(sc)).load(filePath)
        case None     => resultReader.load(filePath)
      }

      if (settings.repartition) res.repartition(spark.sparkContext.defaultParallelism)
      else res
    }.toOption

    result.map(Seq(_)).getOrElse(Nil)
  }

  /**
   * Gets spark row from string and fields. Used in fixed file parsing
   * @param x String to parse
   * @param fields Fields to parse
   * @return Row
   */
  private def getRow(x: String, fields: List[GenStructColumn]) = {
    val columnArray = new Array[String](fields.size)
    var pos         = 0
    fields.head.getType match {
      case "StructFixedColumn" =>
        val ll: List[StructFixedColumn] = fields.map(_.asInstanceOf[StructFixedColumn])
        ll.zipWithIndex.foreach { field =>
          columnArray(field._2) = Try(x.substring(pos, pos + field._1.length).trim).getOrElse(null)
          pos += field._1.length
        }
      case _ => IllegalParameterException(fields.head.toString)
    }
    Row.fromSeq(columnArray)
  }

  /**
   * Maps string to spark DataType
   * Used in schema parsing
   * @param colType string to parse
   * @return parsed datatype
   */
  private def mapToDataType(colType: String): DataType = {
    colType.toUpperCase match {
      case "STRING"  => StringType
      case "DOUBLE"  => DoubleType
      case "INTEGER" => IntegerType
      case "DATE"    => DateType
      case e =>
        log.warn("Failed to load schema")
        throw IllegalParameterException(e)
    }
  }

  /**
   * Tries to load schema from file, otherwise throw an exeption
   * @param filePath path to schema
   * @param fs file system
   * @return list of column object
   */
  private def tryToLoadSchema(filePath: String)(implicit fs: FileSystem): List[GenStructColumn] = {
    if (!fs.exists(new Path(filePath))) {
      log.warn("Schema does not exists")
      throw IllegalParameterException(filePath)
    } else {
      val configObj: Config = Try {
        ConfigFactory.parseFile(new File(filePath)).resolve()
      }.getOrElse(Try {
        val inputStream = fs.open(new Path(filePath))
        val reader      = new InputStreamReader(inputStream)

        ConfigFactory.parseReader(reader)
      }.getOrElse(throw IllegalParameterException(filePath)))

      val structColumns: List[ConfigObject] =
        configObj.getObjectList("Schema").toList
      structColumns.map(col => {
        val conf     = col.toConfig
        val name     = conf.getString("name")
        val typeConf = mapToDataType(conf.getString("type"))
        Try {
          conf.getInt("length")
        }.toOption match {
          // there are 2 types of column, one for FIXED files and for regular CSV.
          // CSV schema can be in both forms, but length field will not affect the loading
          // you should use metrics and checks to operate with values length
          case Some(l) => StructFixedColumn(name, typeConf, l)
          case None    => StructColumn(name, typeConf)
        }
      })
    }
  }
}
