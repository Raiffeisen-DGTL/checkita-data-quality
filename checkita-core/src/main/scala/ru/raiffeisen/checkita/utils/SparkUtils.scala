package ru.raiffeisen.checkita.utils

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions.{col, current_timestamp, window}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.Enums.{CustomTime, EventTime, ProcessingTime, StreamWindowing}
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.reflect.runtime.universe.{TermName, runtimeMirror, typeOf}
import scala.concurrent.duration._
import scala.util.Try
import scala.util.matching.Regex

object SparkUtils {

  /**
   * Matches type string literal to a corresponding Spark DataType.
   * @param typeString Type string literal
   * @return Spark DataType
   */
  def toDataType(typeString: String): DataType =
    typeString.toUpperCase match {
      case "STRING" => StringType
      case "BOOLEAN" => BooleanType
      case "DATE" => DateType
      case "TIMESTAMP" => TimestampType
      case "INTEGER" => IntegerType
      case "LONG" => LongType
      case "SHORT" => ShortType
      case "BYTE" => ByteType
      case "DOUBLE" => DoubleType
      case "FLOAT" => FloatType
      case d if d.startsWith("DECIMAL") =>
        val pattern: Regex = """\d+""".r
        val precision :: scale :: _ = pattern.findAllMatchIn(d).map(_.matched.toInt).toList
        DecimalType(precision, scale)
      case _ => throw new IllegalArgumentException(
        s"Cannot convert type string '$typeString' to Spark DataType."
      )
    }

  /**
   * Converts Spark StorageLevel type to a string that defines this type.
   * Note: default implementation of .toString methods does not convert StorageLevel
   * type back to original string.
   * @param sLvl Spark StorageLevel
   * @return Original string definition of the StorageLevel
   */
  def toStorageLvlString(sLvl: StorageLevel): String =
    Seq(sLvl.useDisk, sLvl.useMemory, sLvl.useOffHeap, sLvl.deserialized, sLvl.replication) match {
      case false :: false :: false :: false :: 1 :: Nil => "NONE"
      case true :: false :: false :: false :: 1 :: Nil => "DISK_ONLY"
      case true :: false :: false :: false :: 2 :: Nil => "DISK_ONLY_2"
      case false :: true :: false :: true :: 1 :: Nil => "MEMORY_ONLY"
      case false :: true :: false :: true :: 2 :: Nil => "MEMORY_ONLY_2"
      case false :: true :: false :: false :: 1 :: Nil => "MEMORY_ONLY_SER"
      case false :: true :: false :: false :: 2 :: Nil => "MEMORY_ONLY_SER_2"
      case true :: true :: false :: true :: 1 :: Nil => "MEMORY_AND_DISK"
      case true :: true :: false :: true :: 2 :: Nil => "MEMORY_AND_DISK_2"
      case true :: true :: false :: false :: 1 :: Nil => "MEMORY_AND_DISK_SER"
      case true :: true :: false :: false :: 2 :: Nil => "MEMORY_AND_DISK_SER_2"
      case true :: true :: true :: false :: 1 :: Nil => "OFF_HEAP"
    }

  /**
   * Creates Spark Session
   * @param sparkConf Spark configuration object
   * @param appName Application name
   * @return Either SparkSession object or a list of errors.
   */
  def makeSparkSession(sparkConf: SparkConf, appName: Option[String]): Result[SparkSession] =
    Try {
      val conf = appName match {
        case Some(name) => sparkConf.setAppName(name)
        case None => 
          if (sparkConf.getOption("spark.app.name").isEmpty) sparkConf.setAppName("Checkita Data Quality")
          else sparkConf
      }
      SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    }.toResult(preMsg = "Failed to create SparkSession due to following error:")
      

  /**
   * Creates hadoop filesystem object provided with a spark session
   * @param spark SparkSession object
   * @return Either Hadoop file system object or a list of errors
   */
  def makeFileSystem(spark: SparkSession): Result[FileSystem] = Try {
    val sc = spark.sparkContext
    val fs = if (sc.hadoopConfiguration.get("fs.defaultFS") == "file:///") 
      FileSystem.getLocal(sc.hadoopConfiguration)
    else 
      FileSystem.get(sc.hadoopConfiguration)
    fs.setWriteChecksum(false) // Suppress .crc files creation:
    fs
  }.toResult(preMsg = "Failed to create FileSystem object due to following error:")

  /**
   * Implicit class conversion for scala duration object, to enhance it with
   * following methods:
   *   - convert duration into spark interval string
   *   - convert duration into string with short notation of time unit (e.g. '10s' or '3h')
   * Conversion is always made in terms of seconds.
   * @param d Scala Duration object
   */
  implicit class DurationOps(d: Duration) {
    private val timeUnits: Map[TimeUnit, String] = Map(
      DAYS -> "d",
      HOURS -> "h",
      MINUTES -> "m",
      SECONDS -> "s",
      MILLISECONDS -> "ms",
      MICROSECONDS -> "micro", // avoid special using character 'µ'
      NANOSECONDS -> "ns"
    )
    def toSparkInterval: String = s"${d.toSeconds} seconds"
    def toShortString: String = d match {
      case Duration(l, tu) => s"$l${timeUnits(tu)}"
      case other => throw new IllegalArgumentException(
        s"Unable to construct short-strung representation for duration of ${other.toString}."
      )
    }
  }

  /**
   * Implicit class conversion for Spark DataFrame, to enhance it with
   * prepareStream method which will process streaming dataframe
   * by adding windowing provided with windowing column.
   * @param df Spark streaming DataFrame to prepare for DQ job
   * @param settings Implicit application settings object
   */
  implicit class DataFrameOps (df: DataFrame)(implicit settings: AppSettings) {
    /**
     * Prepares Spark streaming dataframe by adding windowing to it.
     * @param windowBy Stream windowing type
     * @return Transformed Spark streaming DataFrame.
     */
    def prepareStream(windowBy: StreamWindowing): DataFrame = {

      val windowTsCol = settings.streamConfig.windowTsCol
      val eventTsCol = settings.streamConfig.eventTsCol
      val windowDuration = settings.streamConfig.window.toSparkInterval

      val eventColumn = windowBy match {
        case ProcessingTime => current_timestamp().cast(LongType)
        case EventTime => col("timestamp").cast(LongType)
        case CustomTime(column) => column.cast(LongType)
      }

      val dfColumns = df.columns.map(c => col(c)).toSeq ++ Seq(
        col(eventTsCol),
        col(s"${windowTsCol}_pre").getField("start").cast(LongType).as(windowTsCol),
      )

      df.withColumn(eventTsCol, eventColumn)
        .withColumn(s"${windowTsCol}_pre", window(col(eventTsCol).cast(TimestampType), windowDuration))
        .select(dfColumns:_*)
    }
  }

  /**
   * Gets row encoder for provided schema.
   * Purpose of this method is to provide ability to create
   * row encoder for different versions of Spark.
   * Thus, Encoders API has changes in version 3.5.0.
   * Scala reflection API is used to invoke proper
   * row encoder constructor thus supporting different Encoders APIs.
   * @param schema Dataframe schema to construct encoder for.
   * @param spark Implicit spark session object.
   * @return Row encoder (expression encoder for row).
   */
  def getRowEncoder(schema: StructType)
                   (implicit spark: SparkSession): ExpressionEncoder[Row] = {
    val rm = runtimeMirror(getClass.getClassLoader)
    val (encoderMirror, encoderType) =
      if (spark.version < "3.5.0") (rm.reflect(RowEncoder), typeOf[RowEncoder.type])
      else (rm.reflect(ExpressionEncoder), typeOf[ExpressionEncoder.type])
    val applyMethodSymbol = encoderType.decl(TermName("apply")).asTerm.alternatives.find(s =>
      s.asMethod.paramLists.map(_.map(_.typeSignature)) == List(List(typeOf[StructType]))
    ).get.asMethod
    val applyMethod = encoderMirror.reflectMethod(applyMethodSymbol)
    applyMethod(schema).asInstanceOf[ExpressionEncoder[Row]]
  }

}
