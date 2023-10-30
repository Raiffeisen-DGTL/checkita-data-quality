package ru.raiffeisen.checkita.utils

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import ru.raiffeisen.checkita.utils.ResultUtils._

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
}
