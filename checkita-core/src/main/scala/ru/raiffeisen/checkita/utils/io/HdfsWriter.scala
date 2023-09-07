package ru.raiffeisen.checkita.utils.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import ru.raiffeisen.checkita.checks.{CheckResult, LoadCheckResult}
import ru.raiffeisen.checkita.configs.ConfigReader
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.metrics._
import ru.raiffeisen.checkita.targets.{HdfsTargetConfig, HiveTargetConfig}
import ru.raiffeisen.checkita.utils.{DQSettings, Logging}

import java.io.IOException
import java.time.format.DateTimeFormatter
import scala.util.Try


/**
 * HDFS writing manager
 */
object HdfsWriter extends Logging {

  /**
   * FileUtil.copMerge method is deprecated and no longer available in Hadoop 3+
   * Added scala implementation of copyMerge method directly to HdfsWriter object.
   * Implementation is taken from:
   *
   * https://stackoverflow.com/questions/42035735/how-to-do-copymerge-in-hadoop-3-0
   *
   * Modifications: removed boolean parameter since we'd like that srcDir is always being removed.
   * @param srcFS Source File System
   * @param srcDir Source Path
   * @param dstFS Destination File System
   * @param dstFile Destination Path
   * @param conf Hadoop configuration
   * @return True if files are merged successfully otherwise False
   */
  def copyMerge(
                 srcFS: FileSystem, srcDir: Path,
                 dstFS: FileSystem, dstFile: Path,
                 conf: Configuration
               ): Boolean = {

    if (dstFS.exists(dstFile))
      throw new IOException(s"Target $dstFile already exists")

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory) {

      val outputFile = dstFS.create(dstFile)
      Try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile =>
              val inputFile = srcFS.open(status.getPath)
              Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()
      srcFS.delete(srcDir, true)
    }
    else false
  }

  private def getDataFrameFromResult(sq: Seq[Product with TypedResult])
                                    (implicit spark: SparkSession,
                                     configuration: ConfigReader,
                                     settings: DQSettings): DataFrame = {
    sq.head.getType match {
      case DQResultTypes.column =>
        spark.createDataFrame(sq.asInstanceOf[Seq[ColumnMetricResult]].map(_.toDbFormat))
      case DQResultTypes.file =>
        spark.createDataFrame(sq.asInstanceOf[Seq[FileMetricResult]].map(_.toDbFormat))
      case DQResultTypes.composed =>
        spark.createDataFrame(sq.asInstanceOf[Seq[ComposedMetricResult]].map(_.toDbFormat))
      case DQResultTypes.check =>
        spark.createDataFrame(sq.asInstanceOf[Seq[CheckResult]].map(_.toDbFormat)).drop("execDate")
      case DQResultTypes.load =>
        spark.createDataFrame(sq.asInstanceOf[Seq[LoadCheckResult]].map(_.toDbFormat))
      case x => throw IllegalParameterException(x.toString)
    }
  }

  /**
   * Function-aggregator to save dataframe in HDFS
   *
   * @param target target configuration
   * @param sq sequence to save
   * @param spark Spark Session
   * @param fs file system
   * @param settings DataQuality configuration
   */
  def saveHdfs(target: HdfsTargetConfig, sq: Seq[Product with TypedResult])
              (implicit spark: SparkSession, fs: FileSystem, settings: DQSettings, configuration: ConfigReader): Unit = {

    log.info(s"Saving Results: ${target.fileName.toUpperCase}...")

    if (sq.nonEmpty) {
      val df = getDataFrameFromResult(sq)
      saveDF(target, df)

    } else log.warn("ERROR: Failed to save an empty file")
  }

  /**
   * Function-aggregator to save dataframe into Hive table
   *
   * @param target target configuration
   * @param sq sequence to save
   * @param spark Spark Session
   * @param fs file system
   * @param configuration DataQuality configuration
   */
  def saveHive(target: HiveTargetConfig,
               sq: Seq[Product with TypedResult])(implicit spark: SparkSession,
                                                  fs: FileSystem,
                                                  configuration: ConfigReader,
                                                  settings: DQSettings): Unit = {

    log.info(s"Saving Results into Hive table: ${target.schema}.${target.table}...")

    if (sq.nonEmpty) {
      val df = getDataFrameFromResult(sq)
      val timeFmt = DateTimeFormatter.ofPattern("HH-mm-ss").withZone(settings.tz)
      val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(settings.tz)
      val execTime = settings.executionDate.format(timeFmt)
      val execDate = settings.executionDate.format(dateFmt)
      df.withColumn("exec_time", lit(execTime))
        .withColumn("exec_date", lit(execDate))
        .repartition(1).write.mode("append").format("hive")
        .insertInto(s"${target.schema}.${target.table}")
    } else log.warn("ERROR: Failed to save an empty dataframe")
  }

  def saveDF(target: HdfsTargetConfig,
             df: DataFrame)(implicit spark: SparkSession, fs: FileSystem, settings: DQSettings): Unit = {
    log.info(s"Saving DF: ${target.fileName}...")

    // since we want to allow you to save on the custom date
    val execDate: String = settings.referenceDateString

    target.fileFormat.toUpperCase match {
      case "CSV" | "TXT" => saveCsv(df, target, target.date.getOrElse(execDate))
      case "PARQUET" => saveParquet(df, target, target.date.getOrElse(execDate))
      case "ORC" => saveOrc(df, target, target.date.getOrElse(execDate))
      case _ => throw IllegalParameterException(target.fileFormat.toUpperCase)
    }
  }

  /**
   * Saves CSV file with results
   * @param df data frame to save
   * @param targetConfig target configuration
   * @param fs file system
   */
  private def saveCsv(df: DataFrame, targetConfig: HdfsTargetConfig, execDate: String)(
    implicit fs: FileSystem): Unit = {
    log.debug("path: " + targetConfig.path)

    val tempFileName = targetConfig.path + "/" + targetConfig.fileName + s"_$execDate" + ".tmp" //-${targetConfig.subType}
    val fileName     = targetConfig.path + "/" + targetConfig.fileName + s"_$execDate" + "." + targetConfig.fileFormat //-${targetConfig.subType}

    log.debug("writing temp csv file: " + tempFileName)
    df.write
      .format("csv")
      .option("header", "false")
      .option("quoteMode", targetConfig.quoteMode.getOrElse("MINIMAL"))
      .option("delimiter", targetConfig.delimiter.getOrElse(","))
      .option("quote", targetConfig.quote.getOrElse("\""))
      .option("escape", targetConfig.escape.getOrElse("\\"))
      .option("nullValue", "")
      .mode(SaveMode.Overwrite)
      .save(tempFileName)

    val header: String =
      if (targetConfig.quoteMode.contains("ALL")) {
        df.schema.fieldNames.mkString("\"", "\"" + s"${targetConfig.delimiter.getOrElse(",")}" + "\"", "\"")
      } else {
        df.schema.fieldNames.mkString(targetConfig.delimiter.getOrElse(","))
      }

    log.debug("temp csv file: " + tempFileName + " has been written")

    try {
      val path = new Path(fileName)
      if (fs.exists(path)) fs.delete(path, false)
      val headerOutputStream: FSDataOutputStream =
        fs.create(new Path(tempFileName + "/header"))
      headerOutputStream.writeBytes(header + "\n")
      headerOutputStream.close()
      copyMerge(fs, new Path(tempFileName), fs, path, new Configuration())
    } catch {
      case ioe: IOException => log.warn(ioe)
    }

    log.debug("final csv file: " + fileName + " merged")
    log.debug("'write output' step finished")
  }

  /**
   * Save Parquet file to the HDFS
   * @param df data frame to save
   * @param targetConfig target configuration
   * @param execDate save date
   * @param fs file system
   */
  private def saveParquet(df: DataFrame, targetConfig: HdfsTargetConfig, execDate: String)(
    implicit fs: FileSystem): Unit = {
    log.info(s"starting 'write ${targetConfig.fileName.toUpperCase} results' ")
    log.debug("path: " + targetConfig.path)

    val tempFileName = targetConfig.path + "/" + targetConfig.fileName + s"_$execDate" + ".tmp" //-${targetConfig.subType}
    val fileName     = targetConfig.path + "/" + targetConfig.fileName + s"_$execDate" + "." + targetConfig.fileFormat //-${targetConfig.subType}

    log.info("writing temp parquet file: " + tempFileName)
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(tempFileName)

    log.info("temp parquet file: " + tempFileName + " written")

    copyMerge(fs, new Path(tempFileName), fs, new Path(fileName), new Configuration())

    log.info("final parquet file: " + fileName + " merged")
    log.info("'write output' step finished")
  }

  /**
   * Save Parquet file to the HDFS
   * @param df data frame to save
   * @param targetConfig target configuration
   * @param execDate save date
   * @param fs file system
   */
  private def saveOrc(df: DataFrame, targetConfig: HdfsTargetConfig, execDate: String)
                     (implicit fs: FileSystem): Unit = {
    log.info(s"starting 'write ${targetConfig.fileName.toUpperCase} results' ")
    log.debug("path: " + targetConfig.path)

    val tempFileName = targetConfig.path + "/" + targetConfig.fileName + s"_$execDate" + ".tmp" //-${targetConfig.subType}
    val fileName     = targetConfig.path + "/" + targetConfig.fileName + s"_$execDate" + "." + targetConfig.fileFormat //-${targetConfig.subType}

    log.info("writing temp orc file: " + tempFileName)
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .orc(tempFileName)

    log.info("temp orc file: " + tempFileName + " written")

    copyMerge(fs, new Path(tempFileName), fs, new Path(fileName), new Configuration())

    log.info("final orc file: " + fileName + " merged")
    log.info("'write output' step finished")
  }

}