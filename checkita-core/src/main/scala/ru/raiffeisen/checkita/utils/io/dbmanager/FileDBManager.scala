package ru.raiffeisen.checkita.utils.io.dbmanager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.utils.Logging
import ru.raiffeisen.checkita.utils.io.HdfsWriter.copyMerge

import java.sql.Timestamp
import java.time.ZoneId
import scala.util.Try

final class FileDBManager(config: Option[DBManagerConfig],
                          spark: SparkSession,
                          fs: FileSystem) extends SparkDBManager with Logging {

  private val dbPath: Option[String] = config.flatMap(_.path)

  /**
   * Saves metric and check results to a file.
   * Idea here is that in case of unique key conflict data is replaced but not appended.
   * @param metrics Sequence of metrics (or checks) results
   * @param tb Target table
   */
  def saveResultsToDB(metrics: Seq[AnyRef], tb: String): Unit = dbPath match {
    case Some(path) =>
      val resultsDir = s"$path/$tb"
      val resultsFile = s"$resultsDir/${tb}_data.parquet"
      val tmpPath = s"$path/.tmp/$tb"

      log.info(s"Saving '$tb' to $resultsDir")
      try {
        val resultSchema = buildSchema(metrics.head)
        val uniqueCols = uniqueFields(tb)

        val currentData = if (!fs.exists(new Path(resultsDir)))
          spark.createDataFrame(rowRDD=spark.sparkContext.emptyRDD[Row], schema=resultSchema)
        else
          spark.read.parquet(resultsDir).select(resultSchema.map(c => col(c.name).cast(c.dataType)) : _*)

        val resultsData = updateData(spark, currentData, metrics, resultSchema, uniqueCols)

        resultsData.repartition(1).write.mode(SaveMode.Overwrite).parquet(tmpPath)

        // overwrite existing data:
        val dstPath = new Path(resultsFile)
        val srcPath = new Path(tmpPath)
        // remove old data:
        if (fs.exists(dstPath)) fs.delete(dstPath, true)
        copyMerge(fs, srcPath, fs, dstPath, new Configuration())

        log.info("Success!")
      } catch {
        case _: NoSuchElementException =>
          log.warn("Nothing to save!")
        case e: AnalysisException =>
          log.error("Failed to load with spark error:")
          log.error(e.toString)
        case e: Exception =>
          log.error("Failed with error:")
          log.error(e.toString)
      }
    case None => log.warn("History storage is not connected. Avoiding saving the results...")
  }

  def loadResults[T](metricSet: List[String],
                     jobId: String,
                     rule: String,
                     tw: Int,
                     startDate: Timestamp,
                     tb: String)(f: R => Seq[T]): Seq[T] = dbPath match {
    case Some(path) =>
      val resultsPath = s"$path/$tb"
      log.info(s"Loading results from '$tb' stored in '$resultsPath'...")

      val currentData = Try(spark.read.parquet(resultsPath)).toOption

      val resultSet: Seq[T] = currentData.map{ df =>
        val preDf = df.filter(s"job_id='$jobId'")
          .filter(col("metric_id").isin(metricSet:_*))
          .filter(col("reference_date") <= startDate)

        val results = rule match {
          case "record" =>
            preDf.orderBy(col("reference_date").desc).limit(tw * metricSet.length).collect().toSeq
          case "date" =>
            val lastDate =Timestamp.from(startDate.toInstant.atZone(ZoneId.of("UTC")).minusDays(tw).toInstant)
            preDf.filter(col("reference_date") >= lastDate).collect().toSeq
          case x => throw IllegalParameterException(x)
        }
        results.flatMap(f)
      }.getOrElse(Seq.empty[T])

      if (currentData.isEmpty)
        log.warn("History storage file is absent or is empty. Providing empty historical results...")
      else
        log.info(s"Results found: ${resultSet.length}")
      resultSet
    case None =>
      log.warn("History database is not connected. Providing empty historical results...")
      Seq.empty[T]
  }
}
