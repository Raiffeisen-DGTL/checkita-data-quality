package ru.raiffeisen.checkita.utils.io.dbmanager

import ru.raiffeisen.checkita.metrics.{
  ColumnMetricResult, ColumnMetricResultFormatted, ComposedMetricResult, FileMetricResult
}

import java.sql.Timestamp
import java.util.{Calendar, TimeZone}

private[checkita] abstract class DBManager {
  type R

  def colMetConverter(r: R): Seq[ColumnMetricResult]
  def colMetFmtConverter(r: R): Seq[ColumnMetricResultFormatted]
  def fileMetConverter(r: R): Seq[FileMetricResult]
  def compMetConverter(r: R): Seq[ComposedMetricResult]

  def saveResultsToDB(metrics: Seq[AnyRef], tb: String): Unit

  def loadResults[T](metricSet: List[String],
                     jobId: String,
                     rule: String,
                     tw: Int,
                     startDate: Timestamp,
                     tb: String)(f: R => Seq[T]): Seq[T]

  def loadColMetResults(metricSet: List[String],
                        jobId: String,
                        rule: String,
                        tw: Int,
                        startDate: Timestamp): Seq[ColumnMetricResult] =
    loadResults[ColumnMetricResult](metricSet, jobId, rule, tw, startDate, "results_metric_columnar")(colMetConverter)

  def loadColMetFmtResults(metricSet: List[String],
                           jobId: String,
                           rule: String,
                           tw: Int,
                           startDate: Timestamp): Seq[ColumnMetricResultFormatted] =
    loadResults[ColumnMetricResultFormatted](metricSet, jobId, rule, tw, startDate, "results_metric_columnar")(colMetFmtConverter)

  def loadFileMetResults(metricSet: List[String],
                         jobId: String,
                         rule: String,
                         tw: Int,
                         startDate: Timestamp): Seq[FileMetricResult] =
    loadResults[FileMetricResult](metricSet, jobId, rule, tw, startDate, "results_metric_file")(fileMetConverter)

  def loadCompMetResults(metricSet: List[String],
                         jobId: String,
                         rule: String,
                         tw: Int,
                         startDate: Timestamp): Seq[ComposedMetricResult] =
    loadResults[ComposedMetricResult](metricSet, jobId, rule, tw, startDate, "results_metric_composed")(compMetConverter)

  protected val tzUTC: Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
}
