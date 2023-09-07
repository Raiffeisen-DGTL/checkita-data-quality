package ru.raiffeisen.checkita.checks.trend

import ru.raiffeisen.checkita.checks.{Check, CheckMessageGenerator, CheckResult, CheckStatus, CheckUtil}
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.metrics.{ColumnMetricResult, ComposedMetricResult, FileMetricResult, MetricResult}
import ru.raiffeisen.checkita.utils.{DQSettings, Logging, toUtcTimestamp}
import ru.raiffeisen.checkita.utils.io.dbmanager.DBManager

import java.sql.Timestamp
import scala.util.Try


/**
 * This the core for single result metric trend analysis. Implements common functionality.
 *
 * @param id check id
 * @param description check description
 * @param metrics List of metrics (should be one metric)
 * @param rule selection rule (date/record)
 * @param threshold requested threshold
 * @param timeWindow result selection timeWindow
 * @param resultsWriter local database manager
 * @param settings dataquality configuration
 */
abstract class TrendCheckCore(
                               id: String,
                               description: String,
                               metrics: Seq[MetricResult],
                               rule: String,
                               threshold: Double,
                               timeWindow: Int
                             )(implicit resultsWriter: DBManager, settings: DQSettings)
  extends Check with Logging {

  // Things to be implemented in the child classes
  def addMetricList(metrics: Seq[MetricResult]): Check
  def calculateCheck(metric: Double, avg: Double, threshold: Double): Boolean
  def calculatePrediction(results: Seq[Double]): Double
  def getStatusString(status: CheckStatus,
                      metric: Double,
                      avg: Double,
                      threshold: Double): String
  val subType: String

  override def metricsList: Seq[MetricResult] = metrics

  /**
   * Calculates basic trend check
   * @return Check result
   */
  override def run(): CheckResult = {

    val baseMetricResult: MetricResult = metrics.head
    val jobId: String = baseMetricResult.jobId
    val targetMetricResult: MetricResult = metrics.last

    val metricIds: List[String] = List(baseMetricResult.metricId)
    val startTS: Timestamp = toUtcTimestamp(settings.referenceDate)

    // it will automatically select the correct table to load from, based on the main metric class
    val dbMetResults: Seq[Double] = baseMetricResult match {
      case _: ComposedMetricResult =>
        resultsWriter.loadCompMetResults(
          metricIds,
          jobId,
          rule,
          timeWindow,
          startTS)
          .map(x => x.result)
      case _: ColumnMetricResult =>
        resultsWriter.loadColMetResults(
          metricIds,
          jobId,
          rule,
          timeWindow,
          startTS)
          .map(x => x.result)
      case _: FileMetricResult =>
        resultsWriter.loadFileMetResults(
          metricIds,
          jobId,
          rule,
          timeWindow,
          startTS)
          .map(x => x.result)
      case x => throw IllegalParameterException(x.toString)
    }

    /*
     * in the current state we're assuming that time distance between record is always the same
     * so the prediction in the next record after provided ones
     */
    val avg = calculatePrediction(dbMetResults)

    val (lowerBound, upperBound) = subType match {
      case "AVERAGE_BOUND_FULL_CHECK" => Some(avg * (1 - threshold)) -> Some(avg * (1 + threshold))
      case "AVERAGE_BOUND_LOWER_CHECK" => Some(avg * (1 - threshold)) -> None
      case "AVERAGE_BOUND_UPPER_CHECK" => None -> Some(avg * (1 + threshold))
      case _ => throw IllegalParameterException(subType)
    }

    val checkStatus = CheckUtil.tryToStatus[Double](
      Try(targetMetricResult.result),
      d => calculateCheck(d, avg, threshold))

    val statusString =
      getStatusString(checkStatus, targetMetricResult.result, avg, threshold)

    val checkMessage = CheckMessageGenerator(
      targetMetricResult,
      threshold,
      checkStatus,
      statusString,
      id,
      subType,
      Some(rule),
      Some(timeWindow)
    )

    val cr = CheckResult(
      this.id,
      subType,
      this.description,
      baseMetricResult.sourceId,
      baseMetricResult.metricId,
      Some(targetMetricResult.metricId),
      threshold,
      lowerBound,
      upperBound,
      checkStatus.stringValue,
      checkMessage.message
    )

    cr
  }
}

