package ru.raiffeisen.checkita.checks.trend

import ru.raiffeisen.checkita.checks._
import ru.raiffeisen.checkita.exceptions.{IllegalConstraintResultException, IllegalParameterException}
import ru.raiffeisen.checkita.metrics.{ColumnMetricResult, ComposedMetricResult, FileMetricResult, MetricResult}
import ru.raiffeisen.checkita.utils.{DQSettings, toUtcTimestamp}
import ru.raiffeisen.checkita.utils.io.dbmanager.DBManager

import java.sql.Timestamp
import scala.util.Try


case class AverageBoundRangeCheck(id: String,
                                  description: String,
                                  metrics: Seq[MetricResult],
                                  rule: String,
                                  thresholdUpper: Double,
                                  thresholdLower: Double,
                                  timeWindow: Int)(implicit resultsWriter: DBManager,
                                                   settings: DQSettings)
  extends Check with AverageCheckDistanceCalculator {


  override def metricsList: Seq[MetricResult] = metrics

  def calculateCheck(metric: Double, avg: Double, thresholdUp: Double, thresholdDown: Double): Boolean = {
    val upperBound = avg * (1 + thresholdUp)
    val lowerBound = avg * (1 - thresholdDown)

    lowerBound <= metric && metric <= upperBound
  }

  def getStatusString(status: CheckStatus, metric: Double, avg: Double, thresholdUp: Double, thresholdDown: Double
                     ): String = {
    val upperBound = avg * (1 + thresholdUp)
    val lowerBound = avg * (1 - thresholdDown)

    status match {
      case CheckSuccess =>
        s"$lowerBound <= $metric <= $upperBound (with avg=$avg)"
      case CheckFailure =>
        s"$metric not in [$lowerBound,$upperBound] (with avg=$avg)(failed: Should be avg * (1 + lowerBound) <= metricResult <= avg * (1 + upperBound))"
      case CheckError(throwable) =>
        s"Checking $metric error: $throwable"
      case _ => throw IllegalConstraintResultException(id)
    }
  }

  override def addMetricList(metrics: Seq[MetricResult]): AverageBoundRangeCheck =
    AverageBoundRangeCheck(id,
      description,
      metrics,
      rule,
      thresholdUpper,
      thresholdLower,
      timeWindow)

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

    val checkStatus = CheckUtil.tryToStatus[Double](
      Try(targetMetricResult.result),
      d => calculateCheck(d, avg, thresholdUpper, thresholdLower))

    val statusString =
      getStatusString(checkStatus, targetMetricResult.result, avg, thresholdUpper, thresholdLower)

    val checkMessage = CheckMessageGenerator(
      targetMetricResult,
      thresholdUpper,
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
      thresholdUpper,
      Some(avg * (1 - thresholdLower)),
      Some(avg * (1 + thresholdUpper)),
      checkStatus.stringValue,
      checkMessage.message
    )

    cr
  }

  val subType = "AVERAGE_BOUND_RANGE_CHECK"
}

