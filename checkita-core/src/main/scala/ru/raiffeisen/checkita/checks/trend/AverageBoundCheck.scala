package ru.raiffeisen.checkita.checks.trend

import ru.raiffeisen.checkita.checks.{CheckError, CheckFailure, CheckStatus, CheckSuccess}
import ru.raiffeisen.checkita.exceptions.IllegalConstraintResultException
import ru.raiffeisen.checkita.metrics.MetricResult
import ru.raiffeisen.checkita.utils.DQSettings
import ru.raiffeisen.checkita.utils.io.dbmanager.DBManager


/**
 * Implementation of the trivial average based prediction
 */
trait AverageCheckDistanceCalculator {
  def calculatePrediction(results: Seq[Double]): Double =
    results.sum / results.length
}

/**
 * Checks if the real value within bounds [avg(1-threshold, avg(1+threshold)]
 * @param id check id
 * @param description check description
 * @param metrics List of metrics (should be one metric)
 * @param rule selection rule (date/record)
 * @param threshold requested threshold
 * @param timewindow result selection timewindow
 * @param resultsWriter local database manager
 * @param settings dataquality configuration
 */
case class AverageBoundFullCheck(id: String,
                                 description: String,
                                 metrics: Seq[MetricResult],
                                 rule: String,
                                 threshold: Double,
                                 timewindow: Int)(implicit resultsWriter: DBManager,
                                                  settings: DQSettings)
  extends TrendCheckCore(id,
    description,
    metrics,
    rule,
    threshold,
    timewindow)
    with AverageCheckDistanceCalculator {

  val subType = "AVERAGE_BOUND_FULL_CHECK"

  override def addMetricList(metrics: Seq[MetricResult]): AverageBoundFullCheck =
    AverageBoundFullCheck(id,
      description,
      metrics,
      rule,
      threshold,
      timewindow)

  override def calculateCheck(metric: Double,
                              avg: Double,
                              threshold: Double): Boolean = {
    val lowerBound = avg * (1 - threshold)
    val upperBound = avg * (1 + threshold)
    lowerBound <= metric && metric <= upperBound
  }

  override def getStatusString(status: CheckStatus,
                               metric: Double,
                               avg: Double,
                               threshold: Double): String = {
    val lowerBound = avg * (1 - threshold)
    val upperBound = avg * (1 + threshold)

    status match {
      case CheckSuccess =>
        s"$lowerBound <= $metric <= $upperBound (with avg=$avg)"
      case CheckFailure =>
        s"$metric not in [$lowerBound,$upperBound] (with avg=$avg)(failed: Should be avg * (1 - threshold) <= metricResult <= avg * (1 + threshold))"
      case CheckError(throwable) =>
        s"Checking $metric error: $throwable"
      case _ => throw IllegalConstraintResultException(id)
    }
  }
}

/**
 * Checks if the real value bigger than avg(1-threshold)
 * @param id check id
 * @param description check description
 * @param metrics List of metrics (should be one metric)
 * @param rule selection rule (date/record)
 * @param threshold requested threshold
 * @param timewindow result selection timewindow
 * @param resultsWriter local database manager
 * @param settings dataquality configuration
 */
case class AverageBoundLowerCheck(id: String,
                                  description: String,
                                  metrics: Seq[MetricResult],
                                  rule: String,
                                  threshold: Double,
                                  timewindow: Int)(
                                   implicit resultsWriter: DBManager,
                                   settings: DQSettings)
  extends TrendCheckCore(id,
    description,
    metrics,
    rule,
    threshold,
    timewindow)
    with AverageCheckDistanceCalculator {

  val subType = "AVERAGE_BOUND_LOWER_CHECK"

  override def addMetricList(metrics: Seq[MetricResult]): AverageBoundLowerCheck =
    AverageBoundLowerCheck(id,
      description,
      metrics,
      rule,
      threshold,
      timewindow)

  override def calculateCheck(metric: Double,
                              avg: Double,
                              threshold: Double): Boolean = {
    val lowerBound = avg * (1 - threshold)
    lowerBound <= metric
  }

  override def getStatusString(status: CheckStatus,
                               metric: Double,
                               avg: Double,
                               threshold: Double): String = {
    val lowerBound = avg * (1 - threshold)

    status match {
      case CheckSuccess =>
        s"$metric >= $lowerBound (with avg=$avg)"
      case CheckFailure =>
        s"$metric < $lowerBound (with avg=$avg)(failed: Should be metricResult >= avg * (1 - threshold))"
      case CheckError(throwable) =>
        s"Checking $metric error: $throwable"
      case _ => throw IllegalConstraintResultException(id)
    }
  }
}

/**
 * Checks if the real value less than avg(1+threshold)
 * @param id check id
 * @param description check description
 * @param metrics List of metrics (should be one metric)
 * @param rule selection rule (date/record)
 * @param threshold requested threshold
 * @param timewindow result selection timewindow
 * @param resultsWriter local database manager
 * @param settings dataquality configuration
 */
case class AverageBoundUpperCheck(id: String,
                                  description: String,
                                  metrics: Seq[MetricResult],
                                  rule: String,
                                  threshold: Double,
                                  timewindow: Int)(
                                   implicit resultsWriter: DBManager,
                                   settings: DQSettings)
  extends TrendCheckCore(id,
    description,
    metrics,
    rule,
    threshold,
    timewindow)
    with AverageCheckDistanceCalculator {

  val subType = "AVERAGE_BOUND_UPPER_CHECK"

  override def addMetricList(metrics: Seq[MetricResult]): AverageBoundUpperCheck =
    AverageBoundUpperCheck(id,
      description,
      metrics,
      rule,
      threshold,
      timewindow)

  override def calculateCheck(metric: Double,
                              avg: Double,
                              threshold: Double): Boolean = {
    val upperBound = avg * (1 + threshold)
    metric <= upperBound
  }

  override def getStatusString(status: CheckStatus,
                               metric: Double,
                               avg: Double,
                               threshold: Double): String = {
    val upperBound = avg * (1 + threshold)

    status match {
      case CheckSuccess =>
        s"$metric <= $upperBound (with avg=$avg)"
      case CheckFailure =>
        s"$metric > $upperBound (with avg=$avg)(failed: Should be metricResult <= avg * (1 + threshold))"
      case CheckError(throwable) =>
        s"Checking $metric error: $throwable"
      case _ => throw IllegalConstraintResultException(id)
    }
  }
}

