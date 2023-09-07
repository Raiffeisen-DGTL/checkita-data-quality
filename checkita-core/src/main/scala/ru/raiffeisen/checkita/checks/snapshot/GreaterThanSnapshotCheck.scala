package ru.raiffeisen.checkita.checks.snapshot

import ru.raiffeisen.checkita.checks._
import ru.raiffeisen.checkita.exceptions.IllegalConstraintResultException
import ru.raiffeisen.checkita.metrics.MetricResult
import ru.raiffeisen.checkita.utils.DQSettings

import scala.util.Try

/**
 * Base compare function
 */
abstract class GreaterThanSnapshotCheck extends Check {

  def calculateCheck(base: Double, comparison: Double): Boolean = base > comparison

  val subType = "GREATER_THAN"
}

/**
 * Implementation for Metric VS Metric, Metric VS Threshold
 */

/**
 * Performs greater than check between metric and threshold
 * @param id check id
 * @param description description
 * @param metrics list of metrics (current case length = 1)
 * @param threshold required threshold level
 * @param settings dataquality configuration
 */
case class GreaterThanThresholdCheck(
                                      id: String,
                                      description: String,
                                      metrics: Seq[MetricResult],
                                      threshold: Double
                                    )(implicit settings: DQSettings) extends GreaterThanSnapshotCheck {

  override def metricsList: Seq[MetricResult] = metrics

  override def addMetricList(metrics: Seq[MetricResult]): Check = {
    GreaterThanThresholdCheck(id, description, metrics, threshold)
  }

  override def run(): CheckResult = {

    require(metrics.size == 1)

    val metricResult = metrics.head

    val checkStatus = CheckUtil.tryToStatus[Double](
      Try(metricResult.result),
      d => calculateCheck(d, threshold))

    val statusString = checkStatus match {
      case CheckSuccess =>
        s"${metricResult.result} > $threshold"
      case CheckFailure =>
        s"${metricResult.result} <= $threshold (failed: Difference is ${threshold - metricResult.result})"
      case CheckError(throwable) =>
        s"Checking ${metricResult.result} = $threshold error: $throwable"
      case _ => throw IllegalConstraintResultException(id)
    }

    val checkMessage =
      CheckMessageGenerator(
        metricResult,
        threshold,
        checkStatus,
        statusString,
        id,
        subType
      )

    val cr =
      CheckResult(
        this.id,
        subType,
        this.description,
        metricResult.sourceId,
        metricResult.metricId,
        None,
        threshold,
        Some(threshold),
        None,
        checkStatus.stringValue,
        checkMessage.message
      )

    cr
  }
}

/**
 * Performs greater than check between metric and metric
 * @param description description
 * @param metrics list of metrics (current case length = 2)
 * @param settings dataquality configuration
 */
case class GreaterThanMetricCheck(
                                   id: String,
                                   description: String,
                                   metrics: Seq[MetricResult],
                                   compareMetric: String
                                 )(implicit settings: DQSettings) extends GreaterThanSnapshotCheck {

  override def metricsList: Seq[MetricResult] = metrics

  override def addMetricList(metrics: Seq[MetricResult]): Check = {
    GreaterThanMetricCheck(id, description, metrics, compareMetric)
  }

  override def run(): CheckResult = {

    require(metrics.size == 2)

    val compareMetricResult = metrics.filter(_.metricId == compareMetric).head

    val metricResult = metrics.filter(_.metricId != compareMetric).head

    val checkStatus = CheckUtil.tryToStatus[Double](
      Try(metricResult.result),
      d => calculateCheck(d, compareMetricResult.result))

    val statusString = checkStatus match {
      case CheckSuccess =>
        s"${metricResult.result} > ${compareMetricResult.result}"
      case CheckFailure =>
        s"${metricResult.result} <= ${compareMetricResult.result} (failed: Difference is ${compareMetricResult.result - metricResult.result})"
      case CheckError(throwable) =>
        s"Checking ${metricResult.result} = ${compareMetricResult.result} error: $throwable"
      case _ => throw IllegalConstraintResultException(id)
    }

    val checkMessage =
      CheckMessageGenerator(
        metricResult,
        compareMetricResult.result,
        checkStatus,
        statusString,
        id,
        subType
      )

    val cr =
      CheckResult(
        this.id,
        subType,
        this.description,
        metricResult.sourceId,
        metricResult.metricId,
        Some(compareMetricResult.metricId),
        compareMetricResult.result,
        Some(compareMetricResult.result),
        None,
        checkStatus.stringValue,
        checkMessage.message
      )

    cr
  }
}
