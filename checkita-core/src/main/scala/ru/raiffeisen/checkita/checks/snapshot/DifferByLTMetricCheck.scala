package ru.raiffeisen.checkita.checks.snapshot

import ru.raiffeisen.checkita.checks._
import ru.raiffeisen.checkita.exceptions.IllegalConstraintResultException
import ru.raiffeisen.checkita.metrics.MetricResult
import ru.raiffeisen.checkita.utils.DQSettings

import scala.util.Try


/**
 * Checks if relative error between two metrics less or eq than threshold
 *
 * @param id check id
 * @param description description
 * @param metrics used metric
 * @param compareMetric metric which will be the base of relative error
 * @param threshold required threshold level
 * @param settings dataquality configuration
 */
case class DifferByLTMetricCheck(
                                  id: String,
                                  description: String,
                                  metrics: Seq[MetricResult],
                                  compareMetric: String,
                                  threshold: Double
                                )(implicit settings: DQSettings) extends Check {

  override def metricsList: Seq[MetricResult] = metrics

  override def addMetricList(metrics: Seq[MetricResult]): Check = {
    DifferByLTMetricCheck(id, description, metrics, compareMetric, threshold)
  }

  override def run(): CheckResult = {

    require(metrics.size == 2)

    val compareMetricResult = metrics.filter(_.metricId == compareMetric).head

    val metricResult = metrics.filter(_.metricId != compareMetric).head

    val checkStatus = CheckUtil.tryToStatus[Double](
      Try(metricResult.result),
      d =>
        Math.abs(d - compareMetricResult.result) / compareMetricResult.result <= threshold)

    val statusString = checkStatus match {
      case CheckSuccess =>
        s"Relative error ${Math.abs(metricResult.result - compareMetricResult.result) / compareMetricResult.result} <= $threshold"
      case CheckFailure =>
        s"Relative error ${Math.abs(metricResult.result - compareMetricResult.result) / compareMetricResult.result} > $threshold (failed: Should be less or equal)"
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
        Some((1 - threshold) * compareMetricResult.result),
        Some((1 + threshold) * compareMetricResult.result),
        checkStatus.stringValue,
        checkMessage.message
      )

    cr
  }

  val subType = "DIFFER_BY_LT"
}
