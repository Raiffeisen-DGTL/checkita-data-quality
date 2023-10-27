package ru.raiffeisen.checkita.core.checks.trend

import ru.raiffeisen.checkita.config.Enums.TrendCheckRule
import ru.raiffeisen.checkita.core.Results.MetricCalculatorResult
import ru.raiffeisen.checkita.core.checks.CheckName

/**
 * `Average bound UPPER` check calculator: verifies if metric value is less than (or equals to)
 * its historical average value factored up by a given threshold.
 * @param checkId Check ID
 * @param baseMetric Base metric ID
 * @param compareThreshold Upper threshold
 * @param rule Rule to build time window (either record or duration)
 * @param windowSize Size of the window to pull historical results
 * @param windowOffset Offset current date/record
 */
case class AverageBoundUpperCheckCalculator(checkId: String,
                                            baseMetric: String,
                                            compareThreshold: Double,
                                            rule: TrendCheckRule,
                                            windowSize: String,
                                            windowOffset: Option[String]
                                           ) extends AverageBoundCheckCalculator {

  override val lThreshold: Option[Double] = None
  override val uThreshold: Option[Double] = Some(compareThreshold)
  override val checkName: CheckName = CheckName.AverageBoundUpper

  override protected val compareFuncSuccessRepr: (Double, Double) => String = (base, avg) =>
    s"Metric value of $base <= ${uBound(avg).getOrElse("Nan")} (upper bound)" +
      s" obtained from average metric result of $avg and threshold of $compareThreshold."

  override protected val compareFuncFailureRepr: (Double, Double) => String = (base, avg) =>
    s"Metric value of $base > ${uBound(avg).getOrElse("Nan")} (upper bound)" +
      s" obtained from average metric result of $avg and threshold of $compareThreshold."

  /**
   * Gets check details message to insert into final check message.
   *
   * @param compareMetricResult Compare metric result
   * @return Check details message
   */
  protected def getDetailsMsg(compareMetricResult: Option[MetricCalculatorResult]): Option[String] =
    uThreshold.map(
      ut => s"less than (or equals to) average metric value of a given window factored up by (threshold) $ut."
    )

}
