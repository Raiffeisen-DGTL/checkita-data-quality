package org.checkita.dqf.core.checks.trend

import org.checkita.dqf.config.Enums.TrendCheckRule
import org.checkita.dqf.core.Results.MetricCalculatorResult
import org.checkita.dqf.core.checks.CheckName

/**
 * `Average bound LOWER` check calculator: verifies if metric value is greater than (or equals to)
 * its historical average value factored down by a given threshold.
 * @param checkId Check ID
 * @param baseMetric Base metric ID
 * @param compareThreshold Lower threshold
 * @param isCritical Flag if check is critical
 * @param rule Rule to build time window (either record or duration)
 * @param windowSize Size of the window to pull historical results
 * @param windowOffset Offset current date/record
 */
case class AverageBoundLowerCheckCalculator(checkId: String,
                                            baseMetric: String,
                                            compareThreshold: Double,
                                            isCritical: Boolean,
                                            rule: TrendCheckRule,
                                            windowSize: String,
                                            windowOffset: Option[String]
                                           ) extends AverageBoundCheckCalculator {

  override val lThreshold: Option[Double] = Some(compareThreshold)
  override val uThreshold: Option[Double] = None
  override val checkName: CheckName = CheckName.AverageBoundLower

  override protected val compareFuncSuccessRepr: (Double, Double) => String = (base, avg) =>
    s"Metric value of $base >= ${lBound(avg).getOrElse("Nan")} (lower bound)" +
      s" obtained from average metric result of $avg and threshold of $compareThreshold."

  override protected val compareFuncFailureRepr: (Double, Double) => String = (base, avg) =>
    s"Metric value of $base < ${lBound(avg).getOrElse("Nan")} (lower bound)" +
      s" obtained from average metric result of $avg and threshold of $compareThreshold."

  /**
   * Gets check details message to insert into final check message.
   *
   * @param compareMetricResult Compare metric result
   * @return Check details message
   */
  protected def getDetailsMsg(compareMetricResult: Option[MetricCalculatorResult]): Option[String] =
    lThreshold.map(
      lt => s"greater than (or equals to) average metric value over a given window factored down by (threshold) $lt."
    )

}
