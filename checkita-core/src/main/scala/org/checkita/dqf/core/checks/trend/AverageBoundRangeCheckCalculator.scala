package org.checkita.dqf.core.checks.trend

import org.checkita.dqf.config.Enums.TrendCheckRule
import org.checkita.dqf.core.Results.MetricCalculatorResult
import org.checkita.dqf.core.checks.CheckName

/**
 * `Average bound RANGE` check calculator: verifies if metric value is greater than (or equals to)
 * its historical average value factored down by a given lower threshold and less than (or equals to)
 * the one factored up by upper threshold.
 * @param checkId Check ID
 * @param baseMetric Base metric ID
 * @param compareThresholdLower Lower threshold
 * @param compareThresholdUpper Upper threshold
 * @param isCritical Flag if check is critical
 * @param rule Rule to build time window (either record or duration)
 * @param windowSize Size of the window to pull historical results
 * @param windowOffset Offset current date/record
 */
case class AverageBoundRangeCheckCalculator(checkId: String,
                                            baseMetric: String,
                                            compareThresholdLower: Double,
                                            compareThresholdUpper: Double,
                                            isCritical: Boolean,
                                            rule: TrendCheckRule,
                                            windowSize: String,
                                            windowOffset: Option[String]
                                           ) extends AverageBoundCheckCalculator {

  override val lThreshold: Option[Double] = Some(compareThresholdLower)
  override val uThreshold: Option[Double] = Some(compareThresholdUpper)
  override val checkName: CheckName = CheckName.AverageBoundRange

  override protected val compareFuncSuccessRepr: (Double, Double) => String = (base, avg) =>
    s"Metric value of $base is within interval [${lBound(avg).getOrElse("Nan")}, ${uBound(avg).getOrElse("NaN")}]" +
      s" obtained from average metric result of $avg, lower threshold of $compareThresholdLower and " +
      s"upper threshold of $compareThresholdUpper."

  override protected val compareFuncFailureRepr: (Double, Double) => String = (base, avg) =>
    s"Metric value of $base is outside of interval [${lBound(avg).getOrElse("Nan")}, ${uBound(avg).getOrElse("NaN")}]" +
      s" obtained from average metric result of $avg, lower threshold of $compareThresholdLower and " +
      s"upper threshold of $compareThresholdUpper."

  /**
   * Gets check details message to insert into final check message.
   *
   * @param compareMetricResult Compare metric result
   * @return Check details message
   */
  protected def getDetailsMsg(compareMetricResult: Option[MetricCalculatorResult]): Option[String] = for {
    lt <- lThreshold
    ut <- uThreshold
  } yield s"greater than average metric value over a given window factored down by (thresholdLower) $lt " +
    s"and lower than one factored up by (thresholdUpper) $ut."

}
