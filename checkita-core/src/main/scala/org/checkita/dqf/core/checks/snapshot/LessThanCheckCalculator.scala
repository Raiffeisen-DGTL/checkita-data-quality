package org.checkita.dqf.core.checks.snapshot

import org.checkita.dqf.core.Results.MetricCalculatorResult
import org.checkita.dqf.core.checks.CheckName

/**
 * `Less than` check calculator:
 * verifies that base metric result is less than either a given threshold or
 * a provided compare metric result.
 * @param checkId Check ID
 * @param baseMetric Base metric to check
 * @param compareMetric Metric to compare with
 * @param compareThreshold Threshold to compare with
 * @param isCritical Flag if check is critical
 */
case class LessThanCheckCalculator(checkId: String,
                                   baseMetric: String,
                                   compareMetric: Option[String],
                                   compareThreshold: Option[Double],
                                   isCritical: Boolean
                                  ) extends CompareCheckCalculator {
  val checkName: CheckName = CheckName.LessThan
  protected val compareFunc: (Double, Double) => Boolean = (x, y) => x < y
  protected val compareFuncSuccessRepr: (Double, Double) => String = (x, y) => s"Metrics value of $x < $y."
  protected val compareFuncFailureRepr: (Double, Double) => String = (x, y) => s"Metrics value of $x >= $y."
  protected val compareFuncString: String = "less than"
  protected val lBound: (Option[MetricCalculatorResult], Option[Double]) => Option[Double] = noneBound
  protected val uBound: (Option[MetricCalculatorResult], Option[Double]) => Option[Double] = compareResOption
}
