package ru.raiffeisen.checkita.core.metrics.rdd.regular

import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.metrics.MetricName
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricCalculator

/**
 * File metrics that can be applied to dataframes without reading data in columns
 */
object FileRDDMetrics {
  /**
   * Calculates row count of the dataframe. Must never return any errors.
   *
   * @param cnt current count of rows
   * @return result map with keys:
   *         "ROW_COUNT"
   */
  case class RowCountRDDMetricCalculator(cnt: Int,
                                         protected val status: CalculatorStatus = CalculatorStatus.Success,
                                         protected val failCount: Long = 0,
                                         protected val failMsg: String = "OK") extends RDDMetricCalculator {
    
    // axillary constructor to init metric calculator:
    def this() = this(0)
    
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator =
      RowCountRDDMetricCalculator(cnt + 1)

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    override def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.RowCount.entryName -> (cnt.toDouble, None))

    override def merge(m2: RDDMetricCalculator): RDDMetricCalculator =
      RowCountRDDMetricCalculator(cnt + m2.asInstanceOf[RowCountRDDMetricCalculator].cnt)
  }
}
