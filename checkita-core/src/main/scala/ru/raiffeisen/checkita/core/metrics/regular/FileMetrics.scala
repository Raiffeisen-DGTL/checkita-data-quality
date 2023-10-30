package ru.raiffeisen.checkita.core.metrics.regular

import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.metrics.{MetricCalculator, MetricName}

/**
 * File metrics that can be applied to dataframes without reading data in columns
 */
object FileMetrics {
  /**
   * Calculates row count of the dataframe. Must never return any errors.
   *
   * @param cnt current count of rows
   * @return result map with keys:
   *         "ROW_COUNT"
   */
  case class RowCountMetricCalculator(cnt: Int,
                                      protected val status: CalculatorStatus = CalculatorStatus.Success,
                                      protected val failCount: Long = 0,
                                      protected val failMsg: String = "OK") extends MetricCalculator {
    
    // axillary constructor to init metric calculator:
    def this() = this(0)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator =
      RowCountMetricCalculator(cnt + 1)

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    override def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.RowCount.entryName -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator =
      RowCountMetricCalculator(cnt + m2.asInstanceOf[RowCountMetricCalculator].cnt)
  }
}
