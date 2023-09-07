package ru.raiffeisen.checkita.metrics.file

import ru.raiffeisen.checkita.metrics.MetricCalculator

/**
 * File metrics that can be applied to dataframes
 */
object FileMetrics {

  /**
   * Calculates row count of the dataframe
   * @param cnt current count of rows
   *
   * @return result map with keys:
   *   "ROW_COUNT"
   */
  case class RowCountMetricCalculator(cnt: Int) extends MetricCalculator {

    override def increment(values: Seq[Any]): MetricCalculator =
      RowCountMetricCalculator(cnt + 1)

    override def result(): Map[String, (Double, Option[String])] =
      Map("ROW_COUNT" -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator =
      RowCountMetricCalculator(
        this.cnt + m2.asInstanceOf[RowCountMetricCalculator].cnt)
  }
}
