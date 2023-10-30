package ru.raiffeisen.checkita.core.metrics

import org.apache.spark.util.AccumulatorV2
import ru.raiffeisen.checkita.core.CalculatorStatus

import scala.collection.mutable

object ErrorCollection {

  /**
   * Excerpt from dataframe with data for which metric returned failure (or error) status
   *
   * @param status  Metric status
   * @param message Metric failure (or error) message
   * @param rowData Row data for which metric yielded failure (or error):
   *                contains only excerpt from full dataframe for given columns
   */
  case class ErrorRow(status: CalculatorStatus, message: String, rowData: Seq[String])

  /**
   * Stores all failure (or errors) for a particular metric
   *
   * @param columns Columns for which data is collected: contains metric columns and source key fields
   * @param errors  Sequence of metric errors with corresponding rows data
   */
  case class MetricErrors(columns: Seq[String], errors: Seq[ErrorRow])

  /**
   * Defines metric status
   *
   * @param id      Metric ID
   * @param status  Metric status
   * @param message Metric failure (or error) message
   */
  case class MetricStatus(id: String, status: CalculatorStatus, message: String)

  /**
   * Defines metric failure (or errors) in a way they are collected during metrics processing:
   *
   * @param columnNames   Column names for which data is collected: contains metric columns and source key fields.
   * @param metricStatues Sequence of metric statuses (all metrics that conform to given sequence of columns)
   * @param rowData       Row data for which metrics yielded failures (or errors):
   *                      contains only excerpt from full dataframe for given columns
   */
  case class AccumulatedErrors(
                                columnNames: Seq[String],
                                metricStatues: Seq[MetricStatus],
                                rowData: Seq[String]
                              )

  /**
   * Spark accumulator to collect metric errors
   *
   * @param out Metric errors buffer
   */
  final case class ErrorAccumulator(out: mutable.ArrayBuffer[AccumulatedErrors])
    extends AccumulatorV2[AccumulatedErrors, mutable.ArrayBuffer[AccumulatedErrors]] {

    override def isZero: Boolean = value.isEmpty

    override def copy(): AccumulatorV2[AccumulatedErrors, mutable.ArrayBuffer[AccumulatedErrors]] =
      ErrorAccumulator(value)

    override def reset(): Unit = value.clear()

    override def add(v: AccumulatedErrors): Unit = ErrorAccumulator(value += v)

    override def merge(other: AccumulatorV2[AccumulatedErrors, mutable.ArrayBuffer[AccumulatedErrors]]): Unit =
      ErrorAccumulator(this.value ++= other.value)

    override def value: mutable.ArrayBuffer[AccumulatedErrors] = out
  }
}
