package org.checkita.core.metrics.df.regular

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
import org.checkita.core.metrics.MetricName
import org.checkita.core.metrics.df.DFMetricCalculator
import org.checkita.core.metrics.df.functions.api.{hll_count_distinct, space_saving_top_n}

/**
 * Metrics based on using HyperLogLog algorithm for estimating cardinality of data.
 */
object ApproxCardinalityDFMetrics {

  /**
   * Calculates number of distinct values in processed elements
   *
   * Works for single column only!
   *
   * @param metricId      Id of the metric.
   * @param columns       Sequence of columns which are used for metric calculation
   * @param accuracyError Error of calculation
   */
  case class ApproximateDistinctValuesDFMetricCalculator(metricId: String,
                                                         columns: Seq[String],
                                                         accuracyError: Double) extends DFMetricCalculator {

    assert(columns.size == 1, "approximateDistinctValues metric works for single column only!")

    override val metricName: MetricName = MetricName.ApproximateDistinctValues

    /**
     * Value which is returned when metric result is null.
     */
    override protected val emptyValue: Column = lit(0).cast(DoubleType)

    /**
     * Error message that will be returned when column value cannot be cast to string type.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String = "Provided value cannot be cast to string."

    /**
     * Retrieves string from requested column of row.
     *
     * @return Spark row-level expression yielding numeric result.
     * @note Spark expression MUST process single row but not aggregate multiple rows.
     */
    override protected def resultExpr: Column = col(columns.head).cast(StringType)

    /**
     * If casting value to StringType yields null, then it is a signal that value
     * is not a string. Thus, HyperLogLog computation can't be
     * incremented for this row. This is a metric increment failure.
     *
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * Use approx_count_distinct function which calculates data cardinality
     * using HyperLogLog++ algorithm.
     */
    override protected val resultAggregateFunction: Column => Column =
      rowValue => hll_count_distinct(rowValue, accuracyError)
  }

  /**
   * Calculates approximate completeness of incremental integer (long) sequence,
   * i.e. checks if sequence does not have missing elements.
   *
   * Works for single column only!
   *
   * @param metricId      Id of the metric.
   * @param columns       Sequence of columns which are used for metric calculation
   * @param accuracyError Error of calculation
   * @param increment     Sequence increment
   */
  case class ApproximateSequenceCompletenessDFMetricCalculator(metricId: String,
                                                               columns: Seq[String],
                                                               accuracyError: Double,
                                                               increment: Long) extends DFMetricCalculator {

    assert(columns.size == 1, "approximateSequenceCompleteness metric works for single column only!")

    override val metricName: MetricName = MetricName.ApproximateSequenceCompleteness

    /**
     * Value which is returned when metric result is null.
     */
    override protected val emptyValue: Column = lit(0).cast(DoubleType)

    /**
     * Error message that will be returned when column value cannot be cast to Long type.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String = "Provided value cannot be cast to Long."

    /**
     * Retrieves long value from requested column of row.
     *
     * @return Spark row-level expression yielding numeric result.
     * @note Spark expression MUST process single row but not aggregate multiple rows.
     */
    override protected def resultExpr: Column = col(columns.head).cast(LongType)

    /**
     * If casting value to LongType yields null, then it is a signal that value
     * is not a natural number. Thus, HyperLogLog computation can't be
     * incremented for this row. This is a metric increment failure.
     *
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * Use approx_count_distinct function which calculates data cardinality
     * using HyperLogLog++ algorithm.
     */
    override protected val resultAggregateFunction: Column => Column =
      rowValue => hll_count_distinct(rowValue, accuracyError).cast(DoubleType) / (
        (max(rowValue) - min(rowValue)).cast(DoubleType) / lit(increment) + lit(1)
      )
  }


  case class TopNDFMetricCalculator(metricId: String,
                                    columns: Seq[String],
                                    maxCapacity: Int,
                                    targetNumber: Int) extends DFMetricCalculator {

    assert(columns.size == 1, "topN metric works for single column only!")

    override val metricName: MetricName = MetricName.TopN

    /**
     * Error message that will be returned when column value cannot be cast to string.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String = "Provided value cannot be cast to string."

    /**
     * TopN metric return empty string as value and NaN as frequency when applied to empty sequence.
     */
    override protected val emptyValue: Column =
      array(struct(lit("").as("value"), lit(Double.NaN).as("frequency")))

    /**
     * Spark expression yielding numeric result for processed row.
     * Metric will be incremented with this result using associated aggregation function.
     *
     * @return Spark row-level expression yielding numeric result.
     * @note Spark expression MUST process single row but not aggregate multiple rows.
     */
    override protected def resultExpr: Column = col(columns.head).cast(StringType)

    /**
     * If casting value to StringType yields null, then it is a signal that value is not a string.
     * Thus, TopN computation can't be incremented for this row.
     * This is a metric increment failure.
     *
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * User custom aggregation function to find topN values based on SpaceSaver.
     */
    override protected val resultAggregateFunction: Column => Column =
      rowValue => space_saving_top_n(rowValue, targetNumber, maxCapacity)

    /**
     * Overriding result expression since for TopN metric the result is not a double value
     * but an array with top-N values from column along with their occurrence frequencies.
     *
     * @return Spark expression that will yield result of following type: `array(struct(string, double))`.
     */
    override def result: Column = coalesce(
      resultAggregateFunction(resultExpr),
      emptyValue
    ).as(resultCol)
  }


}
