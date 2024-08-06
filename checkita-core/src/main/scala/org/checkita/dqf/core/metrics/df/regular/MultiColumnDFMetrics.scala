package org.checkita.dqf.core.metrics.df.regular

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.checkita.dqf.core.metrics.MetricName
import org.checkita.dqf.core.metrics.df.functions.api.comoment
import org.checkita.dqf.core.metrics.df.{DFMetricCalculator, ReversibleDFCalculator}

object MultiColumnDFMetrics {

  /**
   * Abstract class for conditional multi-column DF metric calculators.
   * Specific of multi-column conditional metrics is that metric condition is a
   * function of all input columns and need to be applied to all of them at once.
   * Unlike for columns which can work with arbitrary number of columns where
   * metric condition is applied to each of the input column separately.
   *
   * All conditional metrics are reversible: direct error collection logic
   * implies metric increment fails when condition is not met.
   * Correspondingly, for reversed error collection logic, metric increment
   * fails when condition IS met.
   */
  abstract class MultiColumnConditionalDFCalculator extends DFMetricCalculator with ReversibleDFCalculator {
    /**
     * Value which is returned when metric result is null.
     */
    override protected val emptyValue: Column = lit(0).cast(DoubleType)

    /**
     * Create spark expression which checks if metric condition applied to multiple columns is met.
     */
    protected def metricCondExpr: Column

    /**
     * Spark expression yielding numeric result for processed row.
     * Metric will be incremented with this result using associated aggregation function.
     *
     * @return Spark row-level expression yielding numeric result.
     * @note Spark expression MUST process single row but not aggregate multiple rows.
     */
    override protected def resultExpr: Column = when(metricCondExpr, lit(1)).otherwise(lit(0))

    /**
     * Spark expression yielding boolean result for processed row.
     * Indicates whether metric increment failed or not.
     *
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column = if (reversed) metricCondExpr else !metricCondExpr

    /**
     * Aggregation function for all conditional metrics is just a summation.
     */
    override protected val resultAggregateFunction: Column => Column = sum
  }

  /**
   * Calculates population covariance between values of two columns
   *
   * @param metricId   Id of the metric.
   * @param columns    Sequence of columns which are used for metric calculation
   *
   * @note Differs from RDD calculator in terms of processing values that are not numbers:
   *       RDD calculator will yield NaN if at least one value cannot be cast to Double.
   *       DF calculator just skips rows where some of the values cannot be cast to Duble.
   */
  case class CovarianceDFMetricCalculator(metricId: String,
                                          columns: Seq[String]) extends DFMetricCalculator {

    assert(columns.size == 2, "covariance metric works with two columns only!")

    override val metricName: MetricName = MetricName.Covariance

    /**
     * Error message that will be returned when column value cannot be cast to Double type.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String = "Some of the provided values cannot be cast to number."

    /**
     * Value which is returned when metric result is null.
     */
    override protected val emptyValue: Column = lit(Double.NaN)

    /**
     * Retrieves double value from left column.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    override protected def resultExpr: Column = col(columns.head).cast(DoubleType)

    /**
     * Second row-level expression that retrieves double value from right column.
     * @return Spark row-level expression yielding numeric result.
     */
    private def resultExprRight: Column = col(columns.tail.head).cast(DoubleType)

    /**
     * If casting value to DoubleType yields null for any column,
     * then it is a signal that value is not a number.
     * Thus, covariance computation can't be incremented for this row.
     * This is a metric increment failure.
     *
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column = resultExpr.isNull || resultExprRight.isNull

    /**
     * Aggregation function for covariance metric is covar_pop Spark function.
     * This function skips rows where value in other of the columns is null i.e. cannot be cast to double type.
     */
    override protected val resultAggregateFunction: Column => Column =
      leftColumn => covar_pop(leftColumn, resultExprRight)
  }

  /**
   * Calculates sample covariance (covariance with Bessel's correction) between values of two columns
   *
   * @param metricId   Id of the metric.
   * @param columns    Sequence of columns which are used for metric calculation
   *
   * @note Differs from RDD calculator in terms of processing values that are not numbers:
   *       RDD calculator will yield NaN if at least one value cannot be cast to Double.
   *       DF calculator just skips rows where some of the values cannot be cast to Duble.
   */
  case class CovarianceBesselDFMetricCalculator(metricId: String,
                                                columns: Seq[String]) extends DFMetricCalculator {

    assert(columns.size == 2, "covarianceBessel metric works with two columns only!")

    override val metricName: MetricName = MetricName.CovarianceBessel

    /**
     * Error message that will be returned when column value cannot be cast to Double type.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String = "Some of the provided values cannot be cast to number."

    /**
     * Value which is returned when metric result is null.
     */
    override protected val emptyValue: Column = lit(Double.NaN)

    /**
     * Retrieves double value from left column.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    override protected def resultExpr: Column = col(columns.head).cast(DoubleType)

    /**
     * Second row-level expression that retrieves double value from right column.
     * @return Spark row-level expression yielding numeric result.
     */
    private def resultExprRight: Column = col(columns.tail.head).cast(DoubleType)

    /**
     * If casting value to DoubleType yields null for any column,
     * then it is a signal that value is not a number.
     * Thus, covariance computation can't be incremented for this row.
     * This is a metric increment failure.
     *
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column = resultExpr.isNull || resultExprRight.isNull

    /**
     * Aggregation function for covarianceBessel metric is covar_samp Spark function.
     * This function skips rows where value in other of the columns is null i.e. cannot be cast to double type.
     */
    override protected val resultAggregateFunction: Column => Column =
      leftColumn => covar_samp(leftColumn, resultExprRight)
  }

  /**
   * Calculates co-moment between values of two columns
   *
   * @param metricId   Id of the metric.
   * @param columns    Sequence of columns which are used for metric calculation
   *
   * @note Differs from RDD calculator in terms of processing values that are not numbers:
   *       RDD calculator will yield NaN if at least one value cannot be cast to Double.
   *       DF calculator just skips rows where some of the values cannot be cast to Duble.
   */
  case class CoMomentDFMetricCalculator(metricId: String,
                                        columns: Seq[String]) extends DFMetricCalculator {

    assert(columns.size == 2, "coMoment metric works with two columns only!")

    override val metricName: MetricName = MetricName.CoMoment

    /**
     * Error message that will be returned when column value cannot be cast to Double type.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String = "Some of the provided values cannot be cast to number."

    /**
     * Value which is returned when metric result is null.
     */
    override protected val emptyValue: Column = lit(Double.NaN)

    /**
     * Retrieves double value from left column.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    override protected def resultExpr: Column = col(columns.head).cast(DoubleType)

    /**
     * Second row-level expression that retrieves double value from right column.
     * @return Spark row-level expression yielding numeric result.
     */
    private def resultExprRight: Column = col(columns.tail.head).cast(DoubleType)

    /**
     * If casting value to DoubleType yields null for any column,
     * then it is a signal that value is not a number.
     * Thus, covariance computation can't be incremented for this row.
     * This is a metric increment failure.
     *
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column = resultExpr.isNull || resultExprRight.isNull

    /**
     * Aggregation function for covariance metric is covar_pop Spark function.
     * This function skips rows where value in other of the columns is null i.e. cannot be cast to double type.
     * In order to convert covariance value to co-moment value we need to multiply the former one with number of
     * rows where both of the column values can be cast to DoubleType.
     */
    override protected val resultAggregateFunction: Column => Column =
      leftColumn => comoment(leftColumn, resultExprRight)
  }

  /**
   * Calculates amount rows where elements in the given columns are equal
   *
   * @param metricId   Id of the metric.
   * @param columns    Sequence of columns which are used for metric calculation
   * @param reversed   Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class ColumnEqDFMetricCalculator(metricId: String,
                                        columns: Seq[String],
                                        protected val reversed: Boolean) extends MultiColumnConditionalDFCalculator {

    assert(columns.size > 1, "columnEq metric requires at least two columns!")

    override val metricName: MetricName = MetricName.ColumnEq

    /**
     * For direct error collection logic rows where string values in requested columns
     * are not equal are considered as metric failure
     * For reverses error collection logic rows where string values in requested columns
     * ARE equal are considered as metric failure
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) "Provided values ARE equal."
      else "Some of the provided values cannot be cast to string or provided values are not equal."

    /**
     * Create spark expression which checks if string representation of values in requested columns is the same
     * i.e. columns are equal.
     *
     * @note Check is not null safe, i.e. if at least one value in requested columns is null, than metric condition
     *       will not be met.
     */
    override protected def metricCondExpr: Column =
      columns.sliding(2).foldLeft(lit(true)) { (res, cols) =>
        res && (col(cols.head).cast(StringType) === col(cols.tail.head).cast(StringType))
      }
  }

  /**
   * Calculates the number of the rows for which the day difference between
   * two columns given as input is less than the threshold (number of days)
   *
   * @param metricId   Id of the metric.
   * @param columns    Sequence of columns which are used for metric calculation
   * @param dateFormat Date format for values in columns
   * @param threshold  Maximum allowed day distance between dates in columns
   * @param reversed   Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class DayDistanceDFMetricCalculator(metricId: String,
                                           columns: Seq[String],
                                           dateFormat: String,
                                           threshold: Int,
                                           protected val reversed: Boolean) extends MultiColumnConditionalDFCalculator {

    assert(columns.size == 2, "dayDistance metric works with two columns only!")

    override val metricName: MetricName = MetricName.DayDistance

    /**
     * For direct error collection logic rows where date distance between two dates is
     * greater than or equal to provided threshold are considered as metric failure.
     * For reverses error collection logic rows where date distance between two dates is
     * less than provided threshold are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Distance between two dates lower than given threshold of '$threshold'."
      else s"Some of the provided values cannot be cast to date with given format of '$dateFormat' " +
        s"or distance between two dates is greater than or equal to given threshold of '$threshold'."

    /**
     * Row-level spark expression that retrieves day-distance between column values and
     * checks if it is less than provided threshold.
     */
    override protected def metricCondExpr: Column = coalesce(abs(datediff(
      to_timestamp(col(columns.head), dateFormat),
      to_timestamp(col(columns.tail.head), dateFormat),
    )), lit(Int.MaxValue)) < lit(threshold)
  }

  /**
   * Calculates amount of rows where Levenshtein distance between 2 columns is less than threshold.
   *
   * @param metricId  Id of the metric.
   * @param columns   Sequence of columns which are used for metric calculation
   * @param threshold Threshold (should be within [0, 1] range for normalized results)
   * @param normalize Flag to define whether distance should be normalized over maximum length of two input strings
   * @param reversed  Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class LevenshteinDistanceDFMetricCalculator(metricId: String,
                                                   columns: Seq[String],
                                                   threshold: Double,
                                                   normalize: Boolean,
                                                   protected val reversed: Boolean)
    extends MultiColumnConditionalDFCalculator {

    assert(columns.size == 2, "levenshteinDistance metric works with two columns only!")
    assert(
      !normalize || (0 <= threshold && threshold <= 1),
      "levenshteinDistance metric threshold should be within [0, 1] range when normalized set to true."
    )

    override val metricName: MetricName = MetricName.LevenshteinDistance

    /**
     * For direct error collection logic rows where levenshtein distance between two string values
     * is greater than or equal to the provided threshold are considered as metric failure.
     * For reverses error collection logic rows where levenshtein distance between two string values
     * is lower than the provided threshold are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Levenshtein distance for given values is lower than given threshold of '$threshold'."
      else "Some of the provided values cannot be cast to string " +
        s"or levenshtein distance for given values is grater than or equal to given threshold of '$threshold'."

    private def maxStrLength: Column = greatest(
      col(columns.head).cast(StringType),
      col(columns.tail.head).cast(StringType)
    )

    private def levDistance: Column = levenshtein(
      upper(col(columns.head).cast(StringType)),
      upper(col(columns.tail.head).cast(StringType))
    )

    /**
     * Row-level spark expression that retrieves day-distance between column values and
     * checks if it is less than provided threshold.
     */
    override protected def metricCondExpr: Column =
      if (normalize) coalesce(levDistance / maxStrLength, lit(1.1)) < lit(threshold)
      else coalesce(levDistance, lit(Int.MaxValue).cast(DoubleType)) < lit(threshold)
  }

}
