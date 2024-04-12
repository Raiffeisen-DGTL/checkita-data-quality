package ru.raiffeisen.checkita.core.metrics.df.regular

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, count, lit, slice}
import ru.raiffeisen.checkita.core.metrics.MetricName
import ru.raiffeisen.checkita.core.metrics.df.GroupingDFMetricCalculator
import ru.raiffeisen.checkita.core.metrics.df.functions.api.collect_list_limit

/**
 * WARNING: All grouping dataframe metric calculators will group
 * dataset by the input metric columns. Thus, calculation of grouping
 * calculators will definitely involve data shuffling!
 * For large data sets shuffling may significantly slower the application
 * and would require more resources to be completed.
 *
 * Use grouping calculators with caution!
 */
object GroupingDFMetrics {

  /**
   * Calculates count of distinct values in processed elements
   *
   * @note If exact result is not mandatory, then it's better to use
   *       HyperLogLog-based metric calculator called "APPROXIMATE_DISTINCT_VALUES".
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   */
  case class DistinctValuesDFMetricCalculator(metricId: String,
                                              columns: Seq[String]) extends GroupingDFMetricCalculator {

    override val metricName: MetricName = MetricName.DistinctValues

    /**
     * Spark expression yielding numeric result for each row being processed per each group.
     * Metric will be incremented with this result per each group using associated per-group aggregation function.
     *
     * Thus, for the purpose of finding number of distinct values, we just need to assign each group with
     * value 1 in order to count number of groups during final aggregation stage.
     *
     * @return Spark row-level expression yielding numeric result.
     * @note Spark expression MUST process single row but not aggregate multiple rows.
     */
    override protected def resultExpr: Column = lit(1)

    /**
     * Function that aggregates metric increments into intermediate per-group metric results.
     * Accepts spark expression `groupResultExpr` as input and returns another
     * spark expression that will yield aggregated double metric result per each group.
     *
     * As each group will have value 1 assigned to it, then any aggregation is excessive.
     * Thus, identity function is used as aggregation function in order to yield the same result.
     */
    override protected val groupAggregationFunction: Column => Column = identity

    /**
     * Error message that will be returned when metric increment fails.
     *
     * Despite null values are also taken into consideration and counted as a separate value,
     * the error data is collected for rows where some of the columns values are null.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String = "Some of the column values are null."

    /**
     * Collect error data for groups where at least on of the column values is null.
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column =
      columns.map(c => col(c).isNull).foldLeft(lit(false))(_ || _)

    /**
     * Per-group error collection expression: collects row data for ENTIRE group in case of metric error.
     * The size of array is limited by maximum allowed error dump size parameter.
     *
     * @param rowData       Array of row data from columns related to this metric calculator for current group.
     *                      (source keyFields + metric columns + window start time column for streaming applications)
     * @param errorDumpSize Maximum allowed number of errors to be collected per single metric.
     * @return Spark expression that will yield array of row data per group in case of metric error.
     */
    override protected def groupErrorExpr(rowData: Column, errorDumpSize: Int): Column =
      collect_list_limit(rowData, errorDumpSize)

  }


  /**
   * Calculates number of duplicate values for given column or tuple of columns.
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   */
  case class DuplicateValuesDFMetricCalculator(metricId: String,
                                               columns: Seq[String]) extends GroupingDFMetricCalculator {

    override val metricName: MetricName = MetricName.DuplicateValues

    /**
     * Spark expression yielding numeric result for each row being processed per each group.
     * Metric will be incremented with this result per each group using associated per-group aggregation function.
     *
     * Thus, for the purpose of finding number of duplicate values, we just need to assign each row
     * with value 1 in order to count number of rows per-group each group.
     *
     * @return Spark row-level expression yielding numeric result.
     * @note Spark expression MUST process single row but not aggregate multiple rows.
     */
    override protected def resultExpr: Column = lit(1)

    /**
     * Function that aggregates metric increments into intermediate per-group metric results.
     * Accepts spark expression `groupResultExpr` as input and returns another
     * spark expression that will yield aggregated double metric result per each group.
     *
     * As each group represents a distinct tuple value of selected columns then,
     * in order to find number of duplicates it is just needed to count number of rows
     * per each group. If group contains only one row, then it represents a unique
     * value in a dataset.
     */
    override protected val groupAggregationFunction: Column => Column =
      rowValue => count(rowValue) - lit(1)

    /**
     * Error message that will be returned when duplicate values are found.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String = "Duplicates found."

    /**
     * Collect error data for groups which contain more than 1 row i.e. group has duplicates.
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column = groupAggregationFunction(resultExpr) > lit(0)

    /**
     * Per-group error collection expression: collects row data for ENTIRE group in case of metric error.
     * First element of row data array is removed as it is represents a unique value in the dataset, i.e.
     * if group contains only one row, then error data should be empty.
     *
     * The size of array is limited by maximum allowed error dump size parameter.
     *
     * @param rowData       Array of row data from columns related to this metric calculator for current group.
     *                      (source keyFields + metric columns + window start time column for streaming applications)
     * @param errorDumpSize Maximum allowed number of errors to be collected per single metric.
     * @return Spark expression that will yield array of row data per group in case of metric error.
     */
    override protected def groupErrorExpr(rowData: Column, errorDumpSize: Int): Column =
      slice(collect_list_limit(rowData, errorDumpSize + 1), 2, errorDumpSize)

  }
}
