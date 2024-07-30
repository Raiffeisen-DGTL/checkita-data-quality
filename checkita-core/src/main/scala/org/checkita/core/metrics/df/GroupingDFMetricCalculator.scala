package org.checkita.core.metrics.df

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{coalesce, col, lit, sum, typedlit, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType}
import org.checkita.core.metrics.df.Helpers.{DFMetricOutput, addColumnSuffix, tripleBackticks}
import org.checkita.core.metrics.df.functions.api.{collect_list_limit, merge_list_limit}

/**
 * Base class for metric calculators that require data grouping by metric columns.
 * These metric calculators require data shuffle, and, therefore, are processed as
 * separate Spark Jobs.
 *
 * Since, we are groupng data by metric columns, then we have to define
 * two types of aggregation functions:
 *   - aggregation function used to aggregate intermediate metric result per each group.
 *   - aggregation function used to aggregate final metric result from intermediate per-group results.
 *
 * Each aggregation function is accompanied with rowData collection for metric increment errors.
 *
 * @note There are currently only two metric calculators that inherit from this class:
 *       distinctValues and duplicateValues. For both of them final aggregation function
 *       is just a summation of per-group results.
 */
abstract class GroupingDFMetricCalculator extends DFMetricCalculator {
  /**
   * Value which is returned when metric result is null.
   * As all grouping metric calculators use summation during aggregation
   * of intermediate per-group results then they should return zero
   * when applied to empty sequence.
   */
  protected val emptyValue: Column = lit(0).cast(DoubleType)

  /**
   * Name of the column that will store intermediate metric results per each group.
   */
  val groupResultCol: String = addColumnSuffix(metricId, DFMetricOutput.GroupResult.entryName)
  /**
   * Name of the column that will store intermediate metric errors per each group.
   */
  val groupErrorsCol: String = addColumnSuffix(metricId, DFMetricOutput.GroupErrors.entryName)

  /**
   * Function that aggregates metric increments into intermediate per-group metric results.
   * Accepts spark expression `groupResultExpr` as input and returns another
   * spark expression that will yield aggregated double metric result per each group.
   */
  protected val groupAggregationFunction: Column => Column


  /**
   * Grouping calculators will have different API for collecting per-group arrays with error data.
   * Therefore, an exception will be throw if this method is called.
   */
  override protected def errorExpr(rowData: Column): Column =
    throw new UnsupportedOperationException(
      "Grouping dataframe metric calculator uses special API to collect per-group errors first." +
        "Use `groupErrorExpr` method with signature: `(rowData: Column, errorDumpSize: Int) => Column`."
    )

  /**
   * Per-group error collection expression: collects row data for ENTIRE group in case of metric error.
   * The size of array is limited by maximum allowed error dump size parameter.
   *
   * @param rowData       Array of row data from columns related to this metric calculator for current group.
   *                      (source keyFields + metric columns + window start time column for streaming applications)
   * @param errorDumpSize Maximum allowed number of errors to be collected per single metric.
   * @return Spark expression that will yield array of row data per group in case of metric error.
   */
  protected def groupErrorExpr(rowData: Column, errorDumpSize: Int): Column

  /**
   * Per-group intermediate metric aggregation expression that MUST yield double value.
   *
   * @return Spark expression that will yield double metric calculator result
   */
  def groupResult: Column = groupAggregationFunction(resultExpr).cast(DoubleType).as(groupResultCol)

  /**
   * Per-group metric errors aggregation expression.
   * Collects all metric errors into an array column pear each group.
   * The size of array is limited by maximum allowed error dump size parameter.
   *
   * The main difference from one-pass DF-calculators is that error condition depends on
   * the intermediate group aggregation result rather than on individual row result.
   *
   * @param errorDumpSize Maximum allowed number of errors to be collected per single metric.
   * @param keyFields     Sequence of source/stream key fields.
   * @return Spark expression that will yield array of metric errors.
   * @note For streaming applications, we need to collect metric errors in per-window basis.
   *       Therefore, error row data has to contain window start time (first element of array).
   */
  def groupErrors(implicit errorDumpSize: Int, keyFields: Seq[String]): Column = {
    val rowData = rowDataExpr(keyFields)
    when(
      errorConditionExpr, groupErrorExpr(rowData, errorDumpSize)
    ).otherwise(lit(null).cast(ArrayType(ArrayType(StringType)))).as(groupErrorsCol)
  }


  /**
   * Function that aggregates intermediate metric per-group results into final metric value.
   * Default aggregation for grouping metrics it is just a summation.
   */
  protected val resultAggregateFunction: Column => Column = sum

  /**
   * Final metric aggregation expression that MUST yield double value.
   *
   * @return Spark expression that will yield double metric calculator result
   */
  override def result: Column = coalesce(
    resultAggregateFunction(col(tripleBackticks(groupResultCol))).cast(DoubleType),
    emptyValue
  ).as(resultCol)

  /**
   * Final metric errors aggregation expression.
   * Merges all per-group metric errors into final array of error data.
   * The size of array is limited by maximum allowed error dump size parameter.
   *
   * @param errorDumpSize Maximum allowed number of errors to be collected per single metric.
   * @return Spark expression that will yield array of metric errors.
   */
  override def errors(implicit errorDumpSize: Int, keyFields: Seq[String]): Column =
    merge_list_limit(col(tripleBackticks(groupErrorsCol)), errorDumpSize).as(errorsCol)

}
