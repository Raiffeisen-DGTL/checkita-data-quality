package ru.raiffeisen.checkita.core.metrics.df

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import ru.raiffeisen.checkita.core.metrics.MetricName
import ru.raiffeisen.checkita.core.metrics.df.Helpers._
import ru.raiffeisen.checkita.core.metrics.df.functions.api._

/**
 * Basic DF metric calculator
 *
 * @note DF Calculators are intendet to work with Batch applications only.
 *       Hence, their functionality may be revised in future to support streaming applications as well.
 */
abstract class DFMetricCalculator {

  /**
   * Unlike RDD calculators, DF calculators are not groped by its type.
   * For each metric defined in DQ job, there will be created its own instance of
   * DF calculator. Thus, DF metric calculators can be linked to metric definitions
   * by metricId.
   */
  val metricId: String
  val metricName: MetricName
  val columns: Seq[String]

  /**
   * Error message that will be returned when metric increment fails.
   * @return Metric increment failure message.
   */
  def errorMessage: String

  /**
   * Value which is returned when metric result is null.
   */
  protected val emptyValue: Column

  /**
   * Spark expression yielding numeric result for processed row.
   * Metric will be incremented with this result using associated aggregation function.
   * @return Spark row-level expression yielding numeric result.
   *
   * @note Spark expression MUST process single row but not aggregate multiple rows.
   */
  protected def resultExpr: Column

  /**
   * Spark expression yielding boolean result for processed row.
   * Indicates whether metric increment failed or not. Usually
   * checks the outcome of `resultExpr`.
   * @return Spark row-level expression yielding boolean result.
   */
  protected def errorConditionExpr: Column

  /**
   * Function that aggregates metric increments into final metric value.
   * Accepts spark expression `resultExpr` as input and returns another
   * spark expression that will yield aggregated double metric result.
   */
  protected val resultAggregateFunction: Column => Column

  /**
   * Name of the column that will store metric result
   */
  val resultCol: String = addColumnSuffix(metricId, DFMetricOutput.Result.entryName)
  /**
   * Name of the column that will store metric errors
   */
  val errorsCol: String = addColumnSuffix(metricId, DFMetricOutput.Errors.entryName)

  /**
   * Row data collection expression: collects values of selected columns to array for
   * row where metric error occurred.
   *
   * @param keyFields   Sequence of source/stream key fields.
   * @return Spark expression that will yield array of row data for column related to this metric calculator.
   */
  protected def rowDataExpr(keyFields: Seq[String]): Column = {
    val allColumns = withKeyFields(columns, keyFields)
    array(allColumns.map(c => coalesce(col(c).cast(StringType), lit(""))): _*)
  }

  /**
   * Error collection expression: collects row data in case of metric error.
   *
   * @param rowData Array of row data from columns related to this metric calculator
   *                (source keyFields + metric columns + window start time column for streaming applications)
   * @return Spark expression that will yield row data in case of metric error.
   */
  protected def errorExpr(rowData: Column): Column =
    when(errorConditionExpr, rowData).otherwise(typedlit[Option[Array[String]]](None))


  /**
   * Final metric aggregation expression that MUST yield double value.
   *
   * @return Spark expression that will yield double metric calculator result
   */
  def result: Column = coalesce(
    resultAggregateFunction(resultExpr).cast(DoubleType),
    emptyValue
  ).as(resultCol)

  /**
   * Final metric errors aggregation expression.
   * Collects all metric errors into an array column.
   * The size of array is limited by maximum allowed error dump size parameter.
   *
   * @param errorDumpSize Maximum allowed number of errors to be collected per single metric.
   * @param keyFields     Sequence of source/stream key fields.
   * @return Spark expression that will yield array of metric errors.
   */
  def errors(implicit errorDumpSize: Int, keyFields: Seq[String]): Column = {
    val rowData = rowDataExpr(keyFields)
    collect_list_limit(errorExpr(rowData), errorDumpSize).as(errorsCol)
  }
}
