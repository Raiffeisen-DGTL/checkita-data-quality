package ru.raiffeisen.checkita.core.dfmetrics

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import ru.raiffeisen.checkita.core.dfmetrics.Helpers._
import ru.raiffeisen.checkita.core.dfmetrics.functions._
import ru.raiffeisen.checkita.core.metrics.MetricName

abstract class DFMetricCalculator {

  val metricId: String
  val metricName: MetricName
  val columns: Seq[String]
  val errorMessage: String

  protected def resultExpr: Column
  protected def errorConditionExpr: Column
  protected val resultAggregateFunction: Column => Column

  val resultCol: String = addColumnSuffix(metricId, DFMetricOutput.Result.entryName)
  val errorsCol: String = addColumnSuffix(metricId, DFMetricOutput.Errors.entryName)

  private def rowData(keyFields: Seq[String]): Column = {
    val allColumns = withKeyFields(columns, keyFields)
    array(allColumns.map(c => coalesce(col(c).cast(StringType), lit(""))): _*)
//    to_json(struct(allColumns.map(c => coalesce(col(c), lit("")).as(c)): _*))
  }

  private def errorExpr(keyFields: Seq[String]): Column =
    when(errorConditionExpr, rowData(keyFields)).otherwise(typedlit[Option[Array[String]]](None))


  /**
   * Final metric aggregation expression that MUST yield double value.
   * @return Double metric calculator result
   */
  def result: Column = resultAggregateFunction(resultExpr).cast(DoubleType).as(resultCol)

  /**
   * Final metric errors aggregation expression.
   * Collects all metric errors into an array column.
   * The size of array is limited by maximum allowed error dump size parameter.
   * @param errorDumpSize Maximum allowed number of errors to be collected per single metric.
   * @return Array of metric errors.
   */
  def errors(implicit errorDumpSize: Int, keyFields: Seq[String]): Column =
    collect_list_limit(errorExpr(keyFields), errorDumpSize).as(errorsCol)
}
