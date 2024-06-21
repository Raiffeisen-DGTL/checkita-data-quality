package ru.raiffeisen.checkita.core.metrics.df.regular

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import ru.raiffeisen.checkita.core.metrics.MetricName
import ru.raiffeisen.checkita.core.metrics.df.DFMetricCalculator

object FileDFMetrics {

  case class RowCountDFMetricCalculator(metricId: String) extends DFMetricCalculator {
    val metricName: MetricName = MetricName.RowCount
    protected val emptyValue: Column = lit(0).cast(DoubleType)
    val columns: Seq[String] = Seq.empty
    def errorMessage: String = ""
    protected val resultExpr: Column = lit(1)
    protected val errorConditionExpr: Column = lit(false)
    protected val resultAggregateFunction: Column => Column = count
  }
}
