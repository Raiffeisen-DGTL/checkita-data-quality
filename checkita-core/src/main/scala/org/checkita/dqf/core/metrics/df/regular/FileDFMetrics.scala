package org.checkita.dqf.core.metrics.df.regular

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.checkita.dqf.core.metrics.MetricName
import org.checkita.dqf.core.metrics.df.DFMetricCalculator

object FileDFMetrics {

  case class RowCountDFMetricCalculator(metricId: String) extends DFMetricCalculator {
    val metricName: MetricName = MetricName.RowCount
    protected val emptyValue: Column = lit(0).cast(DoubleType)
    val columns: Seq[String] = Seq.empty
    def errorMessage: String = ""
    override protected def resultExpr(implicit colTypes: Map[String, DataType]): Column = lit(1)
    override protected def errorConditionExpr(implicit colTypes: Map[String, DataType]): Column = lit(false)
    override protected val resultAggregateFunction: Column => Column = count
  }
}
