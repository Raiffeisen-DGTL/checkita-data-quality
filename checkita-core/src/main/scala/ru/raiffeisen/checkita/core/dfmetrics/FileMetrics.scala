package ru.raiffeisen.checkita.core.dfmetrics
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import ru.raiffeisen.checkita.core.metrics.MetricName

class FileMetrics {

  case class DFRowCountMetricCalculator(metricId: String) extends DFMetricCalculator {
    override val metricName: MetricName = MetricName.RowCount
    override val columns: Seq[String] = Seq.empty
    override val errorMessage: String = ""
    override protected val resultExpr: Column = lit(1)
    override protected val errorConditionExpr: Column = lit(false)
    override protected val resultAggregateFunction: Column => Column = count
  }
}
