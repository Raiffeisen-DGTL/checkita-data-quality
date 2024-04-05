package ru.raiffeisen.checkita.core.metrics

import org.apache.spark.sql.DataFrame
import ru.raiffeisen.checkita.core.Results.MetricCalculatorResult
import ru.raiffeisen.checkita.core.metrics.composed.ComposedMetricCalculator

import scala.annotation.tailrec

/**
 * Basic trait for metric processors.
 * Both RDD and DF metric processors will inherit this one.
 */
trait BasicMetricProcessor {

  /**
   * Builds map column name -> column index for given dataframe
   *
   * @param df Spark Dataframe
   * @return Map(column name -> column index)
   */
  protected def getColumnIndexMap(df: DataFrame): Map[String, Int] =
    df.schema.fieldNames.map(s => s -> df.schema.fieldIndex(s)).toMap

  /**
   * Builds map column index -> column name for given dataframe
   *
   * @param df Spark Dataframe
   * @return Map(column index -> column name)
   */
  protected def getColumnNamesMap(df: DataFrame): Map[Int, String] =
    df.schema.fieldNames.map(s => df.schema.fieldIndex(s) -> s).toMap

}
object BasicMetricProcessor {

  /**
   * Type alias for calculated metric results in form of:
   *  - Map of metricId to a sequence of metric results for this metricId (some metrics yield multiple results).
   */
  type MetricResults = Map[String, Seq[MetricCalculatorResult]]


  /**
   * Process all composed metrics given already calculated metrics.
   *
   * @note TopN metric cannot be used in composed metric calculation and will be filtered out.
   * @param composedMetrics Sequence of composed metrics to process
   * @param computedMetrics Sequence of computed metric results
   * @return
   */
  def processComposedMetrics(composedMetrics: Seq[ComposedMetric],
                             computedMetrics: Seq[MetricCalculatorResult]): MetricResults = {


    /**
     * Iterates over composed metric sequence and computes then.
     * Idea here is that previously computed composed metrics can be used as input for the next ones.
     *
     * @param composed Sequence of composed metrics to calculate
     * @param computed Sequence of already computed metrics (both source and composed ones)
     * @param results  Sequence of processed composed metrics
     * @return Sequence of composed metric results
     */
    @tailrec
    def loop(composed: Seq[ComposedMetric],
             computed: Seq[MetricCalculatorResult],
             results: Seq[MetricCalculatorResult] = Seq.empty): Seq[MetricCalculatorResult] = {
      if (composed.isEmpty) results
      else {
        val calculator = ComposedMetricCalculator(computed)
        val processedMetric = calculator.run(composed.head)
        loop(composed.tail, computed :+ processedMetric, results :+ processedMetric)
      }
    }

    loop(composedMetrics, computedMetrics.filterNot(_.metricName.startsWith(MetricName.TopN.entryName)))
      .groupBy(_.metricId)
  }
}
