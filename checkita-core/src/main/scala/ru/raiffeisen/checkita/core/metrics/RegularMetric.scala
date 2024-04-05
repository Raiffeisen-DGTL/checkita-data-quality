package ru.raiffeisen.checkita.core.metrics

import ru.raiffeisen.checkita.core.metrics.df.DFMetricCalculator
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricCalculator

/**
 * Base class for source metric. All source metrics should have defined following:
 *   - metric ID
 *   - metric name
 *   - source ID over which metric is calculated
 *   - columns used for metric calculation
 *   - method to init metric calculator
 */
trait RegularMetric extends Serializable {
  val metricId: String
  val metricName: MetricName
  val metricSource: String
  val metricColumns: Seq[String]

  def initRDDMetricCalculator: RDDMetricCalculator
//  def initDFMetricCalculator: DFMetricCalculator
}