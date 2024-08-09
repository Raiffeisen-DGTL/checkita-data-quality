package org.checkita.dqf.core.metrics

import org.checkita.dqf.core.metrics.df.DFMetricCalculator
import org.checkita.dqf.core.metrics.rdd.RDDMetricCalculator

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

  /**
   * Inits RDD metric calculator.
   * @return Initial state of RDD metric calculator.
   */
  def initRDDMetricCalculator: RDDMetricCalculator

  /**
   * Inits DF metric calculator.
   * @param columns Sequence of columns this metric refers to.
   * @return Initial state of DF metric calculator.
   *         
   * @note It is allowed to use `*` in order to refer all source columns.
   *       In this case value of metricColumns will be the sequence with only
   *       one element: `*`. It is required to expand the star with actual list
   *       of source columns. But that information will only be available during
   *       metric processing after the source metadata is being retrieved.
   *       Thus, the expansion of `*` will be performed during metric
   *       processing and DF metric calculator will be initialized with
   *       actual list of source columns passed as `columns` argument.
   */
  def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator
}