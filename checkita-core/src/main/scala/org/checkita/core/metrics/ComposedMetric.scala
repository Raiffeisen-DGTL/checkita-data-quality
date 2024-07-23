package org.checkita.core.metrics

/**
 * Base class for composed metric. All composed metric must have defined following:
 *   - metric ID
 *   - metric name (always COMPOSED)
 *   - formula to calculate composed metric value 
 */
trait ComposedMetric {
  val metricId: String
  val metricName: MetricName = MetricName.Composed
  val metricFormula: String
}
