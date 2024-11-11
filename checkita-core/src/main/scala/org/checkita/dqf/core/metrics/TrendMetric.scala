package org.checkita.dqf.core.metrics

import org.checkita.dqf.config.Enums.TrendCheckRule
import org.checkita.dqf.core.metrics.trend.TrendMetricModel

import java.sql.Timestamp

/**
 * Base class for trend metrics.
 * All trend metrics must have defined following:
 *   - metric ID
 *   - metric name
 *   - lookup metric ID: metric which results will be pulled from DQ storage
 *   - model used to predict metric value based on historical metric results
 *   - rule to build time window: either record or duration
 *   - size of the window to pull historical results
 *   - offset from current date or record (depending on rule)
 */
trait TrendMetric {
  val metricId: String
  val metricName: MetricName
  val lookupMetricId: String
  val model: TrendMetricModel
  val rule: TrendCheckRule
  val wSize: String
  val wOffset: Option[String]
}
