package org.checkita.dqf.core.metrics.trend

import java.sql.Timestamp

/**
 * Trait that should be implemented by
 * all trend metrics as it provides 
 * the fundamental method for inferring
 * trend metric value.
 */
trait TrendMetricModel {
  /**
   * Infer trend metric value for current reference date
   * based on historical metric results.
   * @param data Historical metric results.
   * @param ts UTC timestamp of current reference date.
   * @return Trend metric value and optional additional result 
   *         (may contain some information about model parameters)
   */
  def infer(data: Seq[(Timestamp, Double)], ts: Timestamp): (Double, Option[String])
}

