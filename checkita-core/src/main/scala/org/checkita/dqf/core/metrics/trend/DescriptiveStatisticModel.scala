package org.checkita.dqf.core.metrics.trend

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType

import java.sql.Timestamp

/**
 * Base class to compute statistics over historical metric results.
 * @param f Function that is used to compute statistic value
 */
class DescriptiveStatisticModel(f: DescriptiveStatistics => Double) extends TrendMetricModel {

  /**
   * Function which aggregates historical metric results and computes a desired statistic.
   *
   * @param data Historical metric results
   * @return Statistic computed over historical metric results.
   *
   * @note Linear method is used to compute quantiles (method #7 in [1]).
   *
   * [1] R. J. Hyndman and Y. Fan, "Sample quantiles in statistical packages,
   *     "The American Statistician, 50(4), pp. 361-365, 1996
   */
  override def infer(data: Seq[(Timestamp, Double)], ts: Timestamp): (Double, Option[String]) = {
    val stats = new DescriptiveStatistics
    stats.setPercentileImpl(new Percentile().withEstimationType(EstimationType.R_7))
    data.foreach(d => stats.addValue(d._2))
    f(stats) -> None
  }
}

/**
 * Supported statistics
 */
object DescriptiveStatisticModel {
  object Avg extends DescriptiveStatisticModel(_.getMean)
  object Std extends DescriptiveStatisticModel(_.getStandardDeviation)
  object Min extends DescriptiveStatisticModel(_.getMin)
  object Max extends DescriptiveStatisticModel(_.getMax)
  object Sum extends DescriptiveStatisticModel(_.getSum)
  object Median extends DescriptiveStatisticModel(_.getPercentile(50))
  object FirstQuantile extends DescriptiveStatisticModel(_.getPercentile(25))
  object ThirdQuantile extends DescriptiveStatisticModel(_.getPercentile(75))
  class Quantile(quantile: Double) extends DescriptiveStatisticModel(_.getPercentile(quantile))
}
