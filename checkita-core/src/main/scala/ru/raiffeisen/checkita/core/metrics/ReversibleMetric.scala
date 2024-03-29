package ru.raiffeisen.checkita.core.metrics

/**
 * Trait to be mixed in to reversible metric definitions (configurations).
 *
 *   - Reversible metrics must defined boolean flag indicating whether error collection
 *     logic for metric is direct or reversed.
 *
 *   - Initialization of metric calculator must return instance of reversible metric calculator
 *     (one that supports error collection logic reversal).
 */
trait ReversibleMetric { this: RegularMetric =>
  type ReversibleMetricCalculator = MetricCalculator with ReversibleCalculator
  val reversed: Boolean
  override def initMetricCalculator: ReversibleMetricCalculator
}
