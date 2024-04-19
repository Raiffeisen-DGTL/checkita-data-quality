package ru.raiffeisen.checkita.core.metrics

import ru.raiffeisen.checkita.core.metrics.df.{DFMetricCalculator, ReversibleDFCalculator}
import ru.raiffeisen.checkita.core.metrics.rdd.{RDDMetricCalculator, ReversibleRDDCalculator}

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
  type ReversibleRDDMetricCalculator = RDDMetricCalculator with ReversibleRDDCalculator
  type ReversibleDFMetricCalculator = DFMetricCalculator with ReversibleDFCalculator
  val reversed: Boolean
  override def initRDDMetricCalculator: ReversibleRDDMetricCalculator
  override def initDFMetricCalculator: ReversibleDFMetricCalculator
}
