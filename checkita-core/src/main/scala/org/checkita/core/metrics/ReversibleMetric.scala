package org.checkita.core.metrics

import org.checkita.core.metrics.df.{DFMetricCalculator, ReversibleDFCalculator}
import org.checkita.core.metrics.rdd.{RDDMetricCalculator, ReversibleRDDCalculator}

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
  override def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator
}
