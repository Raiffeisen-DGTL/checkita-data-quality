package org.checkita.dqf.core.metrics.rdd

import org.checkita.dqf.core.CalculatorStatus

import scala.util.{Failure, Success, Try}

/**
 * Trait to be mixed in to metric calculator to support reversal of error collection logic.
 *
 *   - Reversible metric calculators can collect metric errors either in direct or in reversed mode depending
 *     on provided boolean flag.
 */
trait ReversibleRDDCalculator { this: RDDMetricCalculator =>
  protected val reversed: Boolean

  /**
   * Increment metric calculator with REVERSED error collection logic. May throw an exception.
   *
   * @param values values to process
   * @return updated calculator or throws an exception
   */
  protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator

  /**
   * Safely updates metric calculator with respect to
   * specified error collection logic (direct or reversed).
   *
   * @param values values to process
   * @return updated calculator
   */
  override def increment(values: Seq[Any]): RDDMetricCalculator = {
    val incrementFunc: Seq[Any] => RDDMetricCalculator =
      v => if (reversed) tryToIncrementReversed(v) else tryToIncrement(v)

    Try(incrementFunc(values)) match {
      case Success(calc) => calc
      case Failure(e) => copyWithError(CalculatorStatus.Error, e.getMessage)
    }
  }
}
