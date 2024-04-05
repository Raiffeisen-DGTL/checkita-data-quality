package ru.raiffeisen.checkita.core.metrics.df

/**
 * Trait to be mixed in to metric calculator to support reversal of error collection logic.
 * Reversible metric calculators can collect metric errors either in direct or
 * in reversed mode depending on provided boolean flag.
 *
 * @note unlike RDD metric calculators, DF metric calculators essentially require
 *       only presence of `reversed` boolean flag. The rest of reversible logic will
 *       be implemented within spark expression.
 */
trait ReversibleDFCalculator { this: DFMetricCalculator =>
  protected val reversed: Boolean
}
