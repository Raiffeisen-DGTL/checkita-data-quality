package ru.raiffeisen.checkita.core

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

/**
 * Calculator Status enumeration. Following statuses are possible:
 *   - Success: calculator run without errors and returned success.
 *   - Failure: calculator run without errors and returned failure.
 *   - Error: calculator thrown some errors.
 */
sealed trait CalculatorStatus extends EnumEntry
object CalculatorStatus extends Enum[CalculatorStatus] {
  case object Success extends CalculatorStatus
  case object Failure extends CalculatorStatus
  case object Error extends CalculatorStatus
  override def values: immutable.IndexedSeq[CalculatorStatus] = findValues
}