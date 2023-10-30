package ru.raiffeisen.checkita.core.metrics

import ru.raiffeisen.checkita.core.CalculatorStatus

import scala.util.{Failure, Success, Try}

/**
 * Basic metric calculator
 */
abstract class MetricCalculator {

  protected val status: CalculatorStatus
  protected val failCount: Long
  protected val failMsg: String
  
  /**
   * Merges two metric calculators together
   *
   * @param m2 second metric calculator
   * @return merged metric calculator
   */
  def merge(m2: MetricCalculator): MetricCalculator

  /**
   * Increment metric calculator. May throw an exception.
   * @param values values to process
   * @return updated calculator or throws an exception
   */
  protected def tryToIncrement(values: Seq[Any]): MetricCalculator

  /**
   * Copy calculator with error status and corresponding message.
   * @param status Calculator status to copy with
   * @param msg Failure message 
   * @param failInc Failure increment
   * @return Copy of this calculator with error status
   */
  protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator
  
  /**
   * Safely updates metric calculator
   * @param values values to process
   * @return updated calculator
   */
  def increment(values: Seq[Any]): MetricCalculator = Try(tryToIncrement(values)) match {
    case Success(calc) => calc
    case Failure(e) => copyWithError(CalculatorStatus.Error, e.getMessage)
  }
  
  /**
   * Gets results of calculator in the current state
   * @return Map of (result_name -> (result, additionalResults))
   */
  def result(): Map[String, (Double, Option[String])]

  /**
   * Gets current metric calculator status
   * @return Calculator status
   */
  def getStatus: CalculatorStatus = status

  /**
   * Gets current metric failure counts
   * @return Failure count
   */
  def getFailCounter: Long = failCount

  /**
   * Gets current failure or error message
   * @return Failure message
   */
  def getFailMessage: String = failMsg
}
