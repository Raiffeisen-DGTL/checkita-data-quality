package org.checkita.dqf.core.checks

import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.core.Results.LoadCheckCalculatorResult
import org.checkita.dqf.core.{CalculatorStatus, Source}
import org.checkita.dqf.readers.SchemaReaders.SourceSchema

import scala.util.{Failure, Success, Try}

/**
 * Base LoadCheck calculator
 */
abstract class LoadCheckCalculator {
  
  val checkId: String
  val checkName: LoadCheckName
  val expected: String
  val detailsMsg: String // details message to insert into final check message
  val isCritical: Boolean

  /**
   * Generates comprehensive check message
   * @param sourceId Source ID being checked
   * @param status Load check evaluation status
   * @param statusString Load check evaluation status string
   * @return Load check message
   */
  def getMessage(sourceId: String, status: CalculatorStatus, statusString: String): String = {
    val common = s"Load check '$checkId' of type '${checkName.entryName}' on source '$sourceId': check if "
    common + detailsMsg +  s" Result: ${status.toString}. " + statusString
  }

  /**
   * Runs load check for the given source.
   * @param source Source to check
   * @param schemas Map of predefined source schemas
   * @param settings Implicit application settings
   * @return Load check evaluation result with either Success or Failure status
   */
  def tryToRun(source: Source,
               schemas: Map[String, SourceSchema] = Map.empty)
              (implicit settings: AppSettings): LoadCheckCalculatorResult

  /**
   * Safely runs load check for requested source:
   *   - Tries to evaluate check and get either Success or Failure status 
   *     (depending on whether load check criteria is met)
   *   - If check evaluation throws runtime error: returns load check result
   *     with Error status and corresponding error message.
   * @param source Source to check
   * @param schemas Map of predefined source schemas
   * @param settings Implicit application settings
   * @return Load check result
   */
  def run(source: Source,
          schemas: Map[String, SourceSchema] = Map.empty)
         (implicit settings: AppSettings): LoadCheckCalculatorResult =
    Try(tryToRun(source, schemas)) match {
      case Success(result) => result
      case Failure(err) => LoadCheckCalculatorResult(
        checkId,
        checkName.entryName,
        source.id,
        expected,
        CalculatorStatus.Error,
        s"Unable to perform load check due to following error: ${err.getMessage}",
        isCritical
      )
    }
}