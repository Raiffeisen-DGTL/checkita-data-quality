package org.checkita.dqf.core.checks.load

import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.core.Results.LoadCheckCalculatorResult
import org.checkita.dqf.core.checks.{LoadCheckCalculator, LoadCheckName}
import org.checkita.dqf.core.{CalculatorStatus, Source}
import org.checkita.dqf.readers.SchemaReaders.SourceSchema

/**
 * `Exact column number` load check calculator:
 * verifies if number of columns in the source is equal to required one.
 * @param checkId Load check ID
 * @param requiredColNum Required number of columns.
 * @param isCritical Flag if check is critical
 * @note Counts top level columns only.
 */
final case class ExactColNumLoadCheckCalculator(
                                                 checkId: String,
                                                 requiredColNum: Int,
                                                 isCritical: Boolean
                                               ) extends LoadCheckCalculator {
  
  val checkName: LoadCheckName = LoadCheckName.ExactColNum
  val expected: String = requiredColNum.toString
  val detailsMsg: String = s"number of columns equals to $expected."

  /**
   * Runs load check for the given source.
   * @param source Source to check
   * @param schemas Map of predefined source schemas
   * @param settings Implicit application settings
   * @return Load check evaluation result with either Success or Failure status
   */
  def tryToRun(source: Source,
               schemas: Map[String, SourceSchema] = Map.empty)
              (implicit settings: AppSettings): LoadCheckCalculatorResult = {
    val numCols = source.df.columns.length
    val status = if (numCols == requiredColNum) CalculatorStatus.Success else CalculatorStatus.Failure
    val statusMsg = s"(source columns) $numCols " + 
      (if (status == CalculatorStatus.Success) "==" else "!=") + 
      s" $expected (expected)."
    
    LoadCheckCalculatorResult(
      checkId,
      checkName.entryName,
      source.id,
      expected,
      status,
      getMessage(source.id, status, statusMsg)
    )
  }
}
