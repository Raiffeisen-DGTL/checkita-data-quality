package org.checkita.core.checks.load

import org.apache.spark.sql.types.{StructField, StructType}
import org.checkita.appsettings.AppSettings
import org.checkita.core.Results.LoadCheckCalculatorResult
import org.checkita.core.checks.{LoadCheckCalculator, LoadCheckName}
import org.checkita.core.{CalculatorStatus, Source}
import org.checkita.readers.SchemaReaders.SourceSchema

import scala.annotation.tailrec

/**
 * `Columns exists` load check calculator:
 * verifies if source contains required columns
 *
 * @param checkId Load check ID
 * @param requiredColumns Sequence of required columns
 */
final case class ColumnsExistsLoadCheckCalculator(
                                                   checkId: String,
                                                   requiredColumns: Seq[String]
                                                 ) extends LoadCheckCalculator {

  val checkName: LoadCheckName = LoadCheckName.ColumnsExist
  val expected: String = requiredColumns.mkString("[", ",", "]")
  val detailsMsg: String = s"source contains following columns: $expected."

  /**
   * Recursively retrieves all column names from dataframe schema which might have nested StructTypes.
   * @param fields Sequence of fields to retrieve names from
   * @param acc Accumulated column names
   * @return Sequence of column names from provided schema of the dataframe.
   */
  @tailrec
  private def getAllColNames(fields: Seq[StructField], acc: Seq[String] = Seq.empty): Seq[String] =
    fields match {
      case Nil => acc
      case f :: t if f.dataType.isInstanceOf[StructType] =>
        getAllColNames(f.dataType.asInstanceOf[StructType].toList ++ t, acc :+ f.name)
      case f :: t => getAllColNames(t, acc :+ f.name)
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
              (implicit settings: AppSettings): LoadCheckCalculatorResult = {
    
    val allCols = getAllColNames(source.df.schema.toList).toSet
    
    val missedCols =
      if (settings.enableCaseSensitivity) requiredColumns.filterNot(col => allCols.contains(col))
      else requiredColumns.map(_.toLowerCase).filterNot(col => allCols.map(_.toLowerCase).contains(col))
    val status = if (missedCols.isEmpty) CalculatorStatus.Success else CalculatorStatus.Failure
    val statusMsg = 
      if (status == CalculatorStatus.Success) "Source contains all required columns"
      else s"Following columns are not found: ${missedCols.mkString("[", ",", "]")}"

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
