package ru.raiffeisen.checkita.config.jobconf

import eu.timepit.refined.types.string.NonEmptyString
import ru.raiffeisen.checkita.config.RefinedTypes.{ID, NonEmptyStringSeq, PositiveInt}
import ru.raiffeisen.checkita.core.checks.LoadCheckCalculator
import ru.raiffeisen.checkita.core.checks.load._

object LoadChecks {

  /**
   * Base class for all load checks configurations.
   * All load checks must have an ID as well as reference to source ID which is checked.
   * Additionally, it is required to define method for appropriate load check calculator initiation.
   */
  sealed abstract class LoadCheckConfig {
    val id: ID
    val source: NonEmptyString
    def getCalculator: LoadCheckCalculator
  }

  /**
   * Exact column number load check configuration
   * @param id Load check ID
   * @param source Source ID to be checked
   * @param option Required number of columns
   */
  final case class ExactColNumLoadCheckConfig(
                                         id: ID,
                                         source: NonEmptyString,
                                         option: PositiveInt
                                       ) extends LoadCheckConfig {
    def getCalculator: LoadCheckCalculator = ExactColNumLoadCheckCalculator(id.value, option.value)
  }

  /**
   * Minimum column number load check configuration
   * @param id Load check ID
   * @param source Source ID to be checked
   * @param option Minimum number of columns
   */
  final case class MinColNumLoadCheckConfig(
                                       id: ID,
                                       source: NonEmptyString,
                                       option: PositiveInt
                                     ) extends LoadCheckConfig {
    def getCalculator: LoadCheckCalculator = MinColNumLoadCheckCalculator(id.value, option.value)
  }

  /**
   * Columns exists load check configuration
   * @param id Load check ID
   * @param source Source ID to be checked
   * @param columns Sequence of columns that must be presented in the source
   */
  final case class ColumnsExistsLoadCheckConfig(
                                           id: ID,
                                           source: NonEmptyString,
                                           columns: NonEmptyStringSeq
                                         ) extends LoadCheckConfig {
    def getCalculator: LoadCheckCalculator = ColumnsExistsLoadCheckCalculator(id.value, columns.value)
  }

  /**
   * Schema match load check configuration
   * @param id Load check ID
   * @param source Source ID to be checked
   * @param schema Schema ID to match with
   * @param ignoreOrder If true than order of columns in schemas is ignored.
   */
  final case class SchemaMatchLoadCheckConfig(
                                         id: ID,
                                         source: NonEmptyString,
                                         schema: NonEmptyString,
                                         ignoreOrder: Boolean = false
                                       ) extends LoadCheckConfig {
    def getCalculator: LoadCheckCalculator = SchemaMatchLoadCheckCalculator(id.value, schema.value, ignoreOrder)
  }
                                      
  
  /**
   * Data Quality job configuration section describing load checks
   * @param exactColumnNum Sequence of load checks for exact column number
   * @param minColumnNum Sequence of load checks for minimum column number
   * @param columnsExist Sequence of load checks to verify columns existence
   * @param schemaMatch Sequence of load checks to verify if source schema matches target one
   */
  final case class LoadChecksConfig(
                                     exactColumnNum: Seq[ExactColNumLoadCheckConfig] = Seq.empty,
                                     minColumnNum: Seq[MinColNumLoadCheckConfig] = Seq.empty,
                                     columnsExist: Seq[ColumnsExistsLoadCheckConfig] = Seq.empty,
                                     schemaMatch: Seq[SchemaMatchLoadCheckConfig] = Seq.empty
                                   ) {
    def getAllLoadChecks: Seq[LoadCheckConfig] = 
      this.productIterator.toSeq.flatMap(_.asInstanceOf[Seq[Any]]).map(_.asInstanceOf[LoadCheckConfig])
  }
  
  
}
