package org.checkita.dqf.config.jobconf

import eu.timepit.refined.types.string.NonEmptyString
import org.checkita.dqf.config.RefinedTypes.{ID, NonEmptyStringSeq, PositiveInt, SparkParam}
import org.checkita.dqf.core.checks.LoadCheckCalculator
import org.checkita.dqf.core.checks.load._

object LoadChecks {

  /**
   * Base class for all load checks configurations.
   * All load checks are described as DQ entities that must have a reference to source ID which is checked.
   * Additionally, it is required to define method for appropriate load check calculator initiation.
   */
  sealed abstract class LoadCheckConfig extends JobConfigEntity {
    val source: NonEmptyString
    def getCalculator: LoadCheckCalculator
  }

  /**
   * Exact column number load check configuration
   *
   * @param id          Load check ID
   * @param description Load check description
   * @param source      Source ID to be checked
   * @param option      Required number of columns
   * @param metadata    List of metadata parameters specific to this load check
   */
  final case class ExactColNumLoadCheckConfig(
                                               id: ID,
                                               description: Option[NonEmptyString],
                                               source: NonEmptyString,
                                               option: PositiveInt,
                                               metadata: Seq[SparkParam] = Seq.empty
                                             ) extends LoadCheckConfig {
    def getCalculator: LoadCheckCalculator = ExactColNumLoadCheckCalculator(id.value, option.value)
  }

  /**
   * Minimum column number load check configuration
   *
   * @param id          Load check ID
   * @param description Load check description
   * @param source      Source ID to be checked
   * @param option      Minimum number of columns
   * @param metadata    List of metadata parameters specific to this load check
   */
  final case class MinColNumLoadCheckConfig(
                                             id: ID,
                                             description: Option[NonEmptyString],
                                             source: NonEmptyString,
                                             option: PositiveInt,
                                             metadata: Seq[SparkParam] = Seq.empty
                                           ) extends LoadCheckConfig {
    def getCalculator: LoadCheckCalculator = MinColNumLoadCheckCalculator(id.value, option.value)
  }

  /**
   * Columns exists load check configuration
   *
   * @param id          Load check ID
   * @param description Load check description
   * @param source      Source ID to be checked
   * @param columns     Sequence of columns that must be presented in the source
   * @param metadata    List of metadata parameters specific to this load check
   */
  final case class ColumnsExistsLoadCheckConfig(
                                                 id: ID,
                                                 description: Option[NonEmptyString],
                                                 source: NonEmptyString,
                                                 columns: NonEmptyStringSeq,
                                                 metadata: Seq[SparkParam] = Seq.empty
                                               ) extends LoadCheckConfig {
    def getCalculator: LoadCheckCalculator = ColumnsExistsLoadCheckCalculator(id.value, columns.value)
  }

  /**
   * Schema match load check configuration
   *
   * @param id          Load check ID
   * @param description Load check description
   * @param source      Source ID to be checked
   * @param schema      Schema ID to match with
   * @param ignoreOrder If true than order of columns in schemas is ignored
   * @param metadata    List of metadata parameters specific to this load check
   */
  final case class SchemaMatchLoadCheckConfig(
                                               id: ID,
                                               description: Option[NonEmptyString],
                                               source: NonEmptyString,
                                               schema: NonEmptyString,
                                               ignoreOrder: Boolean = false,
                                               metadata: Seq[SparkParam] = Seq.empty
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
