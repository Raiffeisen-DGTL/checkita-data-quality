package ru.raiffeisen.checkita.config.jobconf

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.types.string.NonEmptyString
import org.apache.spark.sql.Column
import org.apache.spark.storage.StorageLevel
import ru.raiffeisen.checkita.config.Enums.{KafkaTopicFormat, SparkJoinType}
import ru.raiffeisen.checkita.config.RefinedTypes._
import ru.raiffeisen.checkita.config.jobconf.Files._
import ru.raiffeisen.checkita.config.jobconf.Outputs._

object Sources {

  /**
   * Base class for all source configurations.
   * All sources must have an ID and optional sequence of keyFields
   * which will uniquely identify data row in error collection reports.
   */
  sealed abstract class SourceConfig {
    val id: ID
    val keyFields: Seq[NonEmptyString]
  }

  /**
   * JDBC Table source configuration
   *
   * @param id         Source ID
   * @param connection Connection ID (must be JDBC connection)
   * @param table      Table to read
   * @param keyFields  Sequence of key fields (columns that identify data row)
   */
  final case class TableSourceConfig(
                                      id: ID,
                                      connection: NonEmptyString,
                                      table: Option[NonEmptyString],
                                      query: Option[NonEmptyString],
                                      keyFields: Seq[NonEmptyString] = Seq.empty
                                    ) extends SourceConfig

  /**
   * Configuration for Hive Table partition values to read.
   *
   * @param name   Name of partition column
   * @param values Sequence of partition values to read
   */
  final case class HivePartition(
                                  name: NonEmptyString,
                                  values: Seq[NonEmptyString]
                                )

  /**
   * Hive table source configuration
   *
   * @param id         Source ID
   * @param schema     Hive schema
   * @param table      Hive table
   * @param partitions Sequence of partitions to read.
   *                   The order of partition columns should correspond to order in which
   *                   partition columns are defined in hive table DDL.
   * @param keyFields  Sequence of key fields (columns that identify data row)
   */
  final case class HiveSourceConfig(
                                     id: ID,
                                     schema: NonEmptyString,
                                     table: NonEmptyString,
                                     partitions: Seq[HivePartition] = Seq.empty,
                                     keyFields: Seq[NonEmptyString] = Seq.empty
                                   ) extends SourceConfig

  /**
   * Kafka source configuration
   *
   * @param id              Source ID
   * @param connection      Connection ID (must be a Kafka Connection)
   * @param topics          Sequence of topics to read
   * @param topicPattern    Pattern that defined topics to read
   * @param format          Topic message format
   * @param startingOffsets Json-string defining starting offsets (default: "earliest")
   * @param endingOffsets   Json-string defining ending offsets (default: "latest")
   * @param options         Sequence of additional Kafka options
   * @param keyFields       Sequence of key fields (columns that identify data row)
   */
  final case class KafkaSourceConfig(
                                      id: ID,
                                      connection: NonEmptyString,
                                      topics: Seq[NonEmptyString] = Seq.empty,
                                      topicPattern: Option[NonEmptyString],
                                      format: KafkaTopicFormat,
                                      startingOffsets: NonEmptyString = "earliest",
                                      endingOffsets: NonEmptyString = "latest",
                                      options: Seq[SparkParam] = Seq.empty,
                                      keyFields: Seq[NonEmptyString] = Seq.empty
                                    ) extends SourceConfig

  /**
   * Base class for file source configurations.
   */
  sealed abstract class FileSourceConfig extends SourceConfig

  /**
   * Fixed-width file source configuration
   *
   * @param id        Source ID
   * @param path      Path to file
   * @param schema    Schema ID (must be either fixedFull or fixedShort schema)
   * @param keyFields Sequence of key fields (columns that identify data row)
   */
  final case class FixedFileSourceConfig(
                                          id: ID,
                                          path: URI,
                                          schema: NonEmptyString,
                                          keyFields: Seq[NonEmptyString] = Seq.empty
                                        ) extends FileSourceConfig with FixedFileConfig

  /**
   * Delimited file source configuration
   *
   * @param id        Source ID
   * @param path      Path to file
   * @param delimiter Column delimiter (default: ,)
   * @param quote     Quotation symbol (default: ")
   * @param escape    Escape symbol (default: \)
   * @param header    Boolean flag indicating whether schema should be read from file header (default: false)
   * @param schema    Schema ID (only if header = false)
   * @param keyFields Sequence of key fields (columns that identify data row)
   */
  final case class DelimitedFileSourceConfig(
                                              id: ID,
                                              path: URI,
                                              schema: Option[NonEmptyString],
                                              delimiter: NonEmptyString = ",",
                                              quote: NonEmptyString = "\"",
                                              escape: NonEmptyString = "\\",
                                              header: Boolean = false,
                                              keyFields: Seq[NonEmptyString] = Seq.empty
                                            ) extends FileSourceConfig with DelimitedFileConfig

  /**
   * Avro file source configuration
   *
   * @param id        Source ID
   * @param path      Path to file
   * @param schema    Schema ID (must be and Avro Schema)
   * @param keyFields Sequence of key fields (columns that identify data row)
   */
  final case class AvroFileSourceConfig(
                                         id: ID,
                                         path: URI,
                                         schema: Option[NonEmptyString],
                                         keyFields: Seq[NonEmptyString] = Seq.empty
                                       ) extends FileSourceConfig with AvroFileConfig

  /**
   * Orc file source configuration
   *
   * @param id        Source ID
   * @param path      Path to file
   * @param keyFields Sequence of key fields (columns that identify data row)
   */
  final case class OrcFileSourceConfig(
                                        id: ID,
                                        path: URI,
                                        keyFields: Seq[NonEmptyString] = Seq.empty
                                      ) extends FileSourceConfig with OrcFileConfig

  /**
   * Parquet file source configuration
   *
   * @param id        Source ID
   * @param path      Path to file
   * @param keyFields Sequence of key fields (columns that identify data row)
   */
  final case class ParquetFileSourceConfig(
                                            id: ID,
                                            path: URI,
                                            keyFields: Seq[NonEmptyString] = Seq.empty
                                          ) extends FileSourceConfig with ParquetFileConfig


  /**
   * Base class for all virtual source configurations.
   * In addition to basic source configuration,
   * virtual sources might have following optional parameters:
   *   - spark persist storage level in order to persist virtual source during job execution
   *   - save configuration in order to save virtual source as a file.
   */
  sealed abstract class VirtualSourceConfig extends SourceConfig {
    val persist: Option[StorageLevel]
    val save: Option[FileOutputConfig]
    val parents: Seq[String]
    // additional validation will be imposed on the required number
    // of parent sources depending on virtual source type.
  }

  /**
   * Sql virtual source configuration
   *
   * @param id            Virtual source ID
   * @param parentSources Non-empty sequence of parent sources
   * @param query         SQL query to build virtual source from parent sources
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   */
  final case class SqlVirtualSourceConfig(
                                           id: ID,
                                           parentSources: NonEmptyStringSeq,
                                           query: NonEmptyString,
                                           persist: Option[StorageLevel],
                                           save: Option[FileOutputConfig],
                                           keyFields: Seq[NonEmptyString] = Seq.empty
                                         ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
  }

  /**
   * Join virtual source configuration
   *
   * @param id            Virtual source ID
   * @param parentSources Sequence of exactly two parent sources.
   * @param joinBy        Non-empty sequence of columns to join by.
   * @param joinType      Spark join type.
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   */
  final case class JoinVirtualSourceConfig(
                                            id: ID,
                                            parentSources: DoubleElemStringSeq,
                                            joinBy: NonEmptyStringSeq,
                                            joinType: SparkJoinType,
                                            persist: Option[StorageLevel],
                                            save: Option[FileOutputConfig],
                                            keyFields: Seq[NonEmptyString] = Seq.empty
                                          ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
  }

  /**
   * Filter virtual source configuration
   *
   * @param id            Virtual source ID
   * @param parentSources Sequence containing exactly one source.
   * @param expr          Non-empty sequence of spark sql expression used to filter source.
   *                      All expressions must return boolean. Source is filtered using logical
   *                      conjunction of all provided expressions.
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   */
  final case class FilterVirtualSourceConfig(
                                              id: ID,
                                              parentSources: SingleElemStringSeq,
                                              expr: Seq[Column] Refined NonEmpty,
                                              persist: Option[StorageLevel],
                                              save: Option[FileOutputConfig],
                                              keyFields: Seq[NonEmptyString] = Seq.empty
                                            ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
  }

  /**
   * Select virtual source configuration
   *
   * @param id            Virtual source ID
   * @param parentSources Sequence containing exactly one source.
   * @param expr          Non-empty sequence of spark sql expression used select column from parent source.
   *                      One expression per each resultant column
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   */
  final case class SelectVirtualSourceConfig(
                                              id: ID,
                                              parentSources: SingleElemStringSeq,
                                              expr: Seq[Column] Refined NonEmpty,
                                              persist: Option[StorageLevel],
                                              save: Option[FileOutputConfig],
                                              keyFields: Seq[NonEmptyString] = Seq.empty
                                            ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
  }

  /**
   * Aggregate virtual source configuration
   *
   * @param id            Virtual source ID
   * @param parentSources Sequence containing exactly one source.
   * @param groupBy       Non-empty sequence of columns by which to perform grouping
   * @param expr          Non-empty sequence of spark sql expression used to get aggregated columns.
   *                      One expression per each resultant column
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   */
  final case class AggregateVirtualSourceConfig(
                                                 id: ID,
                                                 parentSources: SingleElemStringSeq,
                                                 groupBy: NonEmptyStringSeq,
                                                 expr: Seq[Column] Refined NonEmpty,
                                                 persist: Option[StorageLevel],
                                                 save: Option[FileOutputConfig],
                                                 keyFields: Seq[NonEmptyString] = Seq.empty
                                               ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
  }

  /**
   * Data Quality job configuration section describing sources
   *
   * @param table Sequence of table sources (read from JDBC connections)
   * @param hive  Sequence of Hive table sources
   * @param kafka Sequence of sources based on Kafka topics
   * @param file  Sequence of file sources
   */
  final case class SourcesConfig(
                                  table: Seq[TableSourceConfig] = Seq.empty,
                                  hive: Seq[HiveSourceConfig] = Seq.empty,
                                  kafka: Seq[KafkaSourceConfig] = Seq.empty,
                                  file: Seq[FileSourceConfig] = Seq.empty
                                ) {
    def getAllSources: Seq[SourceConfig] =
      this.productIterator.toSeq.flatMap(_.asInstanceOf[Seq[Any]]).map(_.asInstanceOf[SourceConfig])
  }
  
}
