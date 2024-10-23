package org.checkita.dqf.config.jobconf

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.types.string.NonEmptyString
import org.apache.spark.sql.Column
import org.apache.spark.storage.StorageLevel
import org.checkita.dqf.config.Enums.{KafkaTopicFormat, ProcessingTime, SparkJoinType, StreamWindowing}
import org.checkita.dqf.config.RefinedTypes._
import org.checkita.dqf.config.jobconf.Files._
import org.checkita.dqf.config.jobconf.Outputs._

object Sources {

  /**
   * Base class for all source configurations.
   * All sources are described as DQ entities that might have an optional sequence of keyFields
   * which will uniquely identify data row in error collection reports.
   * In addition, it should be indicated whether this source is streamable or not.
   */
  sealed abstract class SourceConfig extends JobConfigEntity {
    val keyFields: Seq[NonEmptyString]
    val streamable: Boolean
    val persist: Option[StorageLevel]
  }

  /**
   * JDBC Table source configuration
   *
   * @param id          Source ID
   * @param description Source description
   * @param connection  Connection ID (must be JDBC connection)
   * @param table       Table to read
   * @param query       Query to execute
   * @param persist     Spark storage level in order to persist dataframe during job execution.
   * @param keyFields   Sequence of key fields (columns that identify data row)
   * @param metadata    List of metadata parameters specific to this source
   * @note Either table to read or query to execute must be defined but not both.
   */
  final case class TableSourceConfig(
                                      id: ID,
                                      description: Option[NonEmptyString],
                                      connection: ID,
                                      table: Option[NonEmptyString],
                                      query: Option[NonEmptyString],
                                      persist: Option[StorageLevel],
                                      keyFields: Seq[NonEmptyString] = Seq.empty,
                                      metadata: Seq[SparkParam] = Seq.empty
                                    ) extends SourceConfig {
    val streamable: Boolean = false
  }

  /**
   * Configuration for Hive Table partition values to read.
   *
   * @param name   Name of partition column
   * @param expr   SQL Expression used to filter partitions to read
   * @param values Sequence of partition values to read
   */
  final case class HivePartition(
                                  name: NonEmptyString,
                                  expr: Option[Column],
                                  values: Seq[NonEmptyString] = Seq.empty
                                )

  /**
   * Hive table source configuration
   *
   * @param id          Source ID
   * @param description Source description
   * @param schema      Hive schema
   * @param table       Hive table
   * @param persist     Spark storage level in order to persist dataframe during job execution.
   * @param partitions  Sequence of partitions to read.
   *                    The order of partition columns should correspond to order in which
   *                    partition columns are defined in hive table DDL.
   * @param keyFields   Sequence of key fields (columns that identify data row)
   * @param metadata    List of metadata parameters specific to this source
   */
  final case class HiveSourceConfig(
                                     id: ID,
                                     description: Option[NonEmptyString],
                                     schema: NonEmptyString,
                                     table: NonEmptyString,
                                     persist: Option[StorageLevel],
                                     partitions: Seq[HivePartition] = Seq.empty,
                                     keyFields: Seq[NonEmptyString] = Seq.empty,
                                     metadata: Seq[SparkParam] = Seq.empty
                                   ) extends SourceConfig {
    val streamable: Boolean = false
  }

  /**
   * Kafka source configuration
   *
   * @param id               Source ID
   * @param description      Source description
   * @param connection       Connection ID (must be a Kafka Connection)
   * @param topics           Sequence of topics to read
   * @param topicPattern     Pattern that defined topics to read
   * @param startingOffsets  Json-string defining starting offsets.
   *                         If none is set, then "earliest" is used in batch jobs and "latest is used in streaming jobs.
   * @param endingOffsets    Json-string defining ending offset. Applicable only to batch jobs.
   *                         If none is set then "latest" is used.
   * @param persist          Spark storage level in order to persist dataframe during job execution.
   * @param windowBy         Source of timestamp used to build windows. Applicable only for streaming jobs!
   *                         Default: processingTime - uses current timestamp at the moment when Spark processes row.
   *                         Other options are:
   *                          - eventTime - uses Kafka message creation timestamp.
   *                          - customTime(columnName) - uses arbitrary user-defined column from kafka message
   *                            (column must be of TimestampType)
   * @param keyFormat        Message key format. Default: string.
   * @param valueFormat      Message value format. Default: string.
   * @param keySchema        Schema ID. Used to parse message key. Ignored when keyFormat is string.
   *                         Mandatory for other formats.
   * @param valueSchema      Schema ID. Used to parse message value. Ignored when valueFormat is string.
   *                         Mandatory for other formats.
   *                         Used to parse kafka message value.
   * @param subtractSchemaId Boolean flag indicating whether a kafka message schema ID encoded into its value, i.e.
   *                         [1 Magic Byte] + [4 Schema ID Bytes] + [Message Value Binary Data].
   *                         If set to `true`, then first five bytes are subtracted before value parsing.
   *                         Default: `false`
   * @param options          Sequence of additional Kafka options
   * @param keyFields        Sequence of key fields (columns that identify data row)
   * @param metadata         List of metadata parameters specific to this source
   */
  final case class KafkaSourceConfig(
                                      id: ID,
                                      description: Option[NonEmptyString],
                                      connection: ID,
                                      topics: Seq[NonEmptyString] = Seq.empty,
                                      topicPattern: Option[NonEmptyString],
                                      startingOffsets: Option[NonEmptyString], // earliest for batch, latest for stream
                                      endingOffsets: Option[NonEmptyString], // latest for batch, ignored for stream.
                                      persist: Option[StorageLevel],
                                      windowBy: StreamWindowing = ProcessingTime,
                                      keyFormat: KafkaTopicFormat = KafkaTopicFormat.String,
                                      valueFormat: KafkaTopicFormat = KafkaTopicFormat.String,
                                      keySchema: Option[ID] = None,
                                      valueSchema: Option[ID] = None,
                                      subtractSchemaId: Boolean = false,
                                      options: Seq[SparkParam] = Seq.empty,
                                      keyFields: Seq[NonEmptyString] = Seq.empty,
                                      metadata: Seq[SparkParam] = Seq.empty
                                    ) extends SourceConfig {
    val streamable: Boolean = true
  }

  /**
   * Greenplum Table source configuration
   *
   * @param id          Source ID
   * @param description Source description
   * @param connection  Connection ID (must be pivotal connection)
   * @param table       Table to read
   * @param persist     Spark storage level in order to persist dataframe during job execution.
   * @param keyFields   Sequence of key fields (columns that identify data row)
   * @param metadata    List of metadata parameters specific to this source
   */
  final case class GreenplumSourceConfig(
                                          id: ID,
                                          description: Option[NonEmptyString],
                                          connection: ID,
                                          table: Option[NonEmptyString],
                                          persist: Option[StorageLevel],
                                          keyFields: Seq[NonEmptyString] = Seq.empty,
                                          metadata: Seq[SparkParam] = Seq.empty
                                        ) extends SourceConfig {
    val streamable: Boolean = false
  }

  /**
   * Base class for file source configurations.
   * All file sources are streamable and therefore must contain windowBy parameter which
   * defined source of timestamp used to build stream windows.
   */
  sealed abstract class FileSourceConfig extends SourceConfig {
    val windowBy: StreamWindowing
  }

  /**
   * Fixed-width file source configuration
   *
   * @param id          Source ID
   * @param description Source description
   * @param path        Path to file
   * @param schema      Schema ID (must be either fixedFull or fixedShort schema)
   * @param persist     Spark storage level in order to persist dataframe during job execution.
   * @param windowBy    Source of timestamp used to build windows. Applicable only for streaming jobs!
   *                    Default: processingTime - uses current timestamp at the moment when Spark processes row.
   *                    Other options are:
   *                    - eventTime - uses column with name 'timestamp' (column must be of TimestampType).
   *                    - customTime(columnName) - uses arbitrary user-defined column
   *                      (column must be of TimestampType)
   * @param keyFields   Sequence of key fields (columns that identify data row)
   * @param metadata    List of metadata parameters specific to this source
   */
  final case class FixedFileSourceConfig(
                                          id: ID,
                                          description: Option[NonEmptyString],
                                          path: URI,
                                          schema: Option[ID],
                                          persist: Option[StorageLevel],
                                          windowBy: StreamWindowing = ProcessingTime,
                                          keyFields: Seq[NonEmptyString] = Seq.empty,
                                          metadata: Seq[SparkParam] = Seq.empty
                                        ) extends FileSourceConfig with FixedFileConfig {
    val streamable: Boolean = true
  }

  /**
   * Delimited file source configuration
   *
   * @param id          Source ID
   * @param description Source description
   * @param path        Path to file
   * @param delimiter   Column delimiter (default: ,)
   * @param quote       Quotation symbol (default: ")
   * @param escape      Escape symbol (default: \)
   * @param header      Boolean flag indicating whether schema should be read from file header (default: false)
   * @param schema      Schema ID (only if header = false)
   * @param persist     Spark storage level in order to persist dataframe during job execution.
   * @param windowBy    Source of timestamp used to build windows. Applicable only for streaming jobs!
   *                    Default: processingTime - uses current timestamp at the moment when Spark processes row.
   *                    Other options are:
   *                    - eventTime - uses column with name 'timestamp' (column must be of TimestampType).
   *                    - customTime(columnName) - uses arbitrary user-defined column
   *                      (column must be of TimestampType)
   * @param keyFields   Sequence of key fields (columns that identify data row)
   * @param metadata    List of metadata parameters specific to this source
   */
  final case class DelimitedFileSourceConfig(
                                              id: ID,
                                              description: Option[NonEmptyString],
                                              path: URI,
                                              schema: Option[ID],
                                              persist: Option[StorageLevel],
                                              delimiter: NonEmptyString = ",",
                                              quote: NonEmptyString = "\"",
                                              escape: NonEmptyString = "\\",
                                              header: Boolean = false,
                                              windowBy: StreamWindowing = ProcessingTime,
                                              keyFields: Seq[NonEmptyString] = Seq.empty,
                                              metadata: Seq[SparkParam] = Seq.empty
                                            ) extends FileSourceConfig with DelimitedFileConfig {
    val streamable: Boolean = true
  }

  /**
   * Avro file source configuration
   *
   * @param id          Source ID
   * @param description Source description
   * @param path        Path to file
   * @param schema      Schema ID
   * @param persist     Spark storage level in order to persist dataframe during job execution.
   * @param windowBy    Source of timestamp used to build windows. Applicable only for streaming jobs!
   *                    Default: processingTime - uses current timestamp at the moment when Spark processes row.
   *                    Other options are:
   *                    - eventTime - uses column with name 'timestamp' (column must be of TimestampType).
   *                    - customTime(columnName) - uses arbitrary user-defined column
   *                      (column must be of TimestampType)
   * @param keyFields   Sequence of key fields (columns that identify data row)
   * @param metadata    List of metadata parameters specific to this source
   */
  final case class AvroFileSourceConfig(
                                         id: ID,
                                         description: Option[NonEmptyString],
                                         path: URI,
                                         schema: Option[ID],
                                         persist: Option[StorageLevel],
                                         windowBy: StreamWindowing = ProcessingTime,
                                         keyFields: Seq[NonEmptyString] = Seq.empty,
                                         metadata: Seq[SparkParam] = Seq.empty
                                       ) extends FileSourceConfig with AvroFileConfig {
    val streamable: Boolean = true
  }

  /**
   * Orc file source configuration
   *
   * @param id          Source ID
   * @param description Source description
   * @param path        Path to file
   * @param schema      Schema ID
   * @param persist     Spark storage level in order to persist dataframe during job execution.
   * @param windowBy    Source of timestamp used to build windows. Applicable only for streaming jobs!
   *                    Default: processingTime - uses current timestamp at the moment when Spark processes row.
   *                    Other options are:
   *                    - eventTime - uses column with name 'timestamp' (column must be of TimestampType).
   *                    - customTime(columnName) - uses arbitrary user-defined column
   *                      (column must be of TimestampType)
   * @param keyFields   Sequence of key fields (columns that identify data row)
   * @param metadata    List of metadata parameters specific to this source
   */
  final case class OrcFileSourceConfig(
                                        id: ID,
                                        description: Option[NonEmptyString],
                                        path: URI,
                                        schema: Option[ID],
                                        persist: Option[StorageLevel],
                                        windowBy: StreamWindowing = ProcessingTime,
                                        keyFields: Seq[NonEmptyString] = Seq.empty,
                                        metadata: Seq[SparkParam] = Seq.empty
                                      ) extends FileSourceConfig with OrcFileConfig {
    val streamable: Boolean = true
  }

  /**
   * Parquet file source configuration
   *
   * @param id          Source ID
   * @param description Source description
   * @param path        Path to file
   * @param schema      Schema ID
   * @param persist     Spark storage level in order to persist dataframe during job execution.
   * @param windowBy    Source of timestamp used to build windows. Applicable only for streaming jobs!
   *                    Default: processingTime - uses current timestamp at the moment when Spark processes row.
   *                    Other options are:
   *                    - eventTime - uses column with name 'timestamp' (column must be of TimestampType).
   *                    - customTime(columnName) - uses arbitrary user-defined column
   *                      (column must be of TimestampType)
   * @param keyFields   Sequence of key fields (columns that identify data row)
   * @param metadata    List of metadata parameters specific to this source
   */
  final case class ParquetFileSourceConfig(
                                            id: ID,
                                            description: Option[NonEmptyString],
                                            path: URI,
                                            schema: Option[ID],
                                            persist: Option[StorageLevel],
                                            windowBy: StreamWindowing = ProcessingTime,
                                            keyFields: Seq[NonEmptyString] = Seq.empty,
                                            metadata: Seq[SparkParam] = Seq.empty
                                          ) extends FileSourceConfig with ParquetFileConfig {
    val streamable: Boolean = true
  }

  /**
   * Custom source configuration:
   * used to read from source types that are not supported explicitly.
   *
   * @param id          Source ID
   * @param description Source description
   * @param format      Source format to set in spark reader.
   * @param path        Path to load the source from (if required)
   * @param schema      Explicit schema applied to source data (if required)
   * @param persist     Spark storage level in order to persist dataframe during job execution.
   * @param options     List of additional spark options required to read the source (if any)
   * @param keyFields   Sequence of key fields (columns that identify data row)
   * @param metadata    List of metadata parameters specific to this source
   */
  final case class CustomSource(
                                 id: ID,
                                 description: Option[NonEmptyString],
                                 format: NonEmptyString,
                                 path: Option[URI],
                                 schema: Option[ID],
                                 persist: Option[StorageLevel],
                                 options: Seq[SparkParam] = Seq.empty,
                                 keyFields: Seq[NonEmptyString] = Seq.empty,
                                 metadata: Seq[SparkParam] = Seq.empty
                               ) extends SourceConfig {
    val streamable: Boolean = false // todo: make custom source streamable
  }

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
    val windowBy: Option[StreamWindowing]
    // additional validation will be imposed on the required number
    // of parent sources depending on virtual source type.
  }

  /**
   * Sql virtual source configuration
   *
   * @param id            Virtual source ID
   * @param description   Source description
   * @param parentSources Non-empty sequence of parent sources
   * @param query         SQL query to build virtual source from parent sources
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   * @param metadata      List of metadata parameters specific to this source
   */
  final case class SqlVirtualSourceConfig(
                                           id: ID,
                                           description: Option[NonEmptyString],
                                           parentSources: NonEmptyStringSeq,
                                           query: NonEmptyString,
                                           persist: Option[StorageLevel],
                                           save: Option[FileOutputConfig],
                                           keyFields: Seq[NonEmptyString] = Seq.empty,
                                           metadata: Seq[SparkParam] = Seq.empty
                                         ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
    val streamable: Boolean = false
    val windowBy: Option[StreamWindowing] = None
  }

  /**
   * Join virtual source configuration
   *
   * @param id            Virtual source ID
   * @param description   Source description
   * @param parentSources Sequence of exactly two parent sources.
   * @param joinBy        Non-empty sequence of columns to join by.
   * @param joinType      Spark join type.
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   * @param metadata      List of metadata parameters specific to this source
   */
  final case class JoinVirtualSourceConfig(
                                            id: ID,
                                            description: Option[NonEmptyString],
                                            parentSources: DoubleElemStringSeq,
                                            joinBy: NonEmptyStringSeq,
                                            joinType: SparkJoinType,
                                            persist: Option[StorageLevel],
                                            save: Option[FileOutputConfig],
                                            keyFields: Seq[NonEmptyString] = Seq.empty,
                                            metadata: Seq[SparkParam] = Seq.empty
                                          ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
    val streamable: Boolean = false
    val windowBy: Option[StreamWindowing] = None
  }

  /**
   * Filter virtual source configuration
   *
   * @param id            Virtual source ID
   * @param description   Source description
   * @param parentSources Sequence containing exactly one source.
   * @param expr          Non-empty sequence of spark sql expression used to filter source.
   *                      All expressions must return boolean. Source is filtered using logical
   *                      conjunction of all provided expressions.
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   * @param metadata      List of metadata parameters specific to this source
   */
  final case class FilterVirtualSourceConfig(
                                              id: ID,
                                              description: Option[NonEmptyString],
                                              parentSources: SingleElemStringSeq,
                                              expr: Seq[Column] Refined NonEmpty,
                                              persist: Option[StorageLevel],
                                              save: Option[FileOutputConfig],
                                              windowBy: Option[StreamWindowing],
                                              keyFields: Seq[NonEmptyString] = Seq.empty,
                                              metadata: Seq[SparkParam] = Seq.empty
                                            ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
    val streamable: Boolean = true
  }

  /**
   * Select virtual source configuration
   *
   * @param id            Virtual source ID
   * @param description   Source description
   * @param parentSources Sequence containing exactly one source.
   * @param expr          Non-empty sequence of spark sql expression used select column from parent source.
   *                      One expression per each resultant column
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   * @param metadata      List of metadata parameters specific to this source
   */
  final case class SelectVirtualSourceConfig(
                                              id: ID,
                                              description: Option[NonEmptyString],
                                              parentSources: SingleElemStringSeq,
                                              expr: Seq[Column] Refined NonEmpty,
                                              persist: Option[StorageLevel],
                                              save: Option[FileOutputConfig],
                                              windowBy: Option[StreamWindowing],
                                              keyFields: Seq[NonEmptyString] = Seq.empty,
                                              metadata: Seq[SparkParam] = Seq.empty
                                            ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
    val streamable: Boolean = true
  }

  /**
   * Aggregate virtual source configuration
   *
   * @param id            Virtual source ID
   * @param description   Source description
   * @param parentSources Sequence containing exactly one source.
   * @param groupBy       Non-empty sequence of columns by which to perform grouping
   * @param expr          Non-empty sequence of spark sql expression used to get aggregated columns.
   *                      One expression per each resultant column
   * @param persist       Spark storage level in order to persist dataframe during job execution.
   * @param save          Configuration to save virtual source as a file.
   * @param keyFields     Sequence of key fields (columns that identify data row)
   * @param metadata      List of metadata parameters specific to this source
   */
  final case class AggregateVirtualSourceConfig(
                                                 id: ID,
                                                 description: Option[NonEmptyString],
                                                 parentSources: SingleElemStringSeq,
                                                 groupBy: NonEmptyStringSeq,
                                                 expr: Seq[Column] Refined NonEmpty,
                                                 persist: Option[StorageLevel],
                                                 save: Option[FileOutputConfig],
                                                 keyFields: Seq[NonEmptyString] = Seq.empty,
                                                 metadata: Seq[SparkParam] = Seq.empty
                                               ) extends VirtualSourceConfig {
    val parents: Seq[String] = parentSources.value
    val streamable: Boolean = false
    val windowBy: Option[StreamWindowing] = None
  }

  /**
   * Data Quality job configuration section describing sources
   *
   * @param table Sequence of table sources (read from JDBC connections)
   * @param hive  Sequence of Hive table sources
   * @param kafka Sequence of sources based on Kafka topics
   * @param greenplum Sequence of greenplum sources (read from pivotal connections)
   * @param file  Sequence of file sources
   * @param custom Sequence of custom sources
   */
  final case class SourcesConfig(
                                  table: Seq[TableSourceConfig] = Seq.empty,
                                  hive: Seq[HiveSourceConfig] = Seq.empty,
                                  kafka: Seq[KafkaSourceConfig] = Seq.empty,
                                  greenplum: Seq[GreenplumSourceConfig] = Seq.empty,
                                  file: Seq[FileSourceConfig] = Seq.empty,
                                  custom: Seq[CustomSource] = Seq.empty
                                ) {
    def getAllSources: Seq[SourceConfig] =
      this.productIterator.toSeq.flatMap(_.asInstanceOf[Seq[Any]]).map(_.asInstanceOf[SourceConfig])
  }

  /**
   * Data Quality job configuration section describing streams
   *
   * @param kafka Sequence of streams based on Kafka topics
   * @param file Sequence of streams based on file sources
   */
  final case class StreamSourcesConfig(
                                        kafka: Seq[KafkaSourceConfig] = Seq.empty,
                                        file: Seq[FileSourceConfig] = Seq.empty,
                                      ) {
    def getAllSources: Seq[SourceConfig] =
      this.productIterator.toSeq.flatMap(_.asInstanceOf[Seq[Any]]).map(_.asInstanceOf[SourceConfig])
  }
}
