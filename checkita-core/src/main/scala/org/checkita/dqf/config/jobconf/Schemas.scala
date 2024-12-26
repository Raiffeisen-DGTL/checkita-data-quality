package org.checkita.dqf.config.jobconf

import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.types.string.NonEmptyString
import org.apache.spark.sql.types.DataType
import org.checkita.dqf.config.RefinedTypes.{FixedShortColumn, ID, NonEmptyURISeq, PositiveInt, SparkParam, URI}
import org.checkita.dqf.config.jobconf.Connections.ConnectionConfig

object Schemas {

  /**
   * Base class for column retrieved from explicit schemas
   */
  sealed abstract class Column

  /**
   * General column configuration used to defined columns for delimited schema
   *
   * @param name   Column name
   * @param `type` Column data type (Spark SQL type)
   */
  final case class GeneralColumn(
                                  name: NonEmptyString,
                                  `type`: DataType
                                ) extends Column

  /**
   * Fixed column configuration with full definition (both column type and width are defined).
   * Used to defined columns for fixed-width schema.
   *
   * @param name   Column name
   * @param `type` Column data type (Spark SQL type)
   * @param width  Column width
   */
  final case class FixedFullColumn(
                                    name: NonEmptyString,
                                    `type`: DataType,
                                    width: PositiveInt
                                  ) extends Column

  /**
   * Base class for all schema configurations. All schemas are described as DQ entities.
   */
  sealed abstract class SchemaConfig extends JobConfigEntity

  /**
   * Delimited schema configuration used for delimited files such as CSV or TSV.
   *
   * @param id          Schema ID
   * @param description Schema description
   * @param schema      List of columns in order.
   * @param metadata    List of metadata parameters specific to this schema
   */
  final case class DelimitedSchemaConfig(
                                          id: ID,
                                          description: Option[NonEmptyString],
                                          schema: Seq[GeneralColumn] Refined NonEmpty,
                                          metadata: Seq[SparkParam] = Seq.empty
                                        ) extends SchemaConfig

  /**
   * Fixed schema configuration used for files with fixed column width.
   * Columns are fully defined with their name, type and width.
   *
   * @param id          Schema ID
   * @param description Schema description
   * @param schema      List of columns in order
   * @param metadata    List of metadata parameters specific to this schema
   */
  final case class FixedFullSchemaConfig(
                                          id: ID,
                                          description: Option[NonEmptyString],
                                          schema: Seq[FixedFullColumn] Refined NonEmpty,
                                          metadata: Seq[SparkParam] = Seq.empty
                                        ) extends SchemaConfig

  /**
   * Fixed schema configuration used for files with fixed column width.
   * Columns are defined in short notation with their name and width only.
   * All columns have StringType.
   *
   * @param id          Schema ID
   * @param description Schema description
   * @param schema      List of columns in order (format is "column_name:width", e.g. "zip:5")
   * @param metadata    List of metadata parameters specific to this schema
   */
  final case class FixedShortSchemaConfig(
                                           id: ID,
                                           description: Option[NonEmptyString],
                                           schema: Seq[FixedShortColumn] Refined NonEmpty,
                                           metadata: Seq[SparkParam] = Seq.empty
                                         ) extends SchemaConfig

  /**
   * Avro schema configuration
   *
   * @param id               Schema ID
   * @param description      Schema description
   * @param schema           Path to Avro schema file (.avsc)
   * @param validateDefaults Boolean flag enabling or disabling default values validation in Avro schema.
   * @param metadata         List of metadata parameters specific to this schema
   */
  final case class AvroSchemaConfig(
                                     id: ID,
                                     description: Option[NonEmptyString],
                                     schema: URI,
                                     validateDefaults: Boolean = false,
                                     metadata: Seq[SparkParam] = Seq.empty
                                   ) extends SchemaConfig

  /**
   * Schema configuration that is read from hive catalog
   *
   * @param id             Schema ID
   * @param description    Schema description
   * @param schema         Hive Schema
   * @param table          Hive Table
   * @param excludeColumns Columns to exclude from schema
   *                       (e.g. it might be necessary to exclude table partitioning columns)
   * @param metadata       List of metadata parameters specific to this schema
   */
  final case class HiveSchemaConfig(
                                     id: ID,
                                     description: Option[NonEmptyString],
                                     schema: NonEmptyString,
                                     table: NonEmptyString,
                                     excludeColumns: Seq[NonEmptyString] = Seq.empty,
                                     metadata: Seq[SparkParam] = Seq.empty
                                   ) extends SchemaConfig


  /**
   * Schema configuration that is used to read schema from
   * Confluent Schema Registry.
   *
   * @param id                  Schema ID
   * @param description         Schema description
   * @param baseUrls            List of urls to connect to Schema Registry
   * @param schemaId            Schema ID to search in schema registry
   * @param schemaSubject       Schema subject to search in schema registry
   * @param version             Schema version (by default latest available version is fetched)
   * @param validateDefaults    Boolean flag enabling or disabling default values validation in Avro schema.
   * @param properties          List of additional connection properties: sequence of strings in format `key=value`.
   * @param headers             List of additional HTML headers: sequence of strings in format `key=value`.
   * @param metadata            List of metadata parameters specific to this schema
   * @param connectionTimeoutMs Maximum time in milliseconds to wait for a response from the Schema Registry.
   * @param retryAttempts       Number of retry attempts in case of a failure.
   * @param retryIntervalMs     Delay in milliseconds between retry attempts.
   */
  final case class RegistrySchemaConfig(
                                         id: ID,
                                         description: Option[NonEmptyString],
                                         baseUrls: NonEmptyURISeq,
                                         schemaId: Option[Int],
                                         schemaSubject: Option[NonEmptyString],
                                         version: Option[Int],
                                         validateDefaults: Boolean = false,
                                         properties: Seq[SparkParam] = Seq.empty,
                                         headers: Seq[SparkParam] = Seq.empty,
                                         metadata: Seq[SparkParam] = Seq.empty,
                                         connectionTimeoutMs: Int = 60000,
                                         retryAttempts: Int = 3,
                                         retryIntervalMs: Int = 5000
                                       ) extends SchemaConfig

}
