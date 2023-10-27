package ru.raiffeisen.checkita.config.jobconf

import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.types.string.NonEmptyString
import org.apache.spark.sql.types.DataType
import ru.raiffeisen.checkita.config.RefinedTypes.{FixedShortColumn, ID, PositiveInt, URI}

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
   * Base class for all schema configurations. All schemas must have an ID.
   */
  sealed abstract class SchemaConfig {
    val id: ID
  }

  /**
   * Delimited schema configuration used for delimited files such as CSV or TSV.
   *
   * @param id     Schema ID
   * @param schema List of columns in order.
   */
  final case class DelimitedSchemaConfig(
                                          id: ID,
                                          schema: Seq[GeneralColumn] Refined NonEmpty
                                        ) extends SchemaConfig

  /**
   * Fixed schema configuration used for files with fixed column width.
   * Columns are fully defined with their name, type and width.
   *
   * @param id     Schema ID
   * @param schema List of columns in order
   */
  final case class FixedFullSchemaConfig(
                                          id: ID,
                                          schema: Seq[FixedFullColumn] Refined NonEmpty
                                        ) extends SchemaConfig
  
  /**
   * Fixed schema configuration used for files with fixed column width.
   * Columns are defined in short notation with their name and width only.
   * All columns have StringType.
   *
   * @param id     Schema ID
   * @param schema List of columns in order (format is "column_name:width", e.g. "zip:5")
   */
  final case class FixedShortSchemaConfig(
                                           id: ID,
                                           schema: Seq[FixedShortColumn] Refined NonEmpty
                                         ) extends SchemaConfig

  /**
   * Avro schema configuration
   *
   * @param id     Schema ID
   * @param schema Path to Avro schema file (.avsc)
   */
  final case class AvroSchemaConfig(
                                     id: ID,
                                     schema: URI,
                                   ) extends SchemaConfig

  /**
   * Schema configuration that is read from hive catalog
   *
   * @param id             Schema ID
   * @param schema         Hive Schema
   * @param table          Hive Table
   * @param excludeColumns Columns to exclude from schema
   *                       (e.g. it might be necessary to exclude table partitioning columns)
   */
  final case class HiveSchemaConfig(
                                     id: ID,
                                     schema: NonEmptyString,
                                     table: NonEmptyString,
                                     excludeColumns: Seq[NonEmptyString] = Seq.empty
                                   ) extends SchemaConfig

}
