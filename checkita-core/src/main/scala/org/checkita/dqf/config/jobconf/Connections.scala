package org.checkita.dqf.config.jobconf

import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.types.string.NonEmptyString
import org.checkita.dqf.config.RefinedTypes.{ID, SparkParam, URI}

object Connections {

  /**
   * Base class for all connection configurations.
   * All connections are described as DQ entities that also might have
   * a list of extra connection parameters (specific to Spark).
   */
  sealed abstract class ConnectionConfig extends JobConfigEntity {
    val parameters: Seq[SparkParam]
  }

  /**
   * Base class for jdbc connection configurations.
   * Such connections must have and URL for connection to database
   * and optional user/password combination
   */
  sealed abstract class JdbcConnectionConfig extends ConnectionConfig {
    val url: URI
    val username: Option[NonEmptyString]
    val password: Option[NonEmptyString]
  }

  /**
   * Connection configuration for SQLite database
   *
   * @param id          Connection Id
   * @param description Connection description
   * @param url         Path to SQLite db-file
   * @param parameters  Sequence of additional connection parameters
   * @param metadata    List of metadata parameters specific to this connection
   */
  final case class SQLiteConnectionConfig(
                                           id: ID,
                                           description: Option[NonEmptyString],
                                           url: URI,
                                           parameters: Seq[SparkParam] = Seq.empty,
                                           metadata: Seq[SparkParam] = Seq.empty
                                         ) extends JdbcConnectionConfig {
    // No need to provide user password for SQLite
    val username: Option[NonEmptyString] = None
    val password: Option[NonEmptyString] = None
  }

  /**
   * Connection configuration for PostgreSQL database
   *
   * @param id          Connection Id
   * @param description Connection description
   * @param url         Url for connection to database
   * @param username    Username used for connection
   * @param password    Password used for connection
   * @param schema      Optional schema to lookup tables from. If omitted, default schema is used.
   * @param parameters  Sequence of additional connection parameters
   * @param metadata    List of metadata parameters specific to this connection
   */
  final case class PostgresConnectionConfig(
                                             id: ID,
                                             description: Option[NonEmptyString],
                                             url: URI,
                                             username: Option[NonEmptyString],
                                             password: Option[NonEmptyString],
                                             schema: Option[NonEmptyString],
                                             parameters: Seq[SparkParam] = Seq.empty,
                                             metadata: Seq[SparkParam] = Seq.empty
                                           ) extends JdbcConnectionConfig

  /**
   * Connection configuration for Oracle database
   *
   * @param id          Connection Id
   * @param description Connection description
   * @param url         Url for connection to database
   * @param username    Username used for connection
   * @param password    Password used for connection
   * @param schema      Optional schema to lookup tables from. If omitted, default schema is used.
   * @param parameters  Sequence of additional connection parameters
   * @param metadata    List of metadata parameters specific to this connection
   */
  final case class OracleConnectionConfig(
                                           id: ID,
                                           description: Option[NonEmptyString],
                                           url: URI,
                                           username: Option[NonEmptyString],
                                           password: Option[NonEmptyString],
                                           schema: Option[NonEmptyString],
                                           parameters: Seq[SparkParam] = Seq.empty,
                                           metadata: Seq[SparkParam] = Seq.empty
                                         ) extends JdbcConnectionConfig

  /**
   * Connection configuration for MySQL database
   *
   * @param id          Connection Id
   * @param description Connection description
   * @param url         Url for connection to database
   * @param username    Username used for connection
   * @param password    Password used for connection
   * @param schema      Optional schema to lookup tables from. If omitted, default schema is used.
   * @param parameters  Sequence of additional connection parameters
   * @param metadata    List of metadata parameters specific to this connection
   */
  final case class MySQLConnectionConfig(
                                          id: ID,
                                          description: Option[NonEmptyString],
                                          url: URI,
                                          username: Option[NonEmptyString],
                                          password: Option[NonEmptyString],
                                          schema: Option[NonEmptyString],
                                          parameters: Seq[SparkParam] = Seq.empty,
                                          metadata: Seq[SparkParam] = Seq.empty
                                        ) extends JdbcConnectionConfig

  /**
   * Connection configuration for MS SQL database
   *
   * @param id          Connection Id
   * @param description Connection description
   * @param url         Url for connection to database
   * @param username    Username used for connection
   * @param password    Password used for connection
   * @param schema      Optional schema to lookup tables from. If omitted, default schema is used.
   * @param parameters  Sequence of additional connection parameters
   * @param metadata    List of metadata parameters specific to this connection
   */
  final case class MSSQLConnectionConfig(
                                          id: ID,
                                          description: Option[NonEmptyString],
                                          url: URI,
                                          username: Option[NonEmptyString],
                                          password: Option[NonEmptyString],
                                          schema: Option[NonEmptyString],
                                          parameters: Seq[SparkParam] = Seq.empty,
                                          metadata: Seq[SparkParam] = Seq.empty
                                        ) extends JdbcConnectionConfig

  /**
   * Connection configuration for H2 database
   *
   * @param id          Connection Id
   * @param description Connection description
   * @param url         Url for connection to database
   * @param parameters  Sequence of additional connection parameters
   * @param metadata    List of metadata parameters specific to this connection
   */
  final case class H2ConnectionConfig(
                                       id: ID,
                                       description: Option[NonEmptyString],
                                       url: URI,
                                       parameters: Seq[SparkParam] = Seq.empty,
                                       metadata: Seq[SparkParam] = Seq.empty
                                     ) extends JdbcConnectionConfig {
    // No need to provide user password for H2
    val username: Option[NonEmptyString] = None
    val password: Option[NonEmptyString] = None
  }

  /**
   * Connection configuration for Greenplum database
   *
   * @param id          Connection Id
   * @param description Connection description
   * @param url         Url for connection to database
   * @param username    Username used for connection
   * @param password    Password used for connection
   * @param schema      Optional schema to lookup tables from. If omitted, default schema is used.
   * @param parameters  Sequence of additional connection parameters
   * @param metadata    List of metadata parameters specific to this connection
   */
  final case class GreenplumConnectionConfig(
                                             id: ID,
                                             description: Option[NonEmptyString],
                                             url: URI,
                                             username: Option[NonEmptyString],
                                             password: Option[NonEmptyString],
                                             schema: Option[NonEmptyString],
                                             parameters: Seq[SparkParam] = Seq.empty,
                                             metadata: Seq[SparkParam] = Seq.empty
                                           ) extends ConnectionConfig

  /**
   * Connection configuration for Kafka
   *
   * @param id          Connection Id
   * @param description Connection description
   * @param servers     Kafka brokers
   * @param parameters  Sequence of additional connection parameters
   * @param metadata    List of metadata parameters specific to this connection
   */
  final case class KafkaConnectionConfig(
                                          id: ID,
                                          description: Option[NonEmptyString],
                                          servers: Seq[URI] Refined NonEmpty,
                                          parameters: Seq[SparkParam] = Seq.empty,
                                          metadata: Seq[SparkParam] = Seq.empty
                                        ) extends ConnectionConfig

  /**
   * Connection configuration for ClickHouse
   *
   * @param id          Connection Id
   * @param description Connection description
   * @param url         Url for connection to database
   * @param username    Username used for connection
   * @param password    Password used for connection
   * @param schema      Optional schema to lookup tables from. If omitted, default schema is used.
   * @param parameters  Sequence of additional connection parameters
   * @param metadata    List of metadata parameters specific to this connection
   */
  final case class ClickHouseConnectionConfig(
                                               id: ID,
                                               description: Option[NonEmptyString],
                                               url: URI,
                                               username: Option[NonEmptyString],
                                               password: Option[NonEmptyString],
                                               schema: Option[NonEmptyString],
                                               parameters: Seq[SparkParam] = Seq.empty,
                                               metadata: Seq[SparkParam] = Seq.empty
                                             ) extends JdbcConnectionConfig

  /**
   * Data Quality job configuration section describing connections to external systems.
   *
   * @param kafka      Sequence of Kafka connections
   * @param postgres   Sequence of PostgresSQL connections
   * @param oracle     Sequence of Oracle connection
   * @param sqlite     Sequence of SQLite connections
   * @param mysql      Sequence of MySQL connections
   * @param mssql      Sequence of MS SQL connections
   * @param h2         Sequence of H2 connections
   * @param greenplum  Sequence of Greenplum connections
   * @param clickhouse Sequence of ClickHouse connections
   */
  final case class ConnectionsConfig(
                                      kafka: Seq[KafkaConnectionConfig] = Seq.empty,
                                      postgres: Seq[PostgresConnectionConfig] = Seq.empty,
                                      oracle: Seq[OracleConnectionConfig] = Seq.empty,
                                      sqlite: Seq[SQLiteConnectionConfig] = Seq.empty,
                                      mysql: Seq[MySQLConnectionConfig] = Seq.empty,
                                      mssql: Seq[MSSQLConnectionConfig] = Seq.empty,
                                      h2: Seq[H2ConnectionConfig] = Seq.empty,
                                      greenplum: Seq[GreenplumConnectionConfig] = Seq.empty,
                                      clickhouse: Seq[ClickHouseConnectionConfig] = Seq.empty
                                    ) {
    def getAllConnections: Seq[ConnectionConfig] = 
      this.productIterator.toSeq.flatMap(_.asInstanceOf[Seq[Any]]).map(_.asInstanceOf[ConnectionConfig])
  }
}
