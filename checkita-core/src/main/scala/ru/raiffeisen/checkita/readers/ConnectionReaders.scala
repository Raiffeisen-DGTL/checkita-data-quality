package ru.raiffeisen.checkita.readers

import org.apache.spark.sql.SparkSession

import ru.raiffeisen.checkita.config.jobconf.Connections._
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.connections.jdbc._
import ru.raiffeisen.checkita.connections.greenplum.PivotalConnection
import ru.raiffeisen.checkita.connections.kafka.KafkaConnection
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.util.Try

object ConnectionReaders {

  /**
   * Base connection reader trait
   * @tparam T Type of connection configuration
   */
  sealed trait ConnectionReader[T <: ConnectionConfig] {
    /**
     * Connection constructor function:
     * builds DQConnection provided with connection configuration of type T
     */
    val constructor: T => DQConnection

    /**
     * Safely reads connection configuration and establishes DQConnection
     * @param config Connection configuration
     * @return Either a valid DQConnection or a list of connection errors.
     */
    def read(config: T): Result[DQConnection] = {
      val conn = Try(constructor(config)).toResult(
        preMsg = s"Unable to setup connection '${config.id.value}' due to following error: "
      )
      val connCheck = conn.flatMap(_.checkConnection)
      conn.combine(connCheck)((c, _) => c)
    }
  }

  /**
   * Kafka connection reader: establishes connection to Kafka brokers.
   */
  implicit object KafkaConnectionReader extends ConnectionReader[KafkaConnectionConfig] {
    val constructor: KafkaConnectionConfig => DQConnection = KafkaConnection
  }

  /**
   * SQLite connection reader: establishes connection to SQLite database file.
   */
  implicit object SQLiteConnectionReader extends ConnectionReader[SQLiteConnectionConfig] {
    val constructor: SQLiteConnectionConfig => DQConnection = SQLiteConnection
  }

  /**
   * Postgres connection reader: establishes connection to PostgreSQL database.
   */
  implicit object PostgresConnectionReader extends ConnectionReader[PostgresConnectionConfig] {
    val constructor: PostgresConnectionConfig => DQConnection = PostgresConnection
  }

  /**
   * Oracle connection reader: establishes connection to Oracle database.
   */
  implicit object OracleConnectionReader extends ConnectionReader[OracleConnectionConfig] {
    val constructor: OracleConnectionConfig => DQConnection = OracleConnection
  }

  /**
   * MySQL connection reader: establishes connection to MySQL database file.
   */
  implicit object MySQLConnectionReader extends ConnectionReader[MySQLConnectionConfig] {
    val constructor: MySQLConnectionConfig => DQConnection = MySQLConnection
  }

  /**
   * MS SQL connection reader: establishes connection to MS SQL database file.
   */
  implicit object MSSQLConnectionReader extends ConnectionReader[MSSQLConnectionConfig] {
    val constructor: MSSQLConnectionConfig => DQConnection = MSSQLConnection
  }

  /**
   * H2 connection reader: establishes connection to H2 database file.
   */
  implicit object H2ConnectionReader extends ConnectionReader[H2ConnectionConfig] {
    val constructor: H2ConnectionConfig => DQConnection = H2Connection
  }

  /**
   * Greenplum connection reader: establishes connection to greenplum database.
   */
  implicit object GreenplumConnectionReader extends ConnectionReader[GreenplumConnectionConfig] {
    val constructor: GreenplumConnectionConfig => DQConnection = PivotalConnection
  }

  /**
   * Greenplum connection reader: establishes connection to greenplum database.
   */
  implicit object ClickHouseConnectionReader extends ConnectionReader[ClickHouseConnectionConfig] {
    val constructor: ClickHouseConnectionConfig => DQConnection = ClickHouseConnection
  }


  /**
   * General connection reader: invokes connection reader that matches provided connection configuration
   */
  implicit object AnyConnectionReader extends ConnectionReader[ConnectionConfig] {
    val constructor: ConnectionConfig => DQConnection = {
      case kafka: KafkaConnectionConfig => KafkaConnection(kafka)
      case sqlite: SQLiteConnectionConfig => SQLiteConnection(sqlite)
      case postgres: PostgresConnectionConfig => PostgresConnection(postgres)
      case oracle: OracleConnectionConfig => OracleConnection(oracle)
      case mysql: MySQLConnectionConfig => MySQLConnection(mysql)
      case mssql: MSSQLConnectionConfig => MSSQLConnection(mssql)
      case h2: H2ConnectionConfig => H2Connection(h2)
      case greenplum: GreenplumConnectionConfig => PivotalConnection(greenplum)
      case clickhouse: ClickHouseConnectionConfig => ClickHouseConnection(clickhouse)
    }
  }

  /**
   * Implicit conversion for connection configurations that enables read method for them.
   * @param config Connection configuration
   * @param reader Implicit connection reader for given connection configuration
   * @tparam T Type of connection configuration
   */
  implicit class ConnectionReaderOps[T <: ConnectionConfig](config: T)
                                                           (implicit reader: ConnectionReader[T]) {
    def read: Result[DQConnection] = reader.read(config)
  }
  
}
