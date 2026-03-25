package org.checkita.dqf.connections.jdbc

import org.checkita.dqf.config.jobconf.Connections.GenericJdbcConnectionConfig
import org.checkita.dqf.utils.ResultUtils._

import java.sql.DriverManager
import java.util.Properties
import scala.util.Try

/**
 * Connection to any JDBC-compatible database
 *
 * @param config Connection configuration
 */
case class GenericJdbcConnection(config: GenericJdbcConnectionConfig) extends JdbcConnection[GenericJdbcConnectionConfig] {
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  protected val connectionUrl: String = config.url.value
  protected val jdbcDriver: String = config.driver.value
  protected val currentSchema: Option[String] = config.schema.map(_.value)

  /**
   * Checks connection with explicit driver class registration.
   *
   * @return Nothing or error message in case if connection is not ready.
   */
  override def checkConnection: Result[Unit] = Try {
    Class.forName(jdbcDriver)
    val connProps = new Properties()
    config.username.map(_.value).foreach(connProps.put("user", _))
    config.password.map(_.value).foreach(connProps.put("password", _))
    val connection = DriverManager.getConnection(connectionUrl, connProps)
    val isValid = connection.isValid(60)
    if (!isValid) throw new RuntimeException("Connection invalid")
  }.toResult(preMsg = s"Unable to establish JDBC connection to following url: $connectionUrl due to following error: ")
}
