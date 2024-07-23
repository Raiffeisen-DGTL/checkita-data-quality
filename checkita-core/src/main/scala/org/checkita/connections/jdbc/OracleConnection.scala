package org.checkita.connections.jdbc

import org.checkita.config.jobconf.Connections.OracleConnectionConfig

/**
 * Connection to Oracle database
 *
 * @param config Connection configuration
 */
case class OracleConnection(config: OracleConnectionConfig) extends JdbcConnection[OracleConnectionConfig] {
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  protected val connectionUrl: String = "jdbc:oracle:thin:@" + config.url.value
  protected val jdbcDriver: String = "oracle.jdbc.driver.OracleDriver"
  protected val currentSchema: Option[String] = config.schema.map(_.value)
}
