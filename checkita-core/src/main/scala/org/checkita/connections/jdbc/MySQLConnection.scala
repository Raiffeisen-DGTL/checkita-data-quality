package org.checkita.connections.jdbc

import org.checkita.config.jobconf.Connections.MySQLConnectionConfig

/**
 * Connection to MySQL database
 *
 * @param config Connection configuration
 */
case class MySQLConnection(config: MySQLConnectionConfig) extends JdbcConnection[MySQLConnectionConfig] {
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  protected val connectionUrl: String = "jdbc:mysql://" + config.url.value
  protected val jdbcDriver: String = "com.mysql.cj.jdbc.Driver"
  protected val currentSchema: Option[String] = config.schema.map(_.value)
}
