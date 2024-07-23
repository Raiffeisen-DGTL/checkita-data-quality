package org.checkita.connections.jdbc

import org.checkita.config.jobconf.Connections.H2ConnectionConfig

/**
 * Connection to H2 database
 *
 * @param config Connection configuration
 */
case class H2Connection(config: H2ConnectionConfig) extends JdbcConnection[H2ConnectionConfig] {
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  protected val connectionUrl: String = "jdbc:h2:tcp://" + config.url.value
  protected val jdbcDriver: String = "org.h2.Driver"
  protected val currentSchema: Option[String] = None
}
