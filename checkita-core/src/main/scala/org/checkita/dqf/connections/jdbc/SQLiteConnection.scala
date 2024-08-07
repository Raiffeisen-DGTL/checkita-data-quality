package org.checkita.dqf.connections.jdbc

import org.checkita.dqf.config.jobconf.Connections.SQLiteConnectionConfig

/**
 * Connection to SQLite database
 *
 * @param config Connection configuration
 */
case class SQLiteConnection(config: SQLiteConnectionConfig) extends JdbcConnection[SQLiteConnectionConfig] {
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  protected val connectionUrl: String = "jdbc:sqlite:" + config.url.value
  protected val jdbcDriver: String = "org.sqlite.JDBC"
  protected val currentSchema: Option[String] = None
}
