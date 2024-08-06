package org.checkita.dqf.connections.jdbc

import org.checkita.dqf.config.jobconf.Connections.ClickHouseConnectionConfig

/**
 * Connection to ClickHouse database
 *
 * @param config Connection configuration
 */
case class ClickHouseConnection(config: ClickHouseConnectionConfig) extends JdbcConnection[ClickHouseConnectionConfig] {
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  protected val connectionUrl: String = "jdbc:clickhouse://" + config.url.value
  protected val jdbcDriver: String = "com.clickhouse.jdbc.ClickHouseDriver"
  protected val currentSchema: Option[String] = config.schema.map(_.value)
}

