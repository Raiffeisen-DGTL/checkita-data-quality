package ru.raiffeisen.checkita.connections.jdbc

import ru.raiffeisen.checkita.config.jobconf.Connections.PostgresConnectionConfig

/**
 * Connection to PostgresSQL database
 *
 * @param config Connection configuration
 */
case class PostgresConnection(config: PostgresConnectionConfig) extends JdbcConnection[PostgresConnectionConfig] {
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  protected val connectionUrl: String = 
    "jdbc:postgresql://" + config.url.value + config.schema.map(s => s"?currentSchema=${s.value}").getOrElse("")
  protected val jdbcDriver: String = "org.postgresql.Driver"
  protected val currentSchema: Option[String] = config.schema.map(_.value)
}
