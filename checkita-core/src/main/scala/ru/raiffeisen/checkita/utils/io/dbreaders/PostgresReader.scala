package ru.raiffeisen.checkita.utils.io.dbreaders

import ru.raiffeisen.checkita.sources.DatabaseConfig

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.TimeZone

case class PostgresReader(config: DatabaseConfig) extends TableReader {

  override val connectionUrl: String = "jdbc:postgresql://" + config.host
  override val jdbcDriver: String = "org.postgresql.Driver"

  override def runQuery[T](query: String,
                           transformOutput: ResultSet => T): T = {
    val connection = getConnection

    val statement = connection.createStatement()
    statement.setFetchSize(1000)

    val queryResult = statement.executeQuery(query)
    val result = transformOutput(queryResult)
    statement.close()
    result
  }

  override def getConnection: Connection = {
    val connectionProperties = new java.util.Properties()
    config.user match {
      case Some(user) => connectionProperties.put("user", user)
      case None       =>
    }
    config.password match {
      case Some(pwd) => connectionProperties.put("password", pwd)
      case None      =>
    }
    connectionProperties.put("driver", "org.postgresql.Driver")

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    DriverManager.getConnection(connectionUrl, connectionProperties)
  }

}
