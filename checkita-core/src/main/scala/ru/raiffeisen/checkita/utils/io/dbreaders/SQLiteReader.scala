package ru.raiffeisen.checkita.utils.io.dbreaders

import ru.raiffeisen.checkita.sources.DatabaseConfig

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.TimeZone

case class SQLiteReader(config: DatabaseConfig) extends TableReader {

  override val connectionUrl: String = "jdbc:sqlite:" + config.host
  override val jdbcDriver: String = "org.sqlite.JDBC"

  override def runQuery[T](query: String,
                           transformOutput: ResultSet => T): T = {
    val metricDBPath: String = "jdbc:sqlite:" + config.host
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("driver", "org.sqlite.JDBC")

    val connection =
      DriverManager.getConnection(metricDBPath, connectionProperties)

    val statement = connection.createStatement()
    statement.setFetchSize(1000)

    val queryResult = statement.executeQuery(query)
    val result = transformOutput(queryResult)
    statement.close()
    result
  }

  override def getConnection: Connection = {
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("driver", "org.sqlite.JDBC")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    DriverManager.getConnection(connectionUrl, connectionProperties)
  }

}
