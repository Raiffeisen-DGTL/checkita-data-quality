package ru.raiffeisen.checkita.utils.io.dbreaders

import ru.raiffeisen.checkita.sources.DatabaseConfig
import oracle.jdbc.pool.OracleDataSource

import java.sql.{Connection, ResultSet}
import java.util.TimeZone

case class ORCLReader(config: DatabaseConfig) extends TableReader {

  override val connectionUrl: String =
    s"jdbc:oracle:thin:@${config.host}:${config.port.get}/${config.service.get}"
  override val jdbcDriver: String = "oracle.jdbc.driver.OracleDriver"

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
    val ods = new OracleDataSource()
    ods.setUser(config.user.get)
    ods.setPassword(config.password.get)
    ods.setLoginTimeout(5)
    ods.setURL(connectionUrl)
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    ods.getConnection()
  }
}
