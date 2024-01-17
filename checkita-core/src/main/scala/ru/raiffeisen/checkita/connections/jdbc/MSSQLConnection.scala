package ru.raiffeisen.checkita.connections.jdbc

import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import scala.util.Try
import ru.raiffeisen.checkita.config.jobconf.Connections.MSSQLConnectionConfig
import ru.raiffeisen.checkita.utils.ResultUtils._

/**
  * Connection to MS SQL database
 *
  * @param config Connection configuration
  */
case class MSSQLConnection(config: MSSQLConnectionConfig) extends JdbcConnection[MSSQLConnectionConfig] {
  val id: String                              = config.id.value
  protected val sparkParams: Seq[String]      = config.parameters.map(_.value)
  protected val connectionUrl: String         = "jdbc:jtds:sqlserver://" + config.url.value
  protected val jdbcDriver: String            = "net.sourceforge.jtds.jdbc.Driver"
  protected val currentSchema: Option[String] = config.schema.map(_.value)

  /**
   * Checks connection.
   *
   * @param spark Implicit spark session object
   * @return Nothing or error message in case if connection is not ready.
   */
  override def checkConnection: Result[Unit] = Try {
    val connection = DriverManager
      .getConnection(connectionUrl, getProperties)
      .prepareStatement("SELECT GETDATE()")
      .execute()
    if (!connection) throw new RuntimeException("Connection invalid")
  }.toResult(preMsg = s"Unable to establish JDBC connection to following url: $connectionUrl due to following error: ")
}
