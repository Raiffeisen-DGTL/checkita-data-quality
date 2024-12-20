package org.checkita.dqf.connections.jdbc

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Connections.JdbcConnectionConfig
import org.checkita.dqf.config.jobconf.Sources.TableSourceConfig
import org.checkita.dqf.connections.DQConnection
import org.checkita.dqf.readers.SchemaReaders.SourceSchema
import org.checkita.dqf.utils.Common.paramsSeqToMap
import org.checkita.dqf.utils.ResultUtils._

import java.sql.DriverManager
import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.Try


/**
 * Generic JDBC Connection to load Table sources
 * @tparam T Type connection configuration
 */
abstract class JdbcConnection[T <: JdbcConnectionConfig] extends DQConnection {
  type SourceType = TableSourceConfig

  val config: T
  protected val connectionUrl: String
  protected val jdbcDriver: String
  protected val currentSchema: Option[String]

  /**
   * Gets basic JDBC connection properties
   * @return Connection properties
   */
  protected def getProperties: Properties = {
    val props = new Properties()
    props.put("driver", jdbcDriver)
    config.username.map(_.value).foreach(props.put("user", _))
    config.password.map(_.value).foreach(props.put("password", _))
    props
  }

    /**
     * Checks connection.
     *
     * @return Nothing or error message in case if connection is not ready.
     */
  def checkConnection: Result[Unit] = Try {
    val connection = DriverManager.getConnection(connectionUrl, getProperties)
    val isValid = connection.isValid(60)
    if (!isValid) throw new RuntimeException("Connection invalid")
  }.toResult(preMsg = s"Unable to establish JDBC connection to following url: $connectionUrl due to following error: ")


  /**
   * Loads external data into dataframe given a source configuration
   *
   * @param sourceConfig Source configuration
   * @param settings     Implicit application settings object
   * @param spark        Implicit spark session object
   * @param schemas      Implicit Map of all explicitly defined schemas (schemaId -> SourceSchema)
   * @return Spark DataFrame
   */
  def loadDataFrame(sourceConfig: SourceType)
                   (implicit settings: AppSettings,
                    spark: SparkSession,
                    schemas: Map[String, SourceSchema]): DataFrame = {

    val props = getProperties
    paramsSeqToMap(sparkParams).foreach{ case (k, v) => props.put(k, v) }
    props.put("url", connectionUrl)

    (sourceConfig.table.map(_.value), sourceConfig.query.map(_.value)) match {
      case (Some(t), None) => props.put("dbtable", s"$t")
      case (None, Some(q)) => if (settings.allowSqlQueries) {
        props.put("dbtable", s"($q) t")
      } else throw new UnsupportedOperationException(
        "FORBIDDEN: Can't load table source with query due to usage of arbitrary SQL queries is not allowed. " +
          "In order to use arbitrary sql queries set `allowSqlQueries` to true in application settings."
      )
      case (t, q) => throw new IllegalArgumentException(
        s"Either table or query to read must be defined for table source but not both. " +
          s"Got following: table = $t, query = $q."
      )
    }

    val allOptions = props.asScala ++ paramsSeqToMap(sourceConfig.options.map(_.value))
    spark.read.format("jdbc").options(allOptions).load()
  }
}
