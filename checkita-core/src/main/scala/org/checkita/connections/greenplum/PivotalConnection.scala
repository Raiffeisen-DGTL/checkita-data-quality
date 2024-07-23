package org.checkita.connections.greenplum

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.checkita.appsettings.AppSettings
import org.checkita.config.jobconf.Sources.GreenplumSourceConfig
import org.checkita.connections.DQConnection
import org.checkita.config.jobconf.Connections.GreenplumConnectionConfig
import org.checkita.readers.SchemaReaders.SourceSchema
import org.checkita.utils.Common.paramsSeqToMap
import org.checkita.utils.ResultUtils._

import java.sql.DriverManager
import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.Try



/**
 * Connection to Greenplum database
 *
 * @param config Connection configuration
 */
case class PivotalConnection(config: GreenplumConnectionConfig) extends DQConnection
{
  type SourceType = GreenplumSourceConfig

  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  protected val connectionUrl: String = "jdbc:postgresql://" + config.url.value
  protected val currentSchema: Option[String] = config.schema.map(_.value)
  protected val jdbcDriver: String = "org.postgresql.Driver"

  /**
   * Gets basic Greenplum connection properties
   *
   * @return Connection properties
   */
  def getProperties: Properties = {
    val props = new Properties()
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
    val props = getProperties
    props.put("driver", jdbcDriver)
    val connection = DriverManager.getConnection(connectionUrl, props)
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
    paramsSeqToMap(sparkParams).foreach { case (k, v) => props.put(k, v) }
    props.put("url", connectionUrl)
    props.put("dbschema", currentSchema.get)
    props.put("dbtable", sourceConfig.table.map(_.value).get)
    spark.read.format("greenplum").options(props.asScala).load()
  }
}
