package org.checkita.dqf.connections.iceberg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Connections.IcebergConnectionConfig
import org.checkita.dqf.config.jobconf.Sources.IcebergSourceConfig
import org.checkita.dqf.connections.DQConnection
import org.checkita.dqf.readers.SchemaReaders.SourceSchema
import org.checkita.dqf.utils.Common.paramsSeqToMap
import org.checkita.dqf.utils.ResultUtils._

import scala.util.Try

/**
 * Connection to Apache Iceberg catalog
 *
 * @param config Connection configuration
 */
case class IcebergConnection(config: IcebergConnectionConfig) extends DQConnection {
  type SourceType = IcebergSourceConfig

  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  private val catalogName: String = config.catalogName.value
  private val catalogType: String = config.catalogType.value
  private val warehouse: Option[String] = config.warehouse.map(_.value)
  private val catalogUri: Option[String] = config.catalogUri.map(_.value)

  /**
   * Checks connection by verifying Iceberg runtime is on the classpath.
   *
   * @return Nothing or error message in case if connection is not ready.
   */
  def checkConnection: Result[Unit] = Try {
    Class.forName("org.apache.iceberg.spark.SparkCatalog")
    ()
  }.toResult(preMsg = s"Unable to verify Iceberg connection '$id'. " +
    "Ensure iceberg-spark-runtime JAR is on the classpath. Error: ")

  /**
   * Configures the Spark session with Iceberg catalog properties.
   *
   * @param spark Spark session to configure
   */
  private def configureCatalog(implicit spark: SparkSession): Unit = {
    val prefix = s"spark.sql.catalog.$catalogName"
    spark.conf.set(prefix, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"$prefix.type", catalogType)
    warehouse.foreach(w => spark.conf.set(s"$prefix.warehouse", w))
    catalogUri.foreach(u => spark.conf.set(s"$prefix.uri", u))
    paramsSeqToMap(sparkParams).foreach { case (k, v) => spark.conf.set(s"$prefix.$k", v) }
  }

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
    configureCatalog
    val db = sourceConfig.database.map(_.value).getOrElse("default")
    val table = sourceConfig.table.value
    val fqn = s"$catalogName.$db.$table"
    spark.table(fqn)
  }
}
