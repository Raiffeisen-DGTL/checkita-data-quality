package ru.raiffeisen.checkita.connections

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Sources.SourceConfig
import ru.raiffeisen.checkita.utils.ResultUtils.Result

/**
 * Connection to external data sources.
 * All DQ connections must be able to read data given
 * an appropriate source configuration
 */
abstract class DQConnection {
  type SourceType <: SourceConfig
  
  val id: String
  protected val sparkParams: Seq[String]

  /**
   * Checks connection.
   * @return Nothing or error message in case if connection is not ready.
   */
  def checkConnection: Result[Unit]

  /**
   * Loads external data into dataframe given a source configuration
   * @param sourceConfig Source configuration
   * @param settings Implicit application settings object
   * @param spark Implicit spark session object
   * @return Spark DataFrame
   */
  def loadDataframe(sourceConfig: SourceType)(implicit settings: AppSettings, spark: SparkSession): DataFrame
}
