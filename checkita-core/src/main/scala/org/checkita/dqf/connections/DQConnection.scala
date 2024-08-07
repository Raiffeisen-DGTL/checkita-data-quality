package org.checkita.dqf.connections

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Sources.SourceConfig
import org.checkita.dqf.readers.SchemaReaders.SourceSchema
import org.checkita.dqf.utils.ResultUtils.Result

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
   *
   * @return Nothing or error message in case if connection is not ready.
   */
  def checkConnection: Result[Unit]

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
                    schemas: Map[String, SourceSchema]): DataFrame
}
