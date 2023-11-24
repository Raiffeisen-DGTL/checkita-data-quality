package ru.raiffeisen.checkita.connections

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema

/**
 * Trait to be mix in connections that can read streams.
 */
trait DQStreamingConnection { this: DQConnection =>

  /**
   * Loads stream into a dataframe given the stream configuration
   *
   * @param sourceConfig Stream configuration
   * @param settings     Implicit application settings object
   * @param spark        Implicit spark session object
   * @param schemas      Implicit Map of all explicitly defined schemas (schemaId -> SourceSchema)
   * @return Spark Streaming DataFrame
   */
  def loadDataStream(sourceConfig: SourceType)
                    (implicit settings: AppSettings,
                     spark: SparkSession,
                     schemas: Map[String, SourceSchema]): DataFrame
}
