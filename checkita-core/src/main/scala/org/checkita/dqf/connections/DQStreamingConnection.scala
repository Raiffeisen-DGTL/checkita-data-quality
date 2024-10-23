package org.checkita.dqf.connections

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.core.streaming.Checkpoints.Checkpoint
import org.checkita.dqf.readers.SchemaReaders.SourceSchema

/**
 * Trait to be mix in connections that can read streams.
 */
trait DQStreamingConnection { this: DQConnection =>
  
  type CheckpointType <: Checkpoint

  /**
   * Creates initial checkpoint for provided Kafka source configuration.
   *
   * @param sourceConfig Kafka source configuration
   * @return Kafka checkpoint
   */
  def initCheckpoint(sourceConfig: SourceType): CheckpointType

  /**
   * Validates checkpoint structure and makes updates in case if
   * checkpoint structure needs to be changed.
   *
   * @param checkpoint   Checkpoint to validate and fix (if needed).
   * @param sourceConfig Source configuration
   * @return Either original checkpoint if it is valid or a fixed checkpoint.
   */
  def validateOrFixCheckpoint(checkpoint: CheckpointType, sourceConfig: SourceType): CheckpointType
  
  /**
   * Loads stream into a dataframe given the stream configuration
   *
   * @param sourceConfig Stream configuration
   * @param checkpoint   Checkpoint for given stream configuration
   * @param settings     Implicit application settings object
   * @param spark        Implicit spark session object
   * @param schemas      Implicit Map of all explicitly defined schemas (schemaId -> SourceSchema)
   * @return Spark Streaming DataFrame
   */
  def loadDataStream(sourceConfig: SourceType,
                     checkpoint: CheckpointType)
                    (implicit settings: AppSettings,
                     spark: SparkSession,
                     schemas: Map[String, SourceSchema]): DataFrame
}
