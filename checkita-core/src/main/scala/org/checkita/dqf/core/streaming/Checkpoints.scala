package org.checkita.dqf.core.streaming

import org.apache.spark.sql.Row
import org.checkita.dqf.config.appconf.StreamConfig
import org.checkita.dqf.config.jobconf.Sources.{KafkaSourceConfig, SourceConfig}

object Checkpoints {

  sealed trait Checkpoint {
    val id: String
    def update(row: Row, colIdxMap: Map[String, Int])(implicit streamConf: StreamConfig): Checkpoint
    def merge(other: Checkpoint): Checkpoint
  }

  object Checkpoint {
    def init[T <: SourceConfig](config: T): Checkpoint = config match {
      case kafkaConfig: KafkaSourceConfig => KafkaCheckpoint(kafkaConfig.id.value, Map.empty)
      case _ => throw new IllegalArgumentException(
        s"Unable to initialize checkpoint for source '${config.id.value}'. " +
          "This source does not support checkpointing."
      )
    }
  }

  final case class KafkaCheckpoint(id: String,
                                   currentOffsets: Map[(String, Int), Long]) extends Checkpoint {

    private def updateOffsets(current: Map[(String, Int), Long],
                              offset: ((String, Int), Long)): Map[(String, Int), Long] = {
      val prevOffset = current.getOrElse(offset._1, Long.MinValue)
      if (offset._2 > prevOffset) current + offset else current
    }

    override def update(row: Row, colIdxMap: Map[String, Int])(implicit streamConf: StreamConfig): Checkpoint = {
      val checkpointData = row.getStruct(colIdxMap(streamConf.checkpointCol))
      val topic = checkpointData.getString(0)
      val partition = checkpointData.getInt(1)
      val offset = checkpointData.getLong(2)
      KafkaCheckpoint(id, updateOffsets(currentOffsets, ((topic, partition), offset)))
    }

    override def merge(other: Checkpoint): Checkpoint = {
      val that = other.asInstanceOf[KafkaCheckpoint]
      val updatedOffsets = that.currentOffsets.foldLeft(this.currentOffsets)(updateOffsets)
      KafkaCheckpoint(id, updatedOffsets)
    }
  }
  
}
