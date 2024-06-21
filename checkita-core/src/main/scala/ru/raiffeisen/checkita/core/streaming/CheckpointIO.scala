package ru.raiffeisen.checkita.core.streaming

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import ru.raiffeisen.checkita.core.serialization.API.{encode, decode}
import ru.raiffeisen.checkita.core.serialization.Implicits._
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.util.Try

object CheckpointIO {
  
  type CheckpointRecord = (Long, String, Array[Byte])
  
  private implicit val checkRecordEncoder: Encoder[CheckpointRecord] =
    Encoders.tuple(Encoders.scalaLong, Encoders.STRING, Encoders.BINARY)
  
  private def buildPath(baseDir: String, jobId: String): String = 
    f"$baseDir/${jobId.replaceAll("""\W""", "")}"
  
  def writeCheckpoint(buffer: ProcessorBuffer,
                      executionTime: Long,
                      checkpointDir: String,
                      jobId: String,
                      jobHash: String)(implicit spark: SparkSession): Result[Unit] = Try {
    val writePath = buildPath(checkpointDir, jobId)
    val record = Seq((executionTime, jobHash, encode(buffer)))
    spark.createDataset(record).write.mode("append").parquet(writePath)
  }.toResult("Unable to write checkpoint due to following error:")
  
  def readCheckpoint(checkpointDir: String, 
                     jobId: String,
                     jobHash: String)(implicit spark: SparkSession): Option[ProcessorBuffer] = Try {
    import spark.implicits._
    val readPath = buildPath(checkpointDir, jobId)
    val bufferBytes = spark.read.parquet(readPath).as[CheckpointRecord]
      .filter($"_2" === jobHash)
      .sort($"_1".desc)
      .take(1).head._3
    
    decode[ProcessorBuffer](bufferBytes)
  }.toOption
}
