package org.checkita.dqf.writers

import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Files.FileConfig
import org.checkita.dqf.config.jobconf.Outputs.KafkaOutputConfig
import org.checkita.dqf.connections.DQConnection
import org.checkita.dqf.connections.kafka.{KafkaConnection, KafkaOutput}
import org.checkita.dqf.utils.Common.getStringHash
import org.checkita.dqf.utils.ResultUtils._

import java.security.MessageDigest
import scala.util.Try

trait KafkaWriter[T <: KafkaOutputConfig] extends OutputWriter[Seq[String], T] {

  /**
   * Generates kafka message key
   * @param msg Message to sent
   * @param jobId Current job ID
   * @param entityType Entity type that message contains
   * @param settings Implicit application settings object
   * @return Unique Kafka message key
   */
  private def keyGenerator(msg: String, jobId: String, entityType: String)
                          (implicit settings: AppSettings): String = {
    val md5hash = getStringHash(msg)
    s"$entityType@$jobId@${settings.referenceDateTime.render}@${settings.executionDateTime.render}@$md5hash"
  }

  /**
   * Retrieves entity type from kafka message.
   * @param msg Kafka message
   * @return Entity type from kafka message
   */
  private def extractEntity(msg: String): String = {
    val entityPattern = """"entityType": "(.+?)"""".r
    Try {
      entityPattern.findFirstMatchIn(msg).get.group(1)
    }.getOrElse("unknownEntity")
  }
    
  /**
   * Writes result to required output channel given the output configuration.
   *
   * @param result Result to be written
   * @param target Output configuration
   * @return "Success" string in case of successful write operation or a list of errors.
   */
  override def write(target: T,
                     result: Seq[String])(implicit jobId: String,
                                          settings: AppSettings,
                                          spark: SparkSession,
                                          connections: Map[String, DQConnection]): Result[String] = Try {

    val conn = connections.getOrElse(target.connection.value, throw new NoSuchElementException(
      s"Kafka connection with id = '${target.connection.value}' not found."
    ))

    require(conn.isInstanceOf[KafkaConnection],
      s"Kafka '$targetType' output configuration refers to not a Kafka connection.")

    val data = if (settings.aggregatedKafkaOutput) {
      val aggregated = if (result.nonEmpty) Seq(result.mkString("[\n", ",\n", "\n]")) else Seq.empty[String]
      aggregated.map(m => keyGenerator(m, jobId, targetType) -> m)
    } else result.map(m => keyGenerator(m, jobId, extractEntity(m)) -> m)
    (conn, data)
  }.toResult().flatMap {
    case (conn, data) => conn.asInstanceOf[KafkaConnection].writeData(KafkaOutput(
      data, target.topic.value, target.options.map(_.value)
    ))
  }.mapLeft(errs =>
    (s"Unable to write '$targetType' output to Kafka topic '${target.topic.value}' using connection " +
      s"'${target.connection.value}' due to following error: \n" + errs.head) +: errs.tail
  )
}