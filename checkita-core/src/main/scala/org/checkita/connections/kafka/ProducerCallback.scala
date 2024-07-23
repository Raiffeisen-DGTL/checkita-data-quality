package org.checkita.connections.kafka

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

import scala.collection.mutable

/**
 * Kafka producer callback that collects error messages if any.
 */
class ProducerCallback extends Callback {
  /** Errors accumulator */
  private val exceptions = mutable.ListBuffer.empty[String]

  /**
   * Gets message from exception if any.
   * @param exception Exception to get message from.
   * @return None if no exception otherwise some exception message.
   */
  private def getErrMsg(exception: Exception): Option[String] =
    if (exception != null) Some(exception.toString) else None

  /** On completion callback */
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = 
    getErrMsg(exception).foreach(e => exceptions += e)
  
  /** Exceptions getter */
  def getExceptions: mutable.ListBuffer[String] = exceptions
}
