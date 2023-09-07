package ru.raiffeisen.checkita.utils.io.kafka

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

import scala.collection.mutable

class ProducerCallback extends Callback {

  private val exceptions = mutable.ListBuffer.empty[String]

  def getExceptions: mutable.ListBuffer[String] = exceptions

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      exceptions += exception.toString
    }
  }
}
