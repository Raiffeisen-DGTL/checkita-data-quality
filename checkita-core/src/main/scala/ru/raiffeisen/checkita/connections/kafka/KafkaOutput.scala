package ru.raiffeisen.checkita.connections.kafka

/**
 * Defines data to be sent to Kafka topic
 * @param messages Sequence of messages to sent. Message is a tuple: key -> value
 * @param topic Topic to send messages to.
 * @param options Additional Kafka options for sending messages.
 */
case class KafkaOutput(
                        messages: Seq[(String, String)],
                        topic: String,
                        options: Seq[String] = Seq.empty[String]
                      )
