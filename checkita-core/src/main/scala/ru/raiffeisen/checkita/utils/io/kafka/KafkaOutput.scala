package ru.raiffeisen.checkita.utils.io.kafka

case class KafkaOutput(
                        messages: Seq[String],
                        topic: String,
                        options: Seq[String] = Seq.empty[String],
                        explicitEntity: Option[String] = None
                      )
