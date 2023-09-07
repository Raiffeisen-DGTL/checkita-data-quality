package ru.raiffeisen.checkita.sources

case class KafkaConfig(
                        id: String,
                        servers: Seq[String],
                        jaasConfFile: Option[String] = None,
                        parameters: Seq[String] = Seq.empty[String]
                      )
