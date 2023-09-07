package ru.raiffeisen.checkita.utils

object enums {
  
  trait ConfigEnum extends Enumeration {
    def names: Set[String]                    = values.map(_.toString)
    def withNameOpt(s: String): Option[Value] = values.find(_.toString == s)
    def contains(s: String): Boolean          = names.contains(s)
  }

  object Entities extends ConfigEnum {
    val databases: Value = Value("Databases")
    val sources: Value   = Value("Sources")
    val virtual: Value   = Value("VirtualSources")
  }

  object Targets extends ConfigEnum {
    val hdfs: Value   = Value("hdfs")
    val hive: Value   = Value("hive")
    val metricError: Value = Value("metricError")
    val checkAlert: Value = Value("checkAlert")
    val summary: Value = Value("summary")
    val system: Value = Value("system")
    type TargetType = Value
  }

  object ResultTargets extends ConfigEnum {
    val columnMetrics: Value   = Value("columnMetrics")
    val fileMetrics: Value   = Value("fileMetrics")
    val composedMetrics: Value = Value("composedMetrics")
    val loadChecks: Value = Value("loadChecks")
    val checks: Value = Value("checks")
    type ResultTargetType = Value
  }

  object KafkaFormats extends ConfigEnum {
    val json: Value = Value("json")
    val xml: Value = Value("xml")
    //    val avro: Value = Value("avro") TODO: add avro support later on.
    type KafkaFormat = Value
  }
}
