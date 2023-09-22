package ru.raiffeisen.checkita.configs

object ConfigSections extends ConfigEnumRefl[String] {
  val databases: String = "databases"
  val messageBrokers: String = "messageBrokers"
  val sources: String = "sources"
  val virtualSources: String = "virtualSources"
  val metrics: String = "metrics"
  val checks: String = "checks"
  val loadChecks: String = "loadChecks"
  val targets: String = "targets"
  val postprocessing: String = "postprocessing"
  val values: List[String] = getValues(this)
}

object ConfigDbSubTypes extends ConfigEnumRefl[String] {
  val oracle: String = "oracle"
  val postgresql: String = "postgresql"
  val sqlite: String = "sqlite"
  val values: List[String] = getValues(this)
}

object ConfigBrokerTypes extends ConfigEnumRefl[String] {
  val kafka: String = "kafka"
  val values: List[String] = getValues(this)
}

object ConfigDbParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val url: String = "url"
  val user: String = "user"
  val password: String = "password"
  val schema: String = "schema"
  val values: List[String] = getValues(this)
}

object ConfigKafkaParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val servers: String = "servers"
  val jaasConfigFile: String = "jaasConfigFile"
  val parameters: String = "parameters"
  val values: List[String] = getValues(this)
}

object ConfigSourceSubTypes extends ConfigEnumRefl[String] {
  val table: String = "table"
  val hive: String = "hive"
  //  val hbase: String = "hbase" - disable hbase loading until. Reason - need to refactor for newer versions of Spark
  val hdfs: String = "hdfs"
  val kafka: String = "kafka"
  val custom: String = "custom"
  val values: List[String] = getValues(this)
}

object ConfigHdfsFileTypes extends ConfigEnumRefl[String] {
  val fixed: String = "fixed"
  val delimited: String = "delimited"
  val avro: String = "avro"
  val parquet: String = "parquet"
  val orc: String = "orc"
  val delta: String = "delta"
  val values: List[String] = getValues(this)
}

object ConfigSourceParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val date: String = "date"
  val database: String = "database"
  val table: String = "table"
  val query: String = "query"
  val columns: String = "columns"
  val path: String = "path"
  val schema: String = "schema"
  val shortSchema: String = "shortSchema"
  val header: String = "header"
  val delimiter: String = "delimiter"
  val quote: String = "quote"
  val escape: String = "escape"
  val keyFields: String = "keyFields"
  // kafka source parameters:
  val brokerId: String = "brokerId"
  val topics: String = "topics"
  val topicPattern: String = "topicPattern"
  val format: String = "format"
  val options: String = "options"
  val startingOffsets: String = "startingOffsets"
  val endingOffsets: String = "endingOffsets"
  val values: List[String] = getValues(this)
}

object ConfigVirtualSourceSybTypes extends ConfigEnumRefl[String] {
  val filterSql: String = "filterSql"
  val joinSql: String = "joinSql"
  val join: String = "join"
  val values: List[String] = getValues(this)
}

object ConfigVirtualSourceParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val parentSources: String = "parentSources"
  val sql: String = "sql"
  val save: String = "save"
  val persist: String = "persist"
  val joinColumns: String = "joinColumns"
  val joinType: String = "joinType"
  val keyFields: String = "keyFields"
  val values: List[String] = getValues(this)
}

object ConfigLoadCheckSubTypes extends ConfigEnumRefl[String] {
  val exist: String = "exist"
  val encoding: String = "encoding"
  val fileType: String = "fileType"
  val minColNum: String = "minColumnNum"
  val exactColNum: String = "exactColumnNum"
  val values: List[String] = getValues(this)
}

object ConfigLoadCheckParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val source: String = "source"
  val option: String = "option"
  val values: List[String]= getValues(this)
}

object ConfigMetricsSubTypes extends ConfigEnumRefl[String] {
  val file: String = "file"
  val column: String = "column"
  val composed: String = "composed"
  val values: List[String] = getValues(this)
}

object ConfigFileMetrics extends ConfigEnumRefl[String] {
  val rowCount: String = "rowCount"
  val values: List[String] = getValues(this)
}

object ConfigFileMetricParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val description: String = "description"
  val source: String = "source"
  val values: List[String] = getValues(this)
}

object ConfigColMetricParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val description: String = "description"
  val source: String = "source"
  val columns: String = "columns"
  val params: String = "params"
  val values: List[String] = getValues(this)
}

object ConfigComposedMetricParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val description: String = "description"
  val formula: String = "formula"
  val values: List[String] = getValues(this)
}

object ConfigColumnMetrics extends ConfigEnumRefl[MetricConfig] {

  val distinctValues: MetricConfig = MetricConfig("distinctValues")
  val approximateDistinctValues: MetricConfig = MetricConfig(
    "approximateDistinctValues", Seq(MetricParameter("accuracyError", "double", isOptional = true))
  )
  val nullValues: MetricConfig = MetricConfig("nullValues")
  val duplicateValues: MetricConfig = MetricConfig("duplicateValues")
  val emptyValues: MetricConfig = MetricConfig("emptyValues")
  val completeness: MetricConfig = MetricConfig(
    "completeness", Seq(MetricParameter("includeEmptyStrings", "boolean", isOptional = true))
  )
  val sequenceCompleteness: MetricConfig = MetricConfig(
    "sequenceCompleteness",
    Seq(MetricParameter("increment", "integer", isOptional = true))
  )
  val approximateSequenceCompleteness: MetricConfig = MetricConfig(
    "approximateSequenceCompleteness",
    Seq(
      MetricParameter("accuracyError", "double", isOptional = true),
      MetricParameter("increment", "integer", isOptional = true)
    )
  )
  val minString: MetricConfig = MetricConfig("minString")
  val maxString: MetricConfig = MetricConfig("maxString")
  val avgString: MetricConfig = MetricConfig("avgString")
  val stringLength: MetricConfig = MetricConfig(
    "stringLength",
    Seq(MetricParameter("length", "string"), MetricParameter("compareRule", "string"))
  )
  val stringInDomain: MetricConfig = MetricConfig("stringInDomain", Seq(MetricParameter("domain", "stringList")))
  val stringOutDomain: MetricConfig = MetricConfig("stringOutDomain", Seq(MetricParameter("domain", "stringList")))
  val stringValues: MetricConfig = MetricConfig("stringValues", Seq(MetricParameter("compareValue", "string")))
  val regexMatch: MetricConfig = MetricConfig("regexMatch", Seq(MetricParameter("regex", "string")))
  val regexMismatch: MetricConfig = MetricConfig("regexMismatch", Seq(MetricParameter("regex", "string")))
  val formattedDate: MetricConfig = MetricConfig(
    "formattedDate", Seq(MetricParameter("dateFormat", "string", isOptional = true))
  )
  val formattedNumber: MetricConfig = MetricConfig(
    "formattedNumber",
    Seq(
      MetricParameter("precision", "integer"),
      MetricParameter("scale", "integer"),
      MetricParameter("compareRule", "string", isOptional = true)
    )
  )
  val minNumber: MetricConfig = MetricConfig("minNumber")
  val maxNumber: MetricConfig = MetricConfig("maxNumber")
  val sumNumber: MetricConfig = MetricConfig("sumNumber")
  val avgNumber: MetricConfig = MetricConfig("avgNumber")
  val stdNumber: MetricConfig = MetricConfig("stdNumber")
  val castedNumber: MetricConfig = MetricConfig("castedNumber")
  val numberInDomain: MetricConfig = MetricConfig("numberInDomain", Seq(MetricParameter("domain", "doubleList")))
  val numberOutDomain: MetricConfig = MetricConfig("numberOutDomain", Seq(MetricParameter("domain", "doubleList")))
  val numberLessThan: MetricConfig = MetricConfig(
    "numberLessThan",
    Seq(MetricParameter("compareValue", "double"), MetricParameter("includeBound", "boolean", isOptional = true))
  )
  val numberGreaterThan: MetricConfig = MetricConfig(
    "numberGreaterThan",
    Seq(MetricParameter("compareValue", "double"), MetricParameter("includeBound", "boolean", isOptional = true))
  )
  val numberBetween: MetricConfig = MetricConfig(
    "numberBetween",
    Seq(
      MetricParameter("lowerCompareValue", "double"),
      MetricParameter("upperCompareValue", "double"),
      MetricParameter("includeBound", "boolean", isOptional = true)
    )
  )
  val numberNotBetween: MetricConfig = MetricConfig(
    "numberNotBetween",
    Seq(
      MetricParameter("lowerCompareValue", "double"),
      MetricParameter("upperCompareValue", "double"),
      MetricParameter("includeBound", "boolean", isOptional = true)
    )
  )
  val numberValues: MetricConfig = MetricConfig(
    "numberValues",
    Seq(MetricParameter("compareValue", "double", isOptional = true))
  )
  val medianValue: MetricConfig = MetricConfig(
    "medianValue",
    Seq(MetricParameter("accuracyError", "double", isOptional = true))
  )
  val firstQuantile: MetricConfig = MetricConfig(
    "firstQuantile",
    Seq(MetricParameter("accuracyError", "double", isOptional = true))
  )
  val thirdQuantile: MetricConfig = MetricConfig(
    "thirdQuantile",
    Seq(MetricParameter("accuracyError", "double", isOptional = true))
  )
  val getQuantile: MetricConfig = MetricConfig(
    "getQuantile",
    Seq(MetricParameter("accuracyError", "double", isOptional = true), MetricParameter("targetSideNumber", "double"))
  )
  val getPercentile: MetricConfig = MetricConfig(
    "getPercentile",
    Seq(MetricParameter("accuracyError", "double", isOptional = true), MetricParameter("targetSideNumber", "double"))
  )
  val columnEq: MetricConfig = MetricConfig("columnEq")
  val dayDistance: MetricConfig = MetricConfig(
    "dayDistance",
    Seq(MetricParameter("dateFormat", "string", isOptional = true), MetricParameter("threshold", "integer"))
  )
  val levenshteinDistance: MetricConfig = MetricConfig(
    "levenshteinDistance",
    Seq(MetricParameter("threshold", "integer"), MetricParameter("normalize", "boolean", isOptional = true))
  )
  val coMoment: MetricConfig = MetricConfig("coMoment")
  val covariance: MetricConfig = MetricConfig("covariance")
  val covarianceBessel: MetricConfig = MetricConfig("covarianceBessel")
  val topN: MetricConfig = MetricConfig(
    "topN",
    Seq(
      MetricParameter("targetNumber", "integer", isOptional = true),
      MetricParameter("maxCapacity", "integer", isOptional = true)
    )
  )

  val values: List[MetricConfig] = getValues(this)
}

object ConfigChecksSubTypes extends ConfigEnumRefl[String] {
  val trend: String = "trend"
  val snapshot: String = "snapshot"
  val sql: String = "sql"
  val values: List[String] = getValues(this)
}

object ConfigTrendChecks extends ConfigEnumRefl[String] {
  val averageBoundFullCheck: String = "averageBoundFullCheck"
  val averageBoundUpperCheck: String = "averageBoundUpperCheck"
  val averageBoundLowerCheck: String = "averageBoundLowerCheck"
  val averageBoundRangeCheck: String = "averageBoundRangeCheck"
  val topNRankCheck: String = "topNRankCheck"
  val values: List[String] = getValues(this)
}

object ConfigSnapshotChecks extends ConfigEnumRefl[String] {
  val differByLT: String = "differByLT"
  val equalTo: String = "equalTo"
  val greaterThan: String = "greaterThan"
  val lessThan: String = "lessThan"
  val values: List[String] = getValues(this)
}

object ConfigSqlChecks extends ConfigEnumRefl[String] {
  val countEqZero: String = "countEqZero"
  val countNotEqZero: String = "countNotEqZero"
  val values: List[String] = getValues(this)
}

object ConfigCheckParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val description: String = "description"
  val metric: String = "metric"
  val compareMetric: String = "compareMetric"
  val rule: String = "rule"
  val timeWindow: String = "timeWindow"
  val startDate: String = "startDate"
  val threshold: String = "threshold"
  val thresholdLower: String = "thresholdLower"
  val thresholdUpper: String = "thresholdUpper"
  val targetNumber: String = "targetNumber"
  val values: List[String] = getValues(this)
}

object ConfigSqlCheckParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val description: String = "description"
  val source: String = "source"
  val date: String = "date"
  val query: String = "query"
  val values: List[String] = getValues(this)
}

object ConfigTargetsTypes extends ConfigEnumRefl[String] {
  val hive: String = "hive"
  val hdfs: String = "hdfs"
  val results: String = "results"
  val errorCollection: String = "errorCollection"
  val summary: String = "summary"
  val checkAlerts: String = "checkAlerts"
  val values: List[String] = getValues(this)
}

object ConfigTargetsSubTypes extends ConfigEnumRefl[String] {
  val columnMetrics: String = "columnMetrics"
  val fileMetrics: String = "fileMetrics"
  val composedMetrics: String = "composedMetrics"
  val checks: String = "checks"
  val loadChecks: String = "loadChecks"
  val hdfs: String = "hdfs"
  val email: String = "email"
  val kafka: String = "kafka"
  val mattermost: String = "mattermost"
  val values: List[String] = getValues(this)
}

object ConfigHiveTargetParameters extends ConfigEnumRefl[String] {
  val schema: String = "schema"
  val table: String = "table"
  val date: String = "date"
  val partitionColumn: String = "partitionColumn"
  val values: List[String] = getValues(this)
}

object ConfigHdfsTargetParameters extends ConfigEnumRefl[String] {
  val fileFormat: String = "fileFormat"
  val path: String = "path"
  val date: String = "date"
  val delimiter: String = "delimiter"
  val quote: String = "quote"
  val quoted: String = "quoted"
  val escape: String = "escape"
  // for errorCollection:
  val metrics: String = "metrics"
  val dumpSize: String = "dumpSize"
  // for system targets:
  //  val id: String = "id"
  //  val checkList: String = "checkList"
  //  val mailingList: String = "mailingList"
  val values: List[String] = getValues(this)
}

object ConfigErrorCollectionKafkaParameters extends ConfigEnumRefl[String] {
  val topic: String = "topic"
  val brokerId: String = "brokerId"
  val options: String = "options"
  val date: String = "date"
  val metrics: String = "metrics"
  val dumpSize: String = "dumpSize"
  val values: List[String] = getValues(this)
}

object ConfigSummaryEmailTargetParameters extends ConfigEnumRefl[String] {
  val attachMetricErrors: String = "attachMetricErrors"
  val metrics: String = "metrics"
  val dumpSize: String = "dumpSize"
  val mailingList: String = "mailingList"
  val template: String = "template"
  val templateFile: String = "templateFile"
  val values: List[String] = getValues(this)
}

object ConfigSummaryMMTargetParameters extends ConfigEnumRefl[String] {
  val attachMetricErrors: String = "attachMetricErrors"
  val metrics: String = "metrics"
  val dumpSize: String = "dumpSize"
  val recipients: String = "recipients"
  val template: String = "template"
  val templateFile: String = "templateFile"
  val values: List[String] = getValues(this)
}

object ConfigSummaryKafkaTargetParameters extends ConfigEnumRefl[String] {
  val topic: String = "topic"
  val brokerId: String = "brokerId"
  val options: String = "options"
  val values: List[String] = getValues(this)
}

object ConfigEmailAlertTargetParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val checks: String = "checks"
  val mailingList: String = "mailingList"
  val template: String = "template"
  val templateFile: String = "templateFile"
  val values: List[String] = getValues(this)
}

object ConfigMMAlertTargetParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val checks: String = "checks"
  val recipients: String = "recipients"
  val template: String = "template"
  val templateFile: String = "templateFile"
  val values: List[String] = getValues(this)
}

object ConfigKafkaAlertTargetParameters extends ConfigEnumRefl[String] {
  val id: String = "id"
  val checks: String = "checks"
  val topic: String = "topic"
  val brokerId: String = "brokerId"
  val options: String = "options"
  val values: List[String] = getValues(this)
}

object ConfigKafkaResultTargetParameters extends ConfigEnumRefl[String] {
  val results: String = "results"
  val topic: String = "topic"
  val brokerId: String = "brokerId"
  val options: String = "options"
  val values: List[String] = getValues(this)
}
