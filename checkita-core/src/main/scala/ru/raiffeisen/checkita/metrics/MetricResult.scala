package ru.raiffeisen.checkita.metrics

import ru.raiffeisen.checkita.utils.{DQSettings, toUtcTimestamp}

object DQResultTypes extends Enumeration {
  type DQResultType = Value
  val column: DQResultType = Value("Column")
  val composed: DQResultType = Value("Composed")
  val file: DQResultType = Value("File")
  val check: DQResultType = Value("Check")
  val load: DQResultType = Value("Load")
}

trait TypedResult {
  def getType: DQResultTypes.DQResultType
}

trait MetricResult extends TypedResult {
  val jobId: String
  val metricId: String
  val metricName: String
  val result: Double
  val additionalResult: String
  val sourceId: String
}

case class ColumnMetricResult(
                               jobId: String,
                               metricId: String,
                               metricName: String,
                               description: String,
                               sourceId: String,
                               columnNames: Seq[String],
                               params: String,
                               result: Double,
                               additionalResult: String
                             ) extends MetricResult {
  override def getType: DQResultTypes.DQResultType = DQResultTypes.column

  def toDbFormat(implicit settings: DQSettings): ColumnMetricResultFormatted = ColumnMetricResultFormatted(
    this.jobId, this.metricId, this.metricName, this.description, this.sourceId,
    this.columnNames.mkString("{", ", ", "}"), this.params, this.result, this.additionalResult,
    toUtcTimestamp(settings.referenceDate), toUtcTimestamp(settings.executionDate)
  )

  def toJsonString(implicit settings: DQSettings): String =
    s"""{
       |  "entityType": "columnMetricResult",
       |  "data": {
       |    "jobId": "$jobId",
       |    "metricId": "$metricId",
       |    "metricName": "$metricName",
       |    "description": "$description",
       |    "sourceId": "$sourceId",
       |    "columnNames": ${columnNames.mkString("[\"", "\", \"", "\"]")},
       |    "params": $params,
       |    "result": "${result.toString}",
       |    "additionalResult": "$additionalResult",
       |    "referenceDate": "${settings.referenceDateString}",
       |    "executionDate": "${settings.executionDateString}"
       |  }
       |}""".stripMargin
}

case class FileMetricResult(
                             jobId: String,
                             metricId: String,
                             metricName: String,
                             description: String,
                             sourceId: String,
                             result: Double,
                             additionalResult: String
                           ) extends MetricResult {
  override def getType: DQResultTypes.DQResultType = DQResultTypes.file

  def toDbFormat(implicit settings: DQSettings): FileMetricResultFormatted = FileMetricResultFormatted(
    this.jobId, this.metricId, this.metricName, this.description, this.sourceId, this.result, this.additionalResult,
    toUtcTimestamp(settings.referenceDate), toUtcTimestamp(settings.executionDate)
  )

  def toJsonString(implicit settings: DQSettings): String =
    s"""{
       |  "entityType": "fileMetricResult",
       |  "data": {
       |    "jobId": "$jobId",
       |    "metricId": "$metricId",
       |    "metricName": "$metricName",
       |    "description": "$description",
       |    "sourceId": "$sourceId",
       |    "result": "${result.toString}",
       |    "additionalResult": "$additionalResult",
       |    "referenceDate": "${settings.referenceDateString}",
       |    "executionDate": "${settings.executionDateString}"
       |  }
       |}""".stripMargin
}

case class ComposedMetricResult(
                                 jobId: String,
                                 metricId: String,
                                 metricName: String,
                                 description: String,
                                 sourceId: String,
                                 formula: String,
                                 result: Double,
                                 additionalResult: String
                               ) extends MetricResult {
  override def getType: DQResultTypes.DQResultType = DQResultTypes.composed

  def toDbFormat(implicit settings: DQSettings): ComposedMetricResultFormatted = ComposedMetricResultFormatted(
    this.jobId, this.metricId, this.metricName, this.description, this.sourceId,
    this.formula, this.result, this.additionalResult,
    toUtcTimestamp(settings.referenceDate), toUtcTimestamp(settings.executionDate)
  )

  def toJsonString(implicit settings: DQSettings): String =
    s"""{
       |  "entityType": "composedMetricResult",
       |  "data": {
       |    "jobId": "$jobId",
       |    "metricId": "$metricId",
       |    "name": "$metricName",
       |    "description": "$description",
       |    "sourceId": "$sourceId",
       |    "formula": "$formula",
       |    "result": "${result.toString}",
       |    "additionalResult": "$additionalResult",
       |    "referenceDate": "${settings.referenceDateString}",
       |    "executionDate": "${settings.executionDateString}"
       |  }
       |}""".stripMargin
}
