package ru.raiffeisen.checkita.checks

import ru.raiffeisen.checkita.checks.CheckStatusEnum.CheckResultStatus
import ru.raiffeisen.checkita.configs.ConfigReader
import ru.raiffeisen.checkita.metrics.DQResultTypes.DQResultType
import ru.raiffeisen.checkita.metrics.{DQResultTypes, TypedResult}
import ru.raiffeisen.checkita.utils.{DQSettings, toUtcTimestamp}


case class CheckResult(
                        checkId: String,
                        checkName: String,
                        description: String,
                        sourceId: String,
                        baseMetric: String,
                        comparedMetric: Option[String],
                        comparedThreshold: Double,
                        lowerBound: Option[Double],
                        upperBound: Option[Double],
                        status: String,
                        message: String
                      ) extends TypedResult {

  def toCsvString(jobId: String)(implicit settings: DQSettings): String = {
    val quote = settings.errorFileQuote.getOrElse("\"")
    val delimiter = settings.tmpFileDelimiter.getOrElse(",")
    Seq(
      jobId,
      this.checkId,
      this.checkName,
      this.description,
      this.sourceId,
      this.baseMetric,
      this.comparedMetric.getOrElse(""),
      this.comparedThreshold.toString,
      this.lowerBound match {
        case Some(n) => n.toString
        case None => ""
      },
      this.upperBound match {
        case Some(n) => n.toString
        case None => ""
      },
      this.status,
      this.message,
      settings.referenceDateString,
      settings.executionDateString
    ).mkString(quote, quote + delimiter + quote, quote)
  }

  def toJsonString(jobId: String)(implicit settings: DQSettings): String = {
    val lowerBoundString = lowerBound match {
      case Some(n) => n.toString
      case None => ""
    }

    val upperBoundString = upperBound match {
      case Some(n) => n.toString
      case None => ""
    }

    s"""{
       |  "entityType": "checkResult",
       |  "data": {
       |    "jobId": "$jobId",
       |    "checkId": "$checkId",
       |    "checkName": "$checkName",
       |    "description": "$description",
       |    "sourceId": "$sourceId",
       |    "baseMetric": "$baseMetric",
       |    "comparedMetric": "${comparedMetric.getOrElse("")}",
       |    "comparedThreshold": "${comparedThreshold.toString}",
       |    "lowerBound": "$lowerBoundString",
       |    "upperBound": "$upperBoundString",
       |    "status": "$status",
       |    "message": "$message",
       |    "referenceDate": "${settings.referenceDateString}",
       |    "executionDate": "${settings.executionDateString}"
       |  }
       |}""".stripMargin
  }

  override def getType: DQResultType = DQResultTypes.check

  def toDbFormat(implicit configuration: ConfigReader,
                 settings: DQSettings): CheckResultFormatted = CheckResultFormatted(
    configuration.jobId,
    this.checkId,
    this.checkName,
    this.description,
    this.sourceId,
    this.baseMetric,
    this.comparedMetric.getOrElse(""),
    this.comparedThreshold,
    this.lowerBound match {
      case Some(n) => n.toString
      case None => ""
    },
    this.upperBound match {
      case Some(n) => n.toString
      case None => ""
    },
    this.status,
    this.message,
    toUtcTimestamp(settings.referenceDate),
    toUtcTimestamp(settings.executionDate)
  )
}

case class LoadCheckResult(
                            checkId: String,
                            checkName: String,
                            sourceId: String,
                            expected: String,
                            status: CheckResultStatus,
                            message: String = ""
                          ) extends TypedResult {
  override def getType: DQResultType = DQResultTypes.load

  def toDbFormat(implicit configuration: ConfigReader,
                 settings: DQSettings): LoadCheckResultFormatted = LoadCheckResultFormatted(
    configuration.jobId,
    this.checkId,
    this.checkName,
    this.sourceId,
    this.expected,
    this.status.toString,
    this.message,
    toUtcTimestamp(settings.referenceDate),
    toUtcTimestamp(settings.executionDate)
  )

  def toCsvString(jobId: String)(implicit settings: DQSettings): String = {
    val quote = settings.errorFileQuote.getOrElse("\"")
    val delimiter = settings.tmpFileDelimiter.getOrElse(",")
    Seq(
      jobId,
      this.checkId,
      this.checkName,
      this.sourceId,
      this.expected,
      this.status.toString,
      this.message,
      settings.referenceDateString,
      settings.executionDateString
    ).mkString(quote, quote + delimiter + quote, quote)
  }

  def toJsonString(jobId: String)(implicit settings: DQSettings): String =
    s"""{
       |  "entityType": "loadCheckResult",
       |  "data": {
       |    "jobId": "$jobId",
       |    "checkId": "$checkId",
       |    "checkName": "$checkName",
       |    "sourceId": "$sourceId",
       |    "expected": "$expected",
       |    "status": "${status.toString}",
       |    "message": "$message",
       |    "referenceDate": "${settings.referenceDateString}",
       |    "executionDate": "${settings.executionDateString}"
       |  }
       |}""".stripMargin
}
