package ru.raiffeisen.checkita.utils.mailing

import ru.raiffeisen.checkita.checks.{CheckResult, CheckStatusEnum, LoadCheckResult}
import ru.raiffeisen.checkita.configs.ConfigReader
import ru.raiffeisen.checkita.utils.DQSettings
import ru.raiffeisen.checkita.utils.Templating.renderTemplate

case class Summary(
                    jobId: String,
                    sources: Int,
                    metrics: Int,
                    composed_metrics: Int,
                    load_checks: Int,
                    checks: Int,
                    failed_load_checks: Seq[String],
                    failed_checks: Seq[String],
                    template: Option[String]
                  ) {

  def this(conf: ConfigReader,
           checks: Option[Seq[CheckResult]] = None,
           lc: Option[Seq[LoadCheckResult]] = None,
           template: Option[String] = None) {
    this(
      jobId = conf.jobId,
      sources = conf.sourcesConfigMap.size,
      metrics = conf.metricsBySourceList.size,
      composed_metrics = conf.composedMetrics.length,
      load_checks = conf.loadChecksMap.values.foldLeft(0)(_ + _.size),
      checks = conf.metricsByChecksList.size,
      failed_load_checks = lc.map(x => x.filter(_.status != CheckStatusEnum.Success).map(_.checkId)).getOrElse(Seq.empty),
      failed_checks = checks.map(x => x.filter(_.status != "Success").map(_.checkId)).getOrElse(Seq.empty),
      template
    )
  }

  val status: String = if (failed_checks.length + failed_load_checks.length == 0) "SUCCESS" else "FAILURE"

  def getParamMap(implicit settings: DQSettings): Map[String, String] = Map(
    "jobId" -> jobId,
    "referenceDateTime" -> settings.referenceDateString,
    "jobConfigPath" -> settings.jobConf,
    "numSources" -> sources.toString,
    "numMetrics" -> metrics.toString,
    "numCompMetrics" -> composed_metrics.toString,
    "numLoadChecks" -> load_checks.toString,
    "numChecks" -> checks.toString,
    "jobStatus" -> status,
    "numFailedLoadChecks" -> failed_load_checks.size.toString,
    "numFailedChecks" -> failed_checks.size.toString,
    "listFailedLoadChecks" -> failed_load_checks.mkString("[", ", ", "]"),
    "listFailedChecks" -> failed_checks.mkString("[", ", ", "]")
  )

  def renderSubject(template: String)(implicit settings: DQSettings): String = {
    val params = getParamMap
    renderTemplate(template, params).getOrElse(s"Data Quality summary report.")
  }

  // Status is appended in the send_mail script with the log file path
  def toMailString()(implicit settings: DQSettings): String = {
    val params = getParamMap

    template.flatMap(t => renderTemplate(t, params)).getOrElse(
      s"""Job ID: ${params("jobId")}
         |Reference date: ${params("referenceDateTime")}
         |Run configuration path: ${params("jobConfigPath")}
         |
         |Number of sources: ${params("numSources")}
         |Number of metrics: ${params("numMetrics")}
         |Number of composed metrics: ${params("numCompMetrics")}
         |Number of load checks: ${params("numLoadChecks")}
         |Number of metric checks: ${params("numChecks")}
         |
         |Status: ${params("jobStatus")}
         |Failed load checks: ${params("listFailedLoadChecks")}
         |Failed checks: ${params("listFailedChecks")}
      """.stripMargin
    )
  }

  def toMMString()(implicit settings: DQSettings): String = {
    val params = getParamMap

    template.flatMap(t => renderTemplate(t, params)).getOrElse(
      s"""
         |##### DQ Job Summary
         |
         |**Job ID**: `${params("jobId")}`
         |**Reference date**: `${params("referenceDateTime")}`
         |**Run configuration path**: `${params("jobConfigPath")}`
         |
         |**Number of sources**: `${params("numSources")}`
         |**Number of metrics**: `${params("numMetrics")}`
         |**Number of composed metrics**: `${params("numCompMetrics")}`
         |**Number of load checks**: `${params("numLoadChecks")}`
         |**Number of metric checks**: `${params("numChecks")}`
         |
         |**Status**: $status ${if (status == "SUCCESS") ":white_check_mark:" else ":alert:"}
         |**Failed load checks**: `${params("listFailedLoadChecks")}`
         |**Failed checks**: `${params("listFailedChecks")}`
         |""".stripMargin
    )
  }

  def toJsonString()(implicit settings: DQSettings): String =
    s"""{
       |  "entityType": "summaryReport",
       |  "data": {
       |    "jobId": "$jobId",
       |    "referenceDate": "${settings.referenceDateString}",
       |    "configurationFile": "${settings.jobConf}",
       |    "sourcesCount": "$sources",
       |    "metricsCount": "$metrics",
       |    "composedMetricsCount": "$composed_metrics",
       |    "loadChecksCount": "$load_checks",
       |    "metricChecksCount": "$checks",
       |    "status": "$status",
       |    "failedLoadChecks": ${failed_load_checks.mkString("[\"", "\", \"", "\"]")},
       |    "failedMetricChecks": ${failed_checks.mkString("[\"", "\", \"", "\"]")}
       |  }
       |}""".stripMargin

  def toCsvString()(implicit settings: DQSettings): String = {
    Seq(
      jobId,
      status,
      settings.referenceDateString,
      sources,
      load_checks,
      metrics,
      composed_metrics,
      checks,
      failed_checks.mkString("[", ", ", "]"),
      failed_load_checks.mkString("[", ", ", "]"),
      settings.jobConf
    ).mkString(settings.tmpFileDelimiter.getOrElse(","))
  }

}
