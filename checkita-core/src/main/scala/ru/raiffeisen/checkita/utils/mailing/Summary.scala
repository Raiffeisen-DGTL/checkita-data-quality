package ru.raiffeisen.checkita.utils.mailing

import ru.raiffeisen.checkita.checks.{CheckResult, CheckStatusEnum, LoadCheckResult}
import ru.raiffeisen.checkita.configs.ConfigReader
import ru.raiffeisen.checkita.utils.DQSettings

case class Summary(
                    jobId: String,
                    sources: Int,
                    metrics: Int,
                    composed_metrics: Int,
                    load_checks: Int,
                    checks: Int,
                    failed_load_checks: Seq[String],
                    failed_checks: Seq[String]
                  ) {

  def this(conf: ConfigReader, checks: Option[Seq[CheckResult]] = None, lc: Option[Seq[LoadCheckResult]] = None) {
    this(
      jobId = conf.jobId,
      sources = conf.sourcesConfigMap.size,
      metrics = conf.metricsBySourceList.size,
      composed_metrics = conf.composedMetrics.length,
      load_checks = conf.loadChecksMap.values.foldLeft(0)(_ + _.size),
      checks = conf.metricsByChecksList.size,
      failed_load_checks = lc.map(x => x.filter(_.status != CheckStatusEnum.Success).map(_.checkId)).getOrElse(Seq.empty),
      failed_checks = checks.map(x => x.filter(_.status != "Success").map(_.checkId)).getOrElse(Seq.empty)
    )
  }

  val status: String = if (failed_checks.length + failed_load_checks.length == 0) "SUCCESS" else "FAILURE"

  // Status is appended in the send_mail script with the log file path
  def toMailString()(implicit settings: DQSettings): String = s"""Job ID: $jobId
                                                                 |Reference date: ${settings.referenceDateString}
                                                                 |Run configuration path: ${settings.jobConf}
                                                                 |
                                                                 |Number of sources: $sources
                                                                 |Number of metrics: $metrics
                                                                 |Number of composed metrics: $composed_metrics
                                                                 |Number of load checks: $load_checks
                                                                 |Number of metric checks: $checks
                                                                 |
                                                                 |Status: $status
                                                                 |Failed load checks: ${failed_load_checks.mkString("[", ", ", "]")}
                                                                 |Failed checks: ${failed_checks.mkString("[", ", ", "]")}
    """.stripMargin

  def toMMString()(implicit settings: DQSettings): String =
    s"""
       |##### DQ Job Summary
       |
       |**Job ID**: `$jobId`
       |**Reference date**: `${settings.referenceDateString}`
       |**Run configuration path**: `${settings.jobConf}`
       |
       |**Number of sources**: `$sources`
       |**Number of metrics**: `$metrics`
       |**Number of composed metrics**: `$composed_metrics`
       |**Number of load checks**: `$load_checks`
       |**Number of metric checks**: `$checks`
       |
       |**Status**: $status ${if (status == "SUCCESS") ":white_check_mark:" else ":alert:"}
       |**Failed load checks**: `${failed_load_checks.mkString("[", ", ", "]")}`
       |**Failed checks**: `${failed_checks.mkString("[", ", ", "]")}`
       |""".stripMargin

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
