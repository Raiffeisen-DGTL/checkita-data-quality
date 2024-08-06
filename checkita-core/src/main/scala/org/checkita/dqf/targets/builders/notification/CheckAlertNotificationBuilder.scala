package org.checkita.dqf.targets.builders.notification

import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Outputs.NotificationOutputConfig
import org.checkita.dqf.config.jobconf.Targets.CheckAlertTargetConfig
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.storage.Models.ResultSet
import org.checkita.dqf.targets.builders.TargetBuilder
import org.checkita.dqf.targets.{EmptyNotificationMessage, NotificationMessage}
import org.checkita.dqf.utils.ResultUtils._

import scala.util.Try

trait CheckAlertNotificationBuilder[T <: CheckAlertTargetConfig with NotificationOutputConfig]
  extends TargetBuilder[T, NotificationMessage] with NotificationBuilder {

  protected val defaultHtmlTemplate: String =
    """
      |<h3><strong>Data Quality Failed Check Alert</strong></h3>
      |
      |<hr/>
      |<p><span style="font-size:12px">Job ID: {{ jobId }}<br/>
      |Reference date: {{ referenceDate }}<br/>
      |Execution date: {{ executionDate }}</span></p>
      |
      |<hr/>
      |<p><strong><span
      |        style="font-size:14px">Some of the watched checks have failed. Please, review attached files. </span></strong></p>
      |""".stripMargin

  protected val defaultMarkdownTemplate: String =
    s"""
       |##### Data Quality Failed Check Alert
       |
       |**Job ID**: `{{ jobId }}`
       |**Reference date**: `{{ referenceDate }}`
       |**Execution date**: `{{ executionDate }}`
       |
       |**Some of the watched checks have failed. Please, review attached files.**
       |""".stripMargin

  protected val defaultSubjectTemplate: String = s"Data Quality Failed Check Alert for job '{{ jobId }}' at {{ referenceDate }}"

  /**
   * Build target output given the target configuration
   *
   * @param target   Target configuration
   * @param results  All job results
   * @param settings Implicit application settings object
   * @param spark    Implicit spark session object
   * @return Target result of required type.
   */
  def build(target: T, results: ResultSet)
           (implicit settings: AppSettings, spark: SparkSession): Result[NotificationMessage] = Try {
    val requested = target.checks.map(_.value)
    val numFailed = Seq(results.checks, results.loadChecks).map(chk =>
      chk.filter(chk => if (requested.isEmpty) true else requested.contains(chk.checkId))
        .count(_.status != CalculatorStatus.Success.toString)
    ).sum

    if (numFailed == 0) EmptyNotificationMessage else {
      val body = buildBody(
        results.summaryMetrics, target.templateFormat, target.template.map(_.value), target.templateFile.map(_.value)
      )

      val checksAttachments =
        buildFailedChecksAttachment(results.checks, "failedChecks.tsv", requested).toSeq ++
          buildFailedChecksAttachment(results.loadChecks, "failedLoadChecks.tsv", requested).toSeq

      NotificationMessage(
        body,
        getSubject(target.subjectTemplate.map(_.value), results.summaryMetrics),
        target.recipientsList,
        checksAttachments
      )
    }
  }.toResult(
    preMsg = s"Unable to prepare message for check alert '${target.id.value}' due to following error:"
  )
}
