package org.checkita.dqf.targets.builders.notification

import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Outputs.NotificationOutputConfig
import org.checkita.dqf.config.jobconf.Targets.SummaryTargetConfig
import org.checkita.dqf.storage.Models.ResultSet
import org.checkita.dqf.targets.NotificationMessage
import org.checkita.dqf.targets.builders.TargetBuilder
import org.checkita.dqf.utils.ResultUtils._

import scala.util.Try

trait SummaryNotificationBuilder[T <: SummaryTargetConfig with NotificationOutputConfig] 
  extends TargetBuilder[T, NotificationMessage] with NotificationBuilder {

  protected val defaultHtmlTemplate: String =
    """
      |<h3><strong>Data Quality Job Summary</strong></h3>
      |
      |<hr />
      |<p><span style="font-size:12px">Job ID: {{ jobId }}<br />
      |Reference date: {{ referenceDate }}<br />
      |Execution date: {{ executionDate }}</span></p>
      |
      |<hr />
      |<p><span style="font-size:12px">Number of sources: {{ numSources }}<br />
      |Number of metrics: {{ numMetrics }}<br />
      |Number of load checks: {{ numLoadChecks }}<br />
      |Number of metric checks: {{ numChecks }}</span></p>
      |
      |<hr />
      |<h3><strong><span style="font-size:14px">Status: {{ jobStatus }}</span></strong></h3>
      |
      |<hr />
      |<h3><span style="font-size:12px">Metrics with errors: {{ listMetricsWithErrors }}<br />
      |Failed load checks: {{ listFailedLoadChecks }}<br />
      |Failed checks: {{ listFailedChecks }}</span></h3>
      |""".stripMargin

  protected val defaultMarkdownTemplate: String =
    s"""
       |##### Data Quality Job Summary
       |
       |**Job ID**: `{{ jobId }}`
       |**Reference date**: `{{ referenceDate }}`
       |**Execution date**: `{{ executionDate }}`
       |
       |**Number of sources**: `{{ numSources }}`
       |**Number of metrics**: `{{ numMetrics }}`
       |**Number of load checks**: `{{ numLoadChecks }}`
       |**Number of metric checks**: `{{ numChecks }}`
       |
       |**Status**: **`{{ jobStatus }}`**
       |**Metrics with errors**: `{{ listMetricsWithErrors }}`
       |**Failed load checks**: `{{ listFailedLoadChecks }}`
       |**Failed checks**: `{{ listFailedChecks }}`
       |""".stripMargin

  protected val defaultSubjectTemplate: String = s"Data Quality Summary Report for job '{{ jobId }}' at {{ referenceDate }}"

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
    val body = buildBody(
      results.summaryMetrics, target.templateFormat, target.template.map(_.value), target.templateFile.map(_.value)
    )

    val metricsAttachment =
      if (target.attachMetricErrors) buildErrorsAttachment(
        results.metricErrors,
        target.metrics.map(_.value),
        target.dumpSize.map(_.value).getOrElse(settings.errorDumpSize)
      ).toSeq else Seq.empty

    val checksAttachments =
      if (target.attachFailedChecks)
        buildFailedChecksAttachment(results.checks, "failedChecks.tsv", Seq.empty).toSeq ++
          buildFailedChecksAttachment(results.loadChecks, "failedLoadChecks.tsv", Seq.empty).toSeq
      else Seq.empty

    NotificationMessage(
      body,
      getSubject(target.subjectTemplate.map(_.value), results.summaryMetrics),
      target.recipientsList,
      metricsAttachment ++ checksAttachments
    )
  }.toResult(
    preMsg = s"Unable to prepare summary message due to following error:"
  )
}
