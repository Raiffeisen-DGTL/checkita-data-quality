package org.checkita.dqf.targets.builders.notification

import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.Enums.TemplateFormat
import org.checkita.dqf.connections.BinaryAttachment
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.storage.Models.{CheckResult, ResultMetricError, ResultSummaryMetrics}
import org.checkita.dqf.storage.Serialization._
import org.checkita.dqf.targets.builders.BuildHelpers
import org.checkita.dqf.utils.Templating.renderTemplate

import java.nio.charset.StandardCharsets
import scala.reflect.runtime.universe.TypeTag

trait NotificationBuilder extends BuildHelpers {

  protected val defaultHtmlTemplate: String
  protected val defaultMarkdownTemplate: String
  protected val defaultSubjectTemplate: String

  protected def buildBody(summaryMetrics: ResultSummaryMetrics,
                          format: TemplateFormat,
                          template: Option[String],
                          templateFile: Option[String])(implicit settings: AppSettings): String = {
      val defaultTemplate = format match {
        case TemplateFormat.Html => defaultHtmlTemplate
        case TemplateFormat.Markdown => defaultMarkdownTemplate
        case _ => ""
      }
      val finalTemplate = template orElse templateFile.flatMap(readTemplate) getOrElse defaultTemplate
      renderTemplate(finalTemplate, summaryMetrics.getFieldsMap)
    }

  protected def buildErrorsAttachment(errors: Seq[ResultMetricError],
                                      requested: Seq[String],
                                      dumpSize: Int)(implicit settings: AppSettings): Option[BinaryAttachment] = {
    filterErrors(errors, requested, dumpSize) match {
      case Nil => None
      case errors => Some(BinaryAttachment(
        "metricErrors.tsv", 
        (errors.head.getTsvHeader +: errors.map(_.toTsvString)).mkString("\n").getBytes(StandardCharsets.UTF_8)
      ))
    }
  }

  protected def buildFailedChecksAttachment[T <: CheckResult : TypeTag](checks: Seq[T],
                                                                        fileName: String,
                                                                        requested: Seq[String])
                                                                       (implicit settings: AppSettings): Option[BinaryAttachment] =

    checks.filter(_.status != CalculatorStatus.Success.toString)
      .filter(chk => if (requested.isEmpty) true else requested.contains(chk.checkId)) match {
      case Nil => None
      case failedChecks => Some(BinaryAttachment(
        fileName,
        (failedChecks.head.getTsvHeader +: failedChecks.map(_.toTsvString)).mkString("\n").getBytes(StandardCharsets.UTF_8)
      ))
    }

  protected def getSubject(subjectTemplate: Option[String],
                           summaryMetrics: ResultSummaryMetrics)
                          (implicit settings: AppSettings): String =
    renderTemplate(subjectTemplate.getOrElse(defaultSubjectTemplate), summaryMetrics.getFieldsMap)
}
