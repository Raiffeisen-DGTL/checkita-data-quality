package ru.raiffeisen.checkita.utils.mailing

import org.apache.spark.sql.Row
import ru.raiffeisen.checkita.checks.{CheckResult, CheckStatusEnum, CheckSuccess, LoadCheckResult}
import ru.raiffeisen.checkita.configs.ConfigReader
import ru.raiffeisen.checkita.metrics.MetricProcessor.MetricErrors
import ru.raiffeisen.checkita.targets.{CheckAlertEmailTargetConfig, CheckAlertKafkaTargetConfig, CheckAlertMMTargetConfig, SummaryEmailTargetConfig, SummaryKafkaTargetConfig, SummaryMMTargetConfig}
import ru.raiffeisen.checkita.utils.io.kafka.{KafkaManager, KafkaOutput}
import ru.raiffeisen.checkita.utils.io.mattermost.MMManager
import ru.raiffeisen.checkita.utils.io.mattermost.MMModels.MMMessage
import ru.raiffeisen.checkita.utils.{BinaryAttachment, DQSettings, log, sendCheckAlertMail}

import java.nio.charset.StandardCharsets

object NotificationManager {

  private def buildMetricErrorStream(requestedMetrics: Seq[String],
                                     metricErrors: MetricErrors,
                                     dumpSize: Int): BinaryAttachment = {

    val data = "\"METRIC_ID\"\t\"METRIC_COLUMNS\"\t\"ROW_DATA\"\n" + metricErrors.flatMap { m =>
      val metIdWithCols = m._1.split("%~%", -1).toSeq
      val metId = metIdWithCols.head
      val metCols = metIdWithCols.tail.mkString("[", ", ", "]")
      val rowCols = m._2._1

      if (requestedMetrics.isEmpty || requestedMetrics.contains(metId))
        m._2._2.zipWithIndex.filter(_._2 < dumpSize).map(_._1).map { rowData =>
          val rowJson = rowCols.zip(rowData).map{
            case (k, v) => "\"" + k + "\": \"" + v + "\""
          }.mkString("{", ", ", "}")

          metId + "\t" + metCols + "\t" + rowJson
        }
      else Seq.empty[Row]
    }.mkString("\n")

    BinaryAttachment("metricErrors.tsv", data.getBytes(StandardCharsets.UTF_8))
  }

  private val failedChecksCsvHeader = Seq(
    "JOB_ID",
    "CHECK_ID",
    "CHECK_NAME",
    "DESCRIPTION",
    "CHECKED_FILE",
    "BASE_METRIC",
    "COMPARED_METRIC",
    "COMPARED_THRESHOLD",
    "LOWER_BOUND",
    "UPPER_BOUND",
    "STATUS",
    "MESSAGE",
    "EXEC_DATE"
  ).mkString("\"", "\",\"", "\"")

  private val failedLoadChecksCsvHeader = Seq(
    "JOB_ID",
    "CHECK_ID",
    "SOURCE_DATE",
    "SOURCE_ID",
    "CHECK_NAME",
    "EXPECTED",
    "STATUS",
    "MESSAGE"
  ).mkString("\"", "\",\"", "\"")

  private def buildFailedChecksStream(
                                       checks: Seq[String],
                                       finalCheckResults: Seq[CheckResult],
                                       finalLoadCheckResults: Seq[LoadCheckResult]
                                     )(implicit settings: DQSettings,
                                       configuration: ConfigReader): Seq[BinaryAttachment] = {

    val requestedChecks: Seq[CheckResult] = finalCheckResults.filter { x =>
      if (checks.isEmpty) true else checks.contains(x.checkId)
    }
    val requestedLoadChecks: Seq[LoadCheckResult] = finalLoadCheckResults.filter { x =>
      if (checks.isEmpty) true else checks.contains(x.checkId)
    }

    val numOfFailedChecks: Int = requestedChecks.count(checkRes => checkRes.status != CheckSuccess.stringValue) +
      requestedLoadChecks.count(checkRes => checkRes.status != CheckStatusEnum.Success)

    if (numOfFailedChecks > 0) {
      log.warn(s"$numOfFailedChecks of requested check failed. Sending alerts...")

      val failedCheckData: String = requestedChecks
        .filter(checkRes => checkRes.status != CheckSuccess.stringValue)
        .map(x => x.toCsvString(configuration.jobId)).mkString("\n")

      val failedLoadCheckData: String = requestedLoadChecks
        .filter(checkRes => checkRes.status != CheckStatusEnum.Success)
        .map(x => x.toCsvString(configuration.jobId)).mkString("\n")

      Seq(
        (failedChecksCsvHeader, failedCheckData, "failedChecks.csv"),
        (failedLoadChecksCsvHeader, failedLoadCheckData, "failedLoadChecks.csv")
      ).flatMap{ t =>
        if (t._2.nonEmpty) {
          val byteArray = (t._1 + "\n" + t._2).getBytes(StandardCharsets.UTF_8)
          Some(BinaryAttachment(t._3, byteArray))
        } else None
      }
    } else Seq.empty[BinaryAttachment]
  }

  def sendSummary(conf: SummaryEmailTargetConfig,
                  finalCheckResults: Seq[CheckResult],
                  finalLoadCheckResults: Seq[LoadCheckResult],
                  metricErrors: MetricErrors)(implicit settings: DQSettings,
                                              configuration: ConfigReader): Unit = {
    if (settings.notifications) {

      val summary = new Summary(configuration, Some(finalCheckResults), Some(finalLoadCheckResults))
      val text = summary.toMailString()

      val errorsAttachment = if (conf.attachMetricErrors && metricErrors.nonEmpty) {
        log.info("Metric errors are requested as attachment to summary report. Building metric errors report...")
        Seq(buildMetricErrorStream(conf.metrics, metricErrors, conf.dumpSize))
      } else Seq.empty[BinaryAttachment]

      (settings.mailingMode, settings.mailingConfig) match {
        case (Some("internal"), _) =>
          import sys.process.stringSeqToProcess
          Seq(
            "/bin/bash",
            settings.scriptPath.get,
            text,
            summary.status
          ) !!

          log.info("Report have been sent internally.")
        case (Some("external"), Some(mailer)) =>
          implicit val mailerConf: MailerConfiguration = mailer

          if (conf.mailingList.nonEmpty) {
            Mail a Mail(
              from = (mailerConf.address, "Checkita DataQuality"),
              to = conf.mailingList,
              subject = s"Data Quality summary for ${summary.jobId} ",
              message = text,
              attachment = errorsAttachment
            )
            log.info(s"Report have been sent externally to ${conf.mailingList.mkString(" ")}.")
          }
          else log.info(s"No mail address specified.")

        case (x, _) => throw new IllegalArgumentException("Mailing configuration is incorrect.")
      }
    } else log.warn("Notifications are disabled.")
  }

  def sendSummaryToMM(conf: SummaryMMTargetConfig,
                      finalCheckResults: Seq[CheckResult],
                      finalLoadCheckResults: Seq[LoadCheckResult],
                      metricErrors: MetricErrors)(implicit settings: DQSettings,
                                                  configuration: ConfigReader,
                                                  mmManager: Option[MMManager]): Unit = {
    if (settings.notifications) {

      val summary = new Summary(configuration, Some(finalCheckResults), Some(finalLoadCheckResults))
      val text = summary.toMMString()

      val errorsAttachment = if (conf.attachMetricErrors && metricErrors.nonEmpty) {
        log.info("Metric errors are requested as attachment to summary report. Building metric errors report...")
        Seq(buildMetricErrorStream(conf.metrics, metricErrors, conf.dumpSize))
      } else Seq.empty[BinaryAttachment]

      mmManager match {
        case Some(manager) =>
          if (conf.recipients.nonEmpty) manager send MMMessage(conf.recipients, text, errorsAttachment)
          else log.info("No mattermost recipients are specified.")
        case None => log.info("Mattermost manager is not defined. Messages cannot be sent.")
      }

    } else log.warn("Notifications are disabled.")
  }

  def sendSummaryToKafka(conf: SummaryKafkaTargetConfig,
                         finalCheckResults: Seq[CheckResult],
                         finalLoadCheckResults: Seq[LoadCheckResult],
                         writer: KafkaManager)(implicit settings: DQSettings,
                                               configuration: ConfigReader): Unit = {
    val summary = new Summary(configuration, Some(finalCheckResults), Some(finalLoadCheckResults))
    val explicitEntity = s"summary@${configuration.jobId}"
    val data = KafkaOutput(
      Seq(summary.toJsonString),
      conf.topic,
      conf.options,
      Some(explicitEntity)
    )

    val response = writer.writeData(data)

    if (response.isLeft) throw new RuntimeException(
      "Error while writing summary to kafka with following error messages:\n" + response.left.get.mkString("\n")
    ) else log.info(s"Summary report has been successfully sent to Kafka broker with id = ${conf.brokerId}")
  }

  def saveResultsLocally(summary: Summary,
                         checks: Option[Seq[CheckResult]] = None,
                         lc: Option[Seq[LoadCheckResult]] = None)(implicit settings: DQSettings): Unit = {

    if (settings.localTmpPath.isDefined) {
      val runName: String = settings.appName
      val dirPath = settings.localTmpPath.get + "/" +settings.referenceDateString + "/" + runName

      import java.io._

      val dir = new File(dirPath)
      dir.mkdirs()

      // summary.csv
      log.info(s"Saving summary file to $dirPath/summary.csv")
      val summaryFile = new File(dirPath + "/" + "summary.csv")
      summaryFile.createNewFile()
      val s_bw = new BufferedWriter(new FileWriter(summaryFile))
      s_bw.write(summary.toCsvString())
      s_bw.close()

      // failed_load_checks.csv
      if(lc.isDefined) {
        log.info(s"Saving failed load checks to $dirPath/failed_load_checks.csv")
        val lcFile = new File(dirPath + "/" + "failed_load_checks.csv")
        lcFile.createNewFile()
        val lc_bw = new BufferedWriter(new FileWriter(lcFile))
        lc_bw.write(lc.get.filter(_.status != CheckStatusEnum.Success).map(_.toCsvString(summary.jobId)).mkString("\n"))
        lc_bw.close()
      }

      // failed_metric_checks.csv
      if(checks.isDefined) {
        log.info(s"Saving failed metric checks to $dirPath/failed_metric_checks.csv")
        val chkFile = new File(dirPath + "/" + "failed_metric_checks.csv")
        chkFile.createNewFile()
        val chk_bw = new BufferedWriter(new FileWriter(chkFile))
        chk_bw.write(checks.get.filter(_.status != "Success").map(_.toCsvString(summary.jobId)).mkString("\n"))
        chk_bw.close()
      }

      log.info("Local results have been saved.")
    } else log.warn("Local temp path is not defined")
  }



  def sendEmailCheckAlerts(conf: CheckAlertEmailTargetConfig,
                           finalCheckResults: Seq[CheckResult],
                           finalLoadCheckResults: Seq[LoadCheckResult])(implicit settings: DQSettings,
                                                                        configuration: ConfigReader): Unit = {

    val attachments = buildFailedChecksStream(
      conf.checks, finalCheckResults, finalLoadCheckResults
    )

    if (attachments.nonEmpty) {
      (settings.mailingMode, settings.mailingConfig) match {
        case (Some("internal"), _) => ()  // do nothing
        case (Some("external"), Some(mconf)) =>
          val text =
            s"""
               |Job ID: ${configuration.jobId}
               |Reference date**: ${settings.referenceDateString}
               |Run configuration path: ${settings.jobConf}
               |
               |Some of watched checks failed. Please, review attached files.
               |""".stripMargin

          sendCheckAlertMail(conf.mailingList, Some(text), attachments)(mconf)
        case (_, _) => log.error("Mailing configuration is incorrect!")
      }
    }
  }

  def sendMMCheckAlerts(conf: CheckAlertMMTargetConfig,
                        finalCheckResults: Seq[CheckResult],
                        finalLoadCheckResults: Seq[LoadCheckResult])(implicit settings: DQSettings,
                                                                     configuration: ConfigReader,
                                                                     mmManager: Option[MMManager]): Unit = {

    val attachments = buildFailedChecksStream(
      conf.checks, finalCheckResults, finalLoadCheckResults
    )

    if (attachments.nonEmpty) {
      val text =
        s"""
           |##### DQ Failed Checks Alerts :alert:
           |
           |**Job ID**: `${configuration.jobId}`
           |**Reference date**: `${settings.referenceDateString}`
           |**Run configuration path**: `${settings.jobConf}`
           |
           |Some of watched checks failed. Please, review attached files.
           |""".stripMargin

      mmManager match {
        case Some(manager) =>
          if (conf.recipients.nonEmpty) manager send MMMessage(conf.recipients, text, attachments)
          else log.info("No mattermost recipients are specified.")
        case None => log.info("Mattermost manager is not defined. Messages cannot be sent.")
      }
    }
  }

  def sendKafkaCheckAlerts(conf: CheckAlertKafkaTargetConfig,
                           finalCheckResults: Seq[CheckResult],
                           finalLoadCheckResults: Seq[LoadCheckResult],
                           writer: KafkaManager)(implicit settings: DQSettings,
                                                 configuration: ConfigReader): Unit = {

    val requestedChecks: Seq[CheckResult] = finalCheckResults.filter { x =>
      if (conf.checks.isEmpty) true else conf.checks.contains(x.checkId)
    }
    val requestedLoadChecks: Seq[LoadCheckResult] = finalLoadCheckResults.filter { x =>
      if (conf.checks.isEmpty) true else conf.checks.contains(x.checkId)
    }

    val numOfFailedChecks: Int = requestedChecks.count(checkRes => checkRes.status != CheckSuccess.stringValue) +
      requestedLoadChecks.count(checkRes => checkRes.status != CheckStatusEnum.Success)

    if (numOfFailedChecks > 0) {
      log.warn(s"$numOfFailedChecks of requested check failed. Sending alerts...")

      val failedCheckData: Seq[String] = requestedChecks
        .filter(checkRes => checkRes.status != CheckSuccess.stringValue)
        .map(x => x.toJsonString(configuration.jobId))

      val failedLoadCheckData: Seq[String] = requestedLoadChecks
        .filter(checkRes => checkRes.status != CheckStatusEnum.Success)
        .map(x => x.toJsonString(configuration.jobId))

      val explicitEntity = s"checkAlert@${conf.id}@${configuration.jobId}"
      val data = KafkaOutput(
        failedCheckData ++ failedLoadCheckData,
        conf.topic,
        conf.options,
        Some(explicitEntity)
      )

      val response = writer.writeData(data)

      if (response.isLeft) throw new RuntimeException(
        "Error while writing check alerts to kafka with following error messages:\n" + response.left.get.mkString("\n")
      ) else log.info(s"Check alerts have been successfully sent to Kafka broker with id = ${conf.brokerId}")
    }
  }
}
