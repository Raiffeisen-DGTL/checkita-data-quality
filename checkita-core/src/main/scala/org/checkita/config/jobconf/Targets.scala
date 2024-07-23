package org.checkita.config.jobconf

import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.types.string.NonEmptyString
import org.checkita.config.Enums.ResultTargetType
import org.checkita.config.RefinedTypes._
import org.checkita.config.jobconf.Outputs._

object Targets {

  /**
   * Base class for all target configurations
   */
  sealed abstract class TargetConfig

  /**
   * Base class for all result target configurations.
   * All result targets must have non-empty sequence of result types to save.
   */
  sealed abstract class ResultTargetConfig extends TargetConfig {
    val resultTypes: Seq[ResultTargetType] Refined NonEmpty
  }

  /**
   * Base class for all error collection target configurations.
   * All error collection targets must have a sequence of metrics for
   * which errors are collected as well as maximum number of errors
   * to be dumped per each metric (both could be optional with default values).
   */
  sealed abstract class ErrorCollTargetConfig extends TargetConfig {
    val metrics: Seq[NonEmptyString]
    val dumpSize: Option[PositiveInt]
  }

  /**
   * Base class for all summary target configurations.
   * Some of output channels support attachments to the messages.
   * Therefore, it is possible to indicate whether metric errors or failed check attachments are required.
   * In addition, for metric errors attachment it is possible to narrow list of metrics by providing
   * explicit list of IDs and also to limit number of errors per each metric by setting dumpSize.
   */
  sealed abstract class SummaryTargetConfig extends TargetConfig {
    val attachMetricErrors: Boolean
    val attachFailedChecks: Boolean
    val metrics: Seq[NonEmptyString]
    val dumpSize: Option[PositiveInt]
  }

  /**
   * Base class for all check alert target configurations.
   * Check alert targets must have an ID and 
   * sequence of checks for which the alerts are sent.
   */
  sealed abstract class CheckAlertTargetConfig extends TargetConfig {
    val id: ID
    val checks: Seq[NonEmptyString]
  }
  
  /**
   * Result target configuration that is written to a file.
   */
  final case class ResultFileTargetConfig(
                                           resultTypes: Seq[ResultTargetType] Refined NonEmpty,
                                           save: FileOutputConfig
                                         ) extends ResultTargetConfig with SaveToFileConfig
  
  
  /**
   * Error collection target configuration that is written to a file
   */
  final case class ErrorCollFileTargetConfig(
                                              metrics: Seq[NonEmptyString] = Seq.empty,
                                              dumpSize: Option[PositiveInt],
                                              save: FileOutputConfig
                                            ) extends ErrorCollTargetConfig with SaveToFileConfig

  /**
   * Result target configuration with output to Hive table.
   * @param resultTypes Non-empty sequence of result types to save
   * @param schema Hive schema to write into
   * @param table Hive table to write into
   */
  final case class ResultHiveTargetConfig(
                                           resultTypes: Seq[ResultTargetType] Refined NonEmpty,
                                           schema: NonEmptyString,
                                           table: NonEmptyString
                                         ) extends ResultTargetConfig with HiveOutputConfig

  /**
   * Result target configuration with output to Kafka topic
   * @param resultTypes Non-empty sequence of result types to save
   * @param connection reference to Kafka connection ID
   * @param topic Kafka topic name to write into
   * @param options Sequence of additional Kafka options
   */
  final case class ResultKafkaTargetConfig(
                                            resultTypes: Seq[ResultTargetType] Refined NonEmpty,
                                            connection: NonEmptyString,
                                            topic: NonEmptyString,
                                            options: Seq[SparkParam] = Seq.empty,
                                          ) extends ResultTargetConfig with KafkaOutputConfig

  /**
   * Error collection target configuration with output to Hive table.
   * @param schema Hive schema to write into
   * @param table Hive table to write into
   * @param metrics Sequence of metrics to collect errors for.
   *                Default: empty sequence (collect errors for all metrics)
   * @param dumpSize Maximum number of errors collected per each metric.
   */
  final case class ErrorCollHiveTargetConfig(
                                              schema: NonEmptyString,
                                              table: NonEmptyString,
                                              metrics: Seq[NonEmptyString] = Seq.empty,
                                              dumpSize: Option[PositiveInt]
                                            ) extends ErrorCollTargetConfig with HiveOutputConfig

  /**
   * Error collection target configuration with output to a Kafka topic.
   * @param connection reference to Kafka connection ID
   * @param topic Kafka topic name to write into
   * @param options Sequence of additional Kafka options
   * @param metrics Sequence of metrics to collect errors for.
   *                Default: empty sequence (collect errors for all metrics)
   * @param dumpSize Maximum number of errors collected per each metric. Default: 100
   */
  final case class ErrorCollKafkaTargetConfig(
                                               connection: NonEmptyString,
                                               topic: NonEmptyString,
                                               options: Seq[SparkParam] = Seq.empty,
                                               metrics: Seq[NonEmptyString] = Seq.empty,
                                               dumpSize: Option[PositiveInt]
                                             ) extends ErrorCollTargetConfig with KafkaOutputConfig

  /**
   * Summary target configuration with output to recipients via email.
   * @param recipients Non-empty sequence of recipients' emails.
   * @param attachMetricErrors Boolean flag indicating whether to attach 
   *                           metric error collection report to summary email.
   *                           Default: false
   * @param metrics Sequence of metrics to collect errors for.
   *                Default: empty sequence (collect errors for all metrics)
   * @param dumpSize Maximum number of errors collected per each metric. Default: 100
   * @param subjectTemplate Mustache template used to customize email subject. If omitted, default subject name is used.
   * @param template Mustache Html template for email body.
   * @param templateFile Location of file with Mustache Html template for email body. 
   * @note Template for email body can be provided either explicitly in `template` argument
   * or read from file provided in `templateFile` argument. Both of these arguments are allowed.
   * Also, if both of these arguments are omitted, then default email body is used.
   */
  final case class SummaryEmailTargetConfig(
                                             recipients: Seq[Email] Refined NonEmpty,
                                             attachMetricErrors: Boolean = false,
                                             attachFailedChecks: Boolean = false,
                                             metrics: Seq[NonEmptyString] = Seq.empty,
                                             dumpSize: Option[PositiveInt],
                                             subjectTemplate: Option[NonEmptyString],
                                             template: Option[NonEmptyString],
                                             templateFile: Option[URI]
                                           ) extends SummaryTargetConfig with EmailOutputConfig

  /**
   * Summary target configuration with output to recipients via Mattermost
   * @param recipients Non-empty sequence of mattermost channel names or usernames
   * @param attachMetricErrors Boolean flag indicating whether to attach 
   *                           metric error collection report to summary email.
   *                           Default: false
   * @param metrics Sequence of metrics to collect errors for.
   *                Default: empty sequence (collect errors for all metrics)
   * @param dumpSize Maximum number of errors collected per each metric. Default: 100
   */
  final case class SummaryMattermostTargetConfig(
                                                  recipients: Seq[MMRecipient] Refined NonEmpty,
                                                  attachMetricErrors: Boolean = false,
                                                  attachFailedChecks: Boolean = false,
                                                  metrics: Seq[NonEmptyString] = Seq.empty,
                                                  dumpSize: Option[PositiveInt],
                                                  template: Option[NonEmptyString],
                                                  templateFile: Option[URI]
                                                ) extends SummaryTargetConfig with MattermostOutputConfig

  /**
   * Summary target configuration with output to a Kafka topic.
   * @param connection reference to Kafka connection ID
   * @param topic Kafka topic name to write into
   * @param options Sequence of additional Kafka options
   */
  final case class SummaryKafkaTargetConfig(
                                             connection: NonEmptyString,
                                             topic: NonEmptyString,
                                             options: Seq[SparkParam] = Seq.empty
                                           ) extends SummaryTargetConfig with KafkaOutputConfig {
    // kafka output does not support attachments, therefore:
    val attachMetricErrors: Boolean = false
    val attachFailedChecks: Boolean = false
    val metrics: Seq[NonEmptyString] = Seq.empty
    val dumpSize: Option[PositiveInt] = None
  }
                                     
  /**
   * Check alert target configuration with output to recipients via email
   * @param id Check alert ID
   * @param recipients Non-empty sequence of recipients' emails.
   * @param checks Sequence of checks to send alerts for. Default: empty sequence (send alerts for all checks)
   * @param subjectTemplate Mustache template used to customize email subject. If omitted, default subject name is used.
   * @param template Mustache Html template for email body.
   * @param templateFile Location of file with Mustache Html template for email body. 
   * @note Template for email body can be provided either explicitly in `template` argument
   * or read from file provided in `templateFile` argument. Both of these arguments are allowed.
   * Also, if both of these arguments are omitted, then default email body is used.
   */
  final case class CheckAlertEmailTargetConfig(
                                                id: ID,
                                                recipients: Seq[Email] Refined NonEmpty,
                                                checks: Seq[NonEmptyString] = Seq.empty,
                                                subjectTemplate: Option[NonEmptyString],
                                                template: Option[NonEmptyString],
                                                templateFile: Option[URI]
                                              ) extends CheckAlertTargetConfig with EmailOutputConfig

  /**
   * Check alert target configuration with output to recipients via Mattermost
   * @param id Check alert ID
   * @param recipients Non-empty sequence of mattermost channel names or usernames
   * @param checks Sequence of checks to send alerts for. Default: empty sequence (send alerts for all checks)
   */
  final case class CheckAlertMattermostTargetConfig(
                                                     id: ID,
                                                     recipients: Seq[MMRecipient] Refined NonEmpty,
                                                     checks: Seq[NonEmptyString] = Seq.empty,
                                                     template: Option[NonEmptyString],
                                                     templateFile: Option[URI]
                                                   ) extends CheckAlertTargetConfig with MattermostOutputConfig
  
  /**
   * Data Quality job configuration section describing result targets.
   * @param file Optional configuration for result targets output to a file
   * @param hive Optional configuration for result targets output to Hive
   * @param kafka Optional configuration for result targets output to a Kafka topic
   */
  final case class ResultTargetsConfig(
                                        file: Option[ResultFileTargetConfig],
                                        hive: Option[ResultHiveTargetConfig],
                                        kafka: Option[ResultKafkaTargetConfig]
                                      ) {
    def getAllResultTargets: Seq[ResultTargetConfig] =
      this.productIterator.toSeq.flatMap(
        t => t.asInstanceOf[Option[Any]].map(_.asInstanceOf[ResultTargetConfig]).toSeq
      )
  }

  /**
   * Data Quality job configuration section describing error collection targets
   * @param file Optional configuration for error collection targets output to a file
   * @param hive Optional configuration for error collection targets output to Hive
   * @param kafka Optional configuration for error collection targets output to a Kafka topic
   */
  final case class ErrorCollTargetsConfig(
                                           file: Option[ErrorCollFileTargetConfig],
                                           hive: Option[ErrorCollHiveTargetConfig],
                                           kafka: Option[ErrorCollKafkaTargetConfig]
                                         ) {
    def getAllErrorCollTargets: Seq[ErrorCollTargetConfig] =
      this.productIterator.toSeq.flatMap(
        t => t.asInstanceOf[Option[Any]].map(_.asInstanceOf[ErrorCollTargetConfig]).toSeq
      )
  }

  /**
   * Data Quality job configuration section describing summary targets
   * @param email Optional configuration for summary target reports sent to recipients via email
   * @param mattermost Optional configuration for summary target reports sent to recipients via mattermost
   * @param kafka Optional configuration for summary target reports sent to a Kafka topic
   */
  final case class SummaryTargetsConfig(
                                         email: Option[SummaryEmailTargetConfig],
                                         mattermost: Option[SummaryMattermostTargetConfig],
                                         kafka: Option[SummaryKafkaTargetConfig]
                                       ) {
    def getAllSummaryTargets: Seq[SummaryTargetConfig] =
      this.productIterator.toSeq.flatMap(
        t => t.asInstanceOf[Option[Any]].map(_.asInstanceOf[SummaryTargetConfig]).toSeq
      )
  }

  /**
   * Data Quality job configuration section describing check alert targets
   * @param email Sequence of configurations for check alert targets sent to recipients via email
   * @param mattermost Sequence of configurations for check alert targets sent to recipients via Mattermost
   */
  final case class CheckAlertTargetsConfig(
                                            email: Seq[CheckAlertEmailTargetConfig] = Seq.empty,
                                            mattermost: Seq[CheckAlertMattermostTargetConfig] = Seq.empty
                                          ) {
    def getAllCheckAlertTargets: Seq[CheckAlertTargetConfig] =
      this.productIterator.toSeq.flatMap(_.asInstanceOf[Seq[Any]]).map(_.asInstanceOf[CheckAlertTargetConfig])
  }

  /**
   * Data Quality job configuration section describing all targets
   * @param results Result targets of all subtypes
   * @param errorCollection Error collection targets of all subtypes
   * @param summary Summary targets of all subtypes
   * @param checkAlerts Check alert targets of all subtypes
   */
  final case class TargetsConfig(
                                  results: Option[ResultTargetsConfig],
                                  errorCollection: Option[ErrorCollTargetsConfig],
                                  summary: Option[SummaryTargetsConfig],
                                  checkAlerts: Option[CheckAlertTargetsConfig]
                                ) {
    def getAllTargets: Seq[TargetConfig] =
      results.map(_.getAllResultTargets).getOrElse(Seq.empty).map(_.asInstanceOf[TargetConfig]) ++
        errorCollection.map(_.getAllErrorCollTargets).getOrElse(Seq.empty).map(_.asInstanceOf[TargetConfig]) ++
        summary.map(_.getAllSummaryTargets).getOrElse(Seq.empty).map(_.asInstanceOf[TargetConfig]) ++
        checkAlerts.map(_.getAllCheckAlertTargets).getOrElse(Seq.empty).map(_.asInstanceOf[TargetConfig])
  }
}
