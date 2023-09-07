package ru.raiffeisen.checkita.targets

import ru.raiffeisen.checkita.utils.enums.ResultTargets.ResultTargetType
import ru.raiffeisen.checkita.utils.enums.Targets
import ru.raiffeisen.checkita.utils.enums.Targets.TargetType

/**
 * Base target trait
 */
trait TargetConfig {
  def getType: TargetType
}

/**
 * System target configuration. Send an email and save a file if some of the checks are failing
 * @param id Target id
 * @param checkList List of check to watch
 * @param mailList List of notification recipients
 * @param outputConfig Output file configuration
 */
case class SystemTargetConfig(
                               id: String,
                               checkList: Seq[String],
                               mailList: Seq[String],
                               outputConfig: TargetConfig
                             ) extends TargetConfig {
  override def getType: Targets.Value = Targets.system
}

/**
 * Check alerts target configuration. Send an email with respective information if some of the checks are failing.
 * @param id Target id
 * @param checks List of check to watch
 * @param mailingList List of recipients to be notified
 */
case class CheckAlertEmailTargetConfig(id: String,
                                       checks: Seq[String],
                                       mailingList: Seq[String]) extends TargetConfig {
  override def getType: TargetType = Targets.checkAlert
}

/**
 * Check alerts target configuration. Send message to mattermost with respective information
 * if some of the checks are failing. Message can be send either to the channel
 * or to user in direct messages.
 * @param id Target id
 * @param checks List of check to watch
 * @param recipients List of recipients to be notified
 */
case class CheckAlertMMTargetConfig(id: String,
                                    checks: Seq[String],
                                    recipients: Seq[String]) extends TargetConfig {
  override def getType: TargetType = Targets.checkAlert
}

/**
 * Check alerts target configuration. Send a message to kafka topic with respective information
 * if some of the checks are failing.
 * @param id Target id
 * @param checks List of check to watch
 * @param topic Kafka topic to write into
 * @param brokerId Id of Kafka broker to connect to (defined in messageBrokers section of the configuration file)
 * @param options Additional kafka options to use for producer.
 */
case class CheckAlertKafkaTargetConfig(id: String,
                                       checks: Seq[String],
                                       topic: String,
                                       brokerId: String,
                                       options: Seq[String] = Seq.empty[String]
                                      ) extends TargetConfig {
  override def getType: TargetType = Targets.checkAlert
}

/**
 * Summary target configuration. Sends summary email after DQ Job is complete.
 * @param mailingList List of recipients to be notified
 * @param metrics If metric errors are attached than this list specifies the metrics to be reported.
 *                Default is empty and all metric errors will be attached if requested.
 * @param attachMetricErrors Boolean flag to indicate whether report with metric errors should be attached to an email.
 */
case class SummaryEmailTargetConfig(mailingList: Seq[String],
                                    metrics: Seq[String] = Seq.empty[String],
                                    dumpSize: Int = 100,
                                    attachMetricErrors: Boolean = false) extends TargetConfig {
  override def getType: TargetType = Targets.summary
}

/**
 * Summary target configuration. Sends summary to mattermost after DQ Job is complete.
 * Message can be send either to the channel or to user in direct messages.
 * @param recipients List of recipients to be notified
 * @param metrics If metric errors are attached than this list specifies the metrics to be reported.
 *                Default is empty and all metric errors will be attached if requested.
 * @param attachMetricErrors Boolean flag to indicate whether report with metric errors should be attached to an email.
 */
case class SummaryMMTargetConfig(recipients: Seq[String],
                                 metrics: Seq[String] = Seq.empty[String],
                                 dumpSize: Int = 100,
                                 attachMetricErrors: Boolean = false) extends TargetConfig {
  override def getType: TargetType = Targets.summary
}

/**
 * Summary target configuration. Sends summary to kafka topic after DQ Job is complete.
 * Unlike email summary there is no option to attach metric errors.
 * @param topic Kafka topic to write into
 * @param brokerId Id of Kafka broker to connect to (defined in messageBrokers section of the configuration file)
 * @param options Additional kafka options to use for producer.
 */
case class SummaryKafkaTargetConfig(topic: String,
                                    brokerId: String,
                                    options: Seq[String] = Seq.empty[String]) extends TargetConfig {
  override def getType: TargetType = Targets.summary
}



/**
 * HDFS file target configuration
 * @param fileName Name of the output file
 * @param fileFormat File type (csv, avro)
 * @param path desired path
 * @param delimiter delimiter
 * @param quote quote char
 * @param escape escape char
 * @param date output date
 * @param quoteMode quote mode (refer to spark-csv)
 */
case class HdfsTargetConfig(
                             fileName: String,
                             fileFormat: String,
                             path: String,
                             delimiter: Option[String] = None,
                             quote: Option[String] = None,
                             escape: Option[String] = None,
                             date: Option[String] = None,
                             quoteMode: Option[String] = None
                           ) extends TargetConfig {
  override def getType: Targets.Value = Targets.hdfs
}

/**
 * Kafka results target configuration
 * @param results list of results to send to kafka topic
 * @param topic Kafka topic to write into
 * @param brokerId Id of Kafka broker to connect to (defined in messageBrokers section of the configuration file)
 * @param options Additional kafka options to use for producer.
 */
case class ResultsKafkaTargetConfig(results: Seq[ResultTargetType],
                                    topic: String,
                                    brokerId: String,
                                    options: Seq[String] = Seq.empty[String]) extends TargetConfig {
  override def getType: Targets.Value = Targets.hdfs
}

/**
 * Target configuration for metric error collection.
 * @param fileName Name of the output file
 * @param fileFormat File type (csv, orc, avro)
 * @param path Desired file path
 * @param metrics List of metric for error collection. Default is empty and errors for all statusable metrics are collected.
 * @param dumpSize Number of errors (rows where metric had failed) per metric to be written in report. Default is 100 errors per metric.
 * @param delimiter Delimiter
 * @param quote Quote character
 * @param escape Escape character
 * @param date Output date
 * @param quoteMode Quote mode (refer to spark-csv)
 */
case class ErrorCollectionHdfsTargetConfig(fileName: String,
                                           fileFormat: String,
                                           path: String,
                                           metrics: Seq[String] = Seq.empty[String],
                                           dumpSize: Int = 100,
                                           delimiter: Option[String] = None,
                                           quote: Option[String] = None,
                                           escape: Option[String] = None,
                                           date: Option[String] = None,
                                           quoteMode: Option[String] = None) extends TargetConfig {
  override def getType: TargetType = Targets.metricError
}

/**
 * Target configuration for metric error collection which are send to kafka topic rather than saved to HDFS.
 * @param metrics List of metric for error collection. Default is empty and errors for all statusable metrics are collected.
 * @param dumpSize Number of errors (rows where metric had failed) per metric to be written in report. Default is 100 errors per metric.
 * @param topic Kafka topic to write into
 * @param brokerId Id of Kafka broker to connect to (defined in messageBrokers section of the configuration file)
 * @param options Additional kafka options to use for producer.
 */
case class ErrorCollectionKafkaTargetConfig(topic: String,
                                            brokerId: String,
                                            options: Seq[String] = Seq.empty[String],
                                            metrics: Seq[String] = Seq.empty[String],
                                            dumpSize: Int = 100,
                                            date: Option[String] = None) extends TargetConfig {
  override def getType: TargetType = Targets.metricError
}

/**
 * HIVE target configuration
 * @param schema HIVE schema to write into
 * @param table HIVE table to write into
 * @param date output date
 * @param datePartCol date partition column name (if defined)
 */
case class HiveTargetConfig(
                             schema: String,
                             table: String,
                             date: Option[String] = None,
                             datePartCol: Option[String] = None
                           ) extends TargetConfig {
  override def getType: Targets.Value = Targets.hive
}
