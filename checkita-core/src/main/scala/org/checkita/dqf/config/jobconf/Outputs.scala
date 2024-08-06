package org.checkita.dqf.config.jobconf

import eu.timepit.refined.auto._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.types.string.NonEmptyString
import org.checkita.dqf.config.Enums.TemplateFormat
import org.checkita.dqf.config.RefinedTypes.{Email, ID, MMRecipient, SparkParam, URI}
import org.checkita.dqf.config.jobconf.Files._


object Outputs {
  
  /** Base class for all outputs configurations */
  trait OutputConfig
  
  /**
   * Base class for file outputs.
   */
  sealed trait FileOutputConfig extends OutputConfig with FileConfig

  /**
   * Base class for `save to a file` configuration
   * @note Idea here is that instead of building multiple subclasses for each supported output file format,
   *       configuration will contain `save` parameter with FileOutputConfig of specific `kind` (format).
   */
  trait SaveToFileConfig extends OutputConfig {
    val save: FileOutputConfig
  }
  
  /**
   * Delimited file output configuration
   *
   * @param path      Path to file
   * @param delimiter Column delimiter (default: ,)
   * @param quote     Quotation symbol (default: ")
   * @param escape    Escape symbol (default: \)
   * @param header    Boolean flag indicating whether schema should be read from file header (default: false)
   */
  final case class DelimitedFileOutputConfig(
                                              path: URI,
                                              delimiter: NonEmptyString = ",",
                                              quote: NonEmptyString = "\"",
                                              escape: NonEmptyString = "\\",
                                              header: Boolean = false
                                            ) extends FileOutputConfig with DelimitedFileConfig {
    // Schema is not required as input parameter as it is enforced on write.
    val schema: Option[ID] = None
  }

  /**
   * Avro file output configuration
   *
   * @param path Path to file
   */
  final case class AvroFileOutputConfig(
                                         path: URI
                                       ) extends FileOutputConfig with AvroFileConfig {
    val schema: Option[ID] = None
  }

  /**
   * Orc file output configuration
   *
   * @param path Path to file
   */
  final case class OrcFileOutputConfig(
                                        path: URI
                                      ) extends FileOutputConfig with OrcFileConfig {
    val schema: Option[ID] = None
  }

  /**
   * Parquet file output configuration
   *
   * @param path Path to file
   */
  final case class ParquetFileOutputConfig(
                                            path: URI
                                          ) extends FileOutputConfig with ParquetFileConfig {
    val schema: Option[ID] = None
  }


  /**
   * Base trait for targets that are written to Hive table.
   * Configuration for such targets must contain Hive schema
   * and Hive table in which target data will be inserted.
   */
  trait HiveOutputConfig extends OutputConfig {
    val schema: NonEmptyString
    val table: NonEmptyString
  }

  /**
   * Base trait for targets that are sent to Kafka topic.
   * Configuration for such targets must contain reference
   * to a connection ID (must be Kafka connection) and
   * topic name to write target data into.
   */
  trait KafkaOutputConfig extends OutputConfig {
    val connection: NonEmptyString
    val topic: NonEmptyString
    val options: Seq[SparkParam]
  }

  /**
   * Base trait for target that are send as notifications to end users.
   * Configuration for such targets must contain a list of recipients to receive that notification
   * In addition an optional message template can be provided in form of either an explicit string
   * or an URI to template file location.
   */
  trait NotificationOutputConfig extends OutputConfig {
    // additional validation will be imposed on the list of recipients depending on output channel.
    val recipientsList: Seq[String]
    val template: Option[NonEmptyString]
    val templateFile: Option[URI]
    val templateFormat: TemplateFormat
    val subjectTemplate: Option[NonEmptyString]
  }
  
  /**
   * Base trait for targets that are sent via email.
   * Configuration for such targets must contain non-empty sequence of recipients' emails.
   * An optional message template can be provided in form of either an explicit string
   * or an URI to template file location.
   * In addition, email subject can be customized by providing optional subject template.
   */
  trait EmailOutputConfig extends NotificationOutputConfig {
    val recipients: Seq[Email] Refined NonEmpty
    val subjectTemplate: Option[NonEmptyString]
    val template: Option[NonEmptyString]
    val templateFile: Option[URI]

    val recipientsList: Seq[String] = recipients.value.map(_.value)
    val templateFormat: TemplateFormat = TemplateFormat.Html
  }

  /**
   * Base trait for targets that are sent as messages to mattermost
   * (to a channels or to users' as direct messages).
   * Configuration for such targets must contain non-empty
   * sequence of recipient: either a channel names prefixed with '#' symbol
   * or usernames prefixed with '@' symbols.
   * In addition an optional message template can be provided in form of either an explicit string
   * or an URI to template file location.
   * @note Mattermost message do not have subjects, therefore, subjectTemplate is set to None
   */
  trait MattermostOutputConfig extends NotificationOutputConfig {
    val recipients: Seq[MMRecipient] Refined NonEmpty
    val template: Option[NonEmptyString]
    val templateFile: Option[URI]
    val subjectTemplate: Option[NonEmptyString] = None
    val recipientsList: Seq[String] = recipients.value.map(_.value)
    val templateFormat: TemplateFormat = TemplateFormat.Markdown
  }
}
