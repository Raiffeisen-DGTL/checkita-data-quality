package ru.raiffeisen.checkita.config.appconf

import eu.timepit.refined.types.string.NonEmptyString
import ru.raiffeisen.checkita.config.RefinedTypes.SparkParam

/**
 * Application-level configuration
 * @param applicationName Name of Checkita Data Quality spark application
 * @param storage Defines parameters for connection to history storage.
 * @param email Defines parameters to sent email notifications
 * @param mattermost Defines parameters to sent mattermost notifications
 * @param dateTimeOptions Defines datetime representation settings
 * @param enablers Configure enablers (switchers) to turn on/off some features of DQ
 * @param defaultSparkOptions List of default Spark Configurations
 */
final case class AppConfig(
                            applicationName: Option[NonEmptyString],
                            storage: Option[StorageConfig],
                            email: Option[EmailConfig],
                            mattermost: Option[MattermostConfig],
                            streaming: StreamConfig = StreamConfig(),
                            dateTimeOptions: DateTimeConfig = DateTimeConfig(),
                            enablers: Enablers = Enablers(),
                            defaultSparkOptions: Seq[SparkParam] = Seq.empty,
                          )

