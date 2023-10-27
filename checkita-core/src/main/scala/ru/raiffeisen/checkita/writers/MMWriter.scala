package ru.raiffeisen.checkita.writers

import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Outputs.MattermostOutputConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.connections.mattermost.MMManager
import ru.raiffeisen.checkita.targets.NotificationMessage
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.util.Try

trait MMWriter[T <: MattermostOutputConfig] extends OutputWriter[NotificationMessage, T] {

  /**
   * Writes result to required output channel given the output configuration.
   *
   * @param result Result to be written
   * @param target Output configuration
   * @return "Success" string in case of successful write operation or a list of errors.
   */
  def write(target: T,
            result: NotificationMessage)(implicit jobId: String,
                                         settings: AppSettings,
                                         spark: SparkSession,
                                         connections: Map[String, DQConnection]): Result[String] = Try {
    if (settings.allowNotifications)
      settings.mattermostConfig.getOrElse(throw new IllegalArgumentException(
        "Failed to construct Mattermost manager: application settings do not contain mattermost configuration."
      ))
    else throw new UnsupportedOperationException(
      "FORBIDDEN: Can't sent mattermost notification since sending notifications is not allowed. " +
        "In order to allow sending notifications set `allowNotifications` to true in application settings."
    )
  }.toResult().flatMap(cfg => Try(MMManager(cfg)).toResult(
    preMsg = "Failed to establish connection to Mattermost API due to following error: "
  )).flatMap(mm =>
    if (result.isEmpty) liftToResult("Nothing to send.") else mm.send(result.asMM)
  ).mapLeft(errs => 
    (s"Unable to send '$targetType' notification to Mattermost due to following error: \n" + errs.head) +: errs.tail
  )
}
