package ru.raiffeisen.checkita.writers

import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Outputs.EmailOutputConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.connections.mail.Mailer
import ru.raiffeisen.checkita.targets.NotificationMessage
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.util.Try

trait EmailWriter[T <: EmailOutputConfig] extends OutputWriter[NotificationMessage, T] {

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
      settings.emailConfig.map(cfg => Mailer(cfg)).getOrElse(throw new IllegalArgumentException(
      "Failed to construct Mailer: either application settings do not contain email configuration " +
        "or provided configuration is invalid."
      ))
    else throw new UnsupportedOperationException(
      "FORBIDDEN: Can't sent email notification since sending notifications is not allowed. " +
        "In order to allow sending notifications set `allowNotifications` to true in application settings."
    )
  }.toResult().flatMap(mailer => 
    if (result.isEmpty) liftToResult("Nothing to send.") else mailer.send(result.asMail)
  ).mapLeft(errs =>
    (s"Unable to send '$targetType' notification by email due to following error: \n" + errs.head) +: errs.tail
  )
}
