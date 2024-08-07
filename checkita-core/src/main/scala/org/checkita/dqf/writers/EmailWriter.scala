package org.checkita.dqf.writers

import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Outputs.EmailOutputConfig
import org.checkita.dqf.connections.DQConnection
import org.checkita.dqf.connections.mail.Mailer
import org.checkita.dqf.targets.NotificationMessage
import org.checkita.dqf.utils.ResultUtils._

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
