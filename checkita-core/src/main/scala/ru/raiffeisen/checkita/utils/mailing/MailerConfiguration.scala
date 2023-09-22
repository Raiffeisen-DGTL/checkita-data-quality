package ru.raiffeisen.checkita.utils.mailing

import com.typesafe.config.Config
import scala.util.Try

case class MailerConfiguration(
                                address: String,
                                name: String,
                                summarySubjectTemplate: String,
                                checkAlertSubjectTemplate: String,
                                hostName: String,
                                username: String,
                                password: String,
                                smtpPortSSL: Int,
                                sslOnConnect: Boolean,
                                tlsEnabled: Boolean
                              ) {
  def this(config: Config) = {
    this(
      config.getString("address"),
      Try(config.getString("name")).getOrElse("CIBAA DataQuality"),
      Try(config.getString("summarySubjectTemplate"))
        .getOrElse("Data Quality summary for JobID: {{ jobId }}"),
      Try(config.getString("checkAlertSubjectTemplate"))
        .getOrElse("Data Quality failed check alert for JobID: {{ jobId }}"),
      config.getString("hostname"),
      Try(config.getString("username")).getOrElse(""),
      Try(config.getString("password")).getOrElse(""),
      Try(config.getInt("smtpPort")).getOrElse(465),
      Try(config.getBoolean("sslOnConnect")).getOrElse(true),
      Try(config.getBoolean("tlsEnabled")).getOrElse(true)
    )
  }
}
