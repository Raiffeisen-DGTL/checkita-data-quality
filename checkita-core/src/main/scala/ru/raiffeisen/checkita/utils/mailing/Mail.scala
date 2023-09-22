package ru.raiffeisen.checkita.utils.mailing

import ru.raiffeisen.checkita.utils.BinaryAttachment
import org.apache.commons.mail.{DefaultAuthenticator, Email, EmailAttachment, EmailConstants, HtmlEmail, MultiPartEmail, SimpleEmail}

import scala.language.implicitConversions

object Mail {
  implicit def stringToSeq(single: String): Seq[String] = Seq(single)
  implicit def liftToOption[T](t: T): Option[T] = Some(t)

  def a(mail: Mail)(implicit mailer: MailerConfiguration) {

    val commonsMail: Email = mail.attachment.foldLeft(new HtmlEmail()){ (email, attachment) =>
      val byteArrayDataSource = new javax.mail.util.ByteArrayDataSource(attachment.content,"text/csv")
      val fileName = attachment.name
      email.attach(byteArrayDataSource, fileName, fileName, EmailAttachment.ATTACHMENT).asInstanceOf[HtmlEmail]
    }.setMsg(mail.message)

    commonsMail.setHostName(mailer.hostName)
    commonsMail.setSmtpPort(mailer.smtpPortSSL)
    commonsMail.setStartTLSEnabled(mailer.tlsEnabled)
    commonsMail.setStartTLSRequired(mailer.tlsEnabled)
    commonsMail.setSSLCheckServerIdentity(mailer.tlsEnabled)
    commonsMail.setCharset(EmailConstants.UTF_8)
    if (mailer.username != "" && mailer.password != "")
      commonsMail.setAuthenticator(new DefaultAuthenticator(mailer.username, mailer.password))

    commonsMail.setSSLOnConnect(mailer.sslOnConnect)

    // Can't add these via fluent API because it produces exceptions
    mail.to foreach commonsMail.addTo
    mail.cc foreach commonsMail.addCc
    mail.bcc foreach commonsMail.addBcc

    commonsMail
      .setFrom(mail.from._1, mail.from._2)
      .setSubject(mail.subject)
      .send()
  }
}

case class Mail(
                 from: (String, String), // (email -> name)
                 to: Seq[String],
                 cc: Seq[String] = Seq.empty,
                 bcc: Seq[String] = Seq.empty,
                 subject: String,
                 message: String,
                 attachment: Seq[BinaryAttachment] = Seq.empty[BinaryAttachment]
               )
