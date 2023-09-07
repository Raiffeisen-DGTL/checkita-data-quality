package ru.raiffeisen.checkita.utils.mailing

import ru.raiffeisen.checkita.utils.BinaryAttachment
import org.apache.commons.mail.{
  DefaultAuthenticator, Email, EmailAttachment, HtmlEmail, MultiPartEmail, SimpleEmail
}

import scala.language.implicitConversions

object Mail {
  implicit def stringToSeq(single: String): Seq[String] = Seq(single)
  implicit def liftToOption[T](t: T): Option[T] = Some(t)

  sealed abstract class MailType
  case object Plain extends MailType
  case object Rich extends MailType
  case object MultiPart extends MailType

  def a(mail: Mail)(implicit mailer: MailerConfiguration) {
    

    val format =
      if (mail.attachment.nonEmpty) MultiPart
      else if (mail.richMessage.isDefined) Rich
      else Plain

    val commonsMail: Email = format match {
      case Plain => new SimpleEmail().setMsg(mail.message)
      case Rich =>
        new HtmlEmail()
          .setHtmlMsg(mail.richMessage.get)
          .setTextMsg(mail.message)
      case MultiPart =>
        mail.attachment.foldLeft(new MultiPartEmail()){ (email, attachment) =>
          val byteArrayDataSource = new javax.mail.util.ByteArrayDataSource(attachment.content,"text/csv")
          val fileName = attachment.name
          email.attach(byteArrayDataSource, fileName, fileName, EmailAttachment.ATTACHMENT)
        }.setMsg(mail.message)
    }

    commonsMail.setHostName(mailer.hostName)
    commonsMail.setSmtpPort(mailer.smtpPortSSL)
    commonsMail.setStartTLSEnabled(mailer.tlsEnabled)
    commonsMail.setStartTLSRequired(mailer.tlsEnabled)
    commonsMail.setSSLCheckServerIdentity(mailer.tlsEnabled)
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
                 richMessage: Option[String] = None,
                 attachment: Seq[BinaryAttachment] = Seq.empty[BinaryAttachment]
               )
