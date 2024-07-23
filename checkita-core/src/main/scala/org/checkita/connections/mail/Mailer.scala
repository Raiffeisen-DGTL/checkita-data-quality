package org.checkita.connections.mail

import org.checkita.config.appconf.EmailConfig
import org.checkita.utils.ResultUtils._
import org.apache.commons.mail._
import org.checkita.connections.BinaryAttachment

import scala.util.Try

/**
 * Mailer
 * @param config Mailer configuration
 */
case class Mailer(config: EmailConfig) {

  /**
   * Sends mail.
   * @param mail Definition of email to be sent.
   * @return Message ID in case of success or a list of errors.
   */
  def send(mail: Mail): Result[String] = Try{
    val initMail: HtmlEmail = new HtmlEmail()

    // in-place setters:
    initMail.setCharset(EmailConstants.UTF_8)
    initMail.setHostName(config.host.value)
    initMail.setSmtpPort(config.port.value)
    for {
      user <- config.username.map(_.value)
      pass <- config.password.map(_.value)
    } yield initMail.setAuthenticator(new DefaultAuthenticator(user, pass))
    
    // function to add attachment:
    val addAttachment = (email: HtmlEmail, attachment: BinaryAttachment) => {
      val byteArrayDataSource = new javax.mail.util.ByteArrayDataSource(
        attachment.content, "text/tab-separated-values"
      )
      val fileName = attachment.name
      email.attach(byteArrayDataSource, fileName, fileName, EmailAttachment.ATTACHMENT).asInstanceOf[HtmlEmail]
    }
    
    // adding attachments if any:
    val mailWithAttachments = mail.attachments.foldLeft(initMail)(
      (email, attachment) => addAttachment(email, attachment)
    )
    
    // adding recipients:
    val mailWithTo = Seq(
      (mail.to, (m: HtmlEmail, s: String) => m.addTo(s).asInstanceOf[HtmlEmail]),
      (mail.cc, (m: HtmlEmail, s: String) => m.addCc(s).asInstanceOf[HtmlEmail]),
      (mail.bcc, (m: HtmlEmail, s: String) => m.addBcc(s).asInstanceOf[HtmlEmail]),
    ).flatMap {
      case (sq, f) => sq.map(s => (s, f))
    }.foldLeft(mailWithAttachments)((email, add) => add._2(email, add._1))

    // set remaining parameters:
    val finalMail: Email = mailWithTo
      .setMsg(mail.message)
      .setSubject(mail.subject)
      .setFrom(config.address.value, config.name.value)
      .setStartTLSEnabled(config.tlsEnabled)
      .setStartTLSRequired(config.tlsEnabled)
      .setSSLCheckServerIdentity(config.tlsEnabled)
      .setSSLOnConnect(config.sslOnConnect)

    finalMail.send()
  }.toResult(preMsg = "Unable to send email due to following error: ")
}
