package org.checkita.targets

import org.checkita.connections.BinaryAttachment
import org.checkita.connections.mail.Mail
import org.checkita.connections.mattermost.MMMessage

/**
 * Target notification message to send via one of the supported changes such as email or mattermost.
 *
 * @param body Body of the message
 * @param subject Message subject name (if applicable)
 * @param recipients Sequence of message recipients
 * @param attachments Sequence of binary attachments
 */
case class NotificationMessage(
                                body: String,
                                subject: String,
                                recipients: Seq[String],
                                attachments: Seq[BinaryAttachment]
                              ) {
  def asMail: Mail = Mail(recipients, subject, body, attachments)
  def asMM: MMMessage = MMMessage(recipients, body, attachments)
  def isEmpty: Boolean = false
}

/**
 * Empty notification message. For cases when check alert is configured but all watched checks have passed.
 */
object EmptyNotificationMessage extends NotificationMessage(
  body = "", subject = "", recipients = Seq.empty, attachments = Seq.empty
) {
  override def isEmpty: Boolean = true
}