package ru.raiffeisen.checkita.connections.mail

import ru.raiffeisen.checkita.connections.BinaryAttachment

/**
 * Mail definition
 * @note For now, `cc` and `bcc` are always empty.
 * @param to List of emails to send to.
 * @param subject Mail subject
 * @param message Mail body
 * @param attachments Mail attachments
 */
case class Mail(to: Seq[String],
                subject: String,
                message: String,
                attachments: Seq[BinaryAttachment]) {
  val cc: Seq[String] = Seq.empty
  val bcc: Seq[String] = Seq.empty
}
