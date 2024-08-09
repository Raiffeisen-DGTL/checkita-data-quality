package org.checkita.dqf.connections.mattermost

import org.checkita.dqf.connections.BinaryAttachment


/**
 * Defines mattermost message.
 * @param recipients Sequence of recipients to send message to (both channels and users).
 * @param body Message text to be sent.
 * @param attachments Sequence of file attachments for the message (in form of InputStreams)
 */
final case class MMMessage(recipients: Seq[String],
                           body: String,
                           attachments: Seq[BinaryAttachment])