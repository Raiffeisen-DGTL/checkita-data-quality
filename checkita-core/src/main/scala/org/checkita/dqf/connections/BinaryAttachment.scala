package org.checkita.dqf.connections

/**
 * Attachment to notifications in form of byte array
 *
 * @param name    File name to be set upon uploading
 * @param content File content in form of byte array
 */
case class BinaryAttachment(
                             name: String,
                             content: Array[Byte]
                           )
