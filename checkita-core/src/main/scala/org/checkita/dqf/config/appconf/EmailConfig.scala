package org.checkita.dqf.config.appconf

import eu.timepit.refined.types.string.NonEmptyString
import org.checkita.dqf.config.RefinedTypes.{Email, Port, URI}

/**
 * Application-level configuration describing connection to SMTP server
 * @param host SMTP host
 * @param port SMTP port
 * @param address Email address to sent notification from
 * @param name Name of the sender
 * @param sslOnConnect Boolean flag indication whether to use SSL on connect
 * @param tlsEnabled Boolean flag indication whether to enable TLS
 * @param username Username for connection to SMTP server (if required)
 * @param password Password for connection to SMTP server (if required)
 */
final case class EmailConfig(
                              host: URI,
                              port: Port,
                              address: Email,
                              name: NonEmptyString,
                              sslOnConnect: Boolean = false,
                              tlsEnabled: Boolean = false,
                              username: Option[NonEmptyString],
                              password: Option[NonEmptyString],
                            )
