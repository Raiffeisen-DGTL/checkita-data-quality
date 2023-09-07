package ru.raiffeisen.checkita.utils.io.mattermost

import com.typesafe.config.Config
import ru.raiffeisen.checkita.utils.BinaryAttachment


object MMModels {

  /**
   * Configuration class to define parameters for connection to Mattermost API
   * @param host Mattermost host
   * @param token User access token (preferably BOT access token)
   */
  final case class MMConfig(host: String, token: String)

  object MMConfig {
    /**
     * Factory method to create MMConfig from application settings config.
     * @param conf TypeSafe Config object with mattermost configurations
     * @return mattermost configuration
     */
    def apply(conf: Config): MMConfig = MMConfig(
      conf.getString("host"),
      conf.getString("token")
    )
  }

  /**
   * Defines mattermost message.
   * @param recipients Sequence of recipients to send message to (both channels and users).
   * @param body Message text to be sent.
   * @param attachments Sequence of file attachments for the message (in form of InputStreams)
   */
  final case class MMMessage(recipients: Seq[String],
                             body: String,
                             attachments: Seq[BinaryAttachment] = Seq.empty)

  /**
   * List of Mattermost API end points to interact with while sending messages.
   */
  object EndPoints extends Enumeration {
    type EndPoint = String
    val Users = "api/v4/users"
    val Files = "api/v4/files"
    val Posts = "api/v4/posts"
    val Channels = "api/v4/channels"
  }
}
