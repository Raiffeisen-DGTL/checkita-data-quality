package org.checkita.dqf.connections.mattermost

import org.apache.http.client.methods.{HttpUriRequest, RequestBuilder}
import org.apache.http.entity.{InputStreamEntity, StringEntity}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.{HttpEntity, HttpException, HttpHeaders}
import org.json4s.JValue
import org.json4s.jackson.Serialization
import org.checkita.dqf.config.appconf.MattermostConfig
import org.checkita.dqf.connections.BinaryAttachment
import org.checkita.dqf.utils.Common.jsonFormats
import org.checkita.dqf.utils.ResultUtils._

import java.io.ByteArrayInputStream
import scala.util.{Failure, Success, Try}

/**
 * Mattermost API manager. Used to send message to mattermost after DQ Job completion.
 * @param conf configuration (host and token) used to connect to Mattermost API.
 */
case class MMManager(conf: MattermostConfig) {

  private val host = conf.host.value
  private val token = conf.token.value
  private val client = HttpClientBuilder.create().build()

  private val responseHandler = new MMResponseHandler

  /**
   * This field contains user ID which is used to connect to Mattermost API and from which the messages will be sent.
   */
  private val fromUserId: String = executeGetRequest(s"$host/${MMEndPoint.Users.entryName}/me") match {
    case Right(data) =>
      data.extract[Map[String, Any]]
        .get("id").map(_.toString).getOrElse(throw new HttpException("Unable to fetch bot user id"))
    case Left(err) => throw new HttpException(err)
  }

  /**
   * This field contains a list of channels that the user connected to the API is a member of.
   */
  private val channels: Map[String, String] =
    executeGetRequest(s"$host/${MMEndPoint.Users.entryName}/$fromUserId/channels") match {
      case Right(data) =>
        data.extract[Seq[Map[String, Any]]].flatMap{ ch =>
          for (name <- ch.get("name"); id <- ch.get("id")) yield name.toString -> id.toString
        }.toMap
      case Left(err) => throw new HttpException(err)
    }

  /**
   * Executes request and returns its response.
   * @param request request to execute.
   * @return Either parsed request response or an error message.
   */
  private def executeRequest(request: HttpUriRequest): Either[String, JValue] =
    Try(client.execute(request, responseHandler)) match {
      case Success(responseData) => responseData
      case Failure(err) => Left(s"Failed to execute request with error message: ${err.toString}")
    }

  /**
   * Executes simple GET request.
   * @param endPoint API end point to send GET request to.
   * @return Either parsed request response or an error message.
   */
  private def executeGetRequest(endPoint: String): Either[String, JValue] = {
    val request = RequestBuilder.get()
      .setUri(endPoint)
      .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
      .setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $token")
      .build()

    executeRequest(request)
  }

  /**
   * Executes POST request with optional query parameters and HTTP entities.
   * @param endPoint API end point to send POST request to.
   * @param entities HTTP entities to add to POST request.
   * @param parameters Query parameters to add to POST request.
   * @return Either parsed request response or an error message.
   */
  private def executePostRequest(endPoint: String,
                                 entities: Seq[HttpEntity],
                                 parameters: Map[String, String] = Map.empty): Either[String, JValue] = {
    val request = RequestBuilder.post()
      .setUri(endPoint)
      .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
      .setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $token")

    parameters.foreach(kv => request.addParameter(kv._1, kv._2))
    entities.foreach(request.setEntity)
    executeRequest(request.build())
  }

  /**
   * Get user IDs by their usernames.
   * @param usernames sequence of usernames.
   * @return sequence of user IDs.
   */
  private def getUserIds(usernames: Seq[String]): Seq[String] = {
    val payload = new StringEntity(usernames.mkString("[\"", "\", \"", "\"]"))
    val endPoint = s"$host/${MMEndPoint.Users.entryName}/usernames"
    executePostRequest(endPoint, Seq(payload)) match {
      case Right(data) =>
        data.extract[Seq[Map[String, Any]]].flatMap(_.get("id").map(_.toString))
      case Left(err) => throw new HttpException(err)
    }
  }

  /**
   * Gets direct message channel ID between the user connected to the API and the given user.
   * @param userId ID of the user for direct messaging.
   * @return direct message channel ID.
   */
  private def getDirectChannelId(userId: String): Option[String] = {
    val payload = new StringEntity(Seq(fromUserId, userId).mkString("[\"", "\", \"", "\"]"))
    val endPoint = s"$host/${MMEndPoint.Channels.entryName}/direct"
    executePostRequest(endPoint, Seq(payload)) match {
      case Right(data) =>
        data.extract[Map[String, Any]].get("id").map(_.toString)
      case Left(err) => throw new HttpException(err)
    }
  }

  /**
   * Uploads file in form of byte array to the channel with given ID.
   * @param channelId ID of channel to upload file to.
   * @param fileName Name of the file
   * @param fileContent File content in form of an array of bytes
   * @return sequence with file IDs.
   */
  private def uploadFile(channelId: String, fileName: String, fileContent: Array[Byte]): Seq[String] = {
    val parameters = Map(
      "channel_id" -> channelId,
      "filename" -> fileName
    )

    val fileEntity = new InputStreamEntity(new ByteArrayInputStream(fileContent), -1)
    fileEntity.setContentType("binary/octet-stream")

    val endPoint = s"$host/${MMEndPoint.Files.entryName}"

    executePostRequest(endPoint, Seq(fileEntity), parameters) match {
      case Right(data) =>
        val fileInfo = data.extract[Map[String, Any]].get("file_infos")
          .map(_.asInstanceOf[Seq[Map[String, Any]]]).getOrElse(Seq.empty)

        fileInfo.flatMap(_.get("id").map(_.toString))
      case Left(err) => throw new HttpException(err)
    }
  }

  /**
   * Posts message to given channel ID. Optionally attaches files to message.
   * @param channelId Channel ID to post message to.
   * @param message Message text (markdown)
   * @param attachments Sequence of attachments for the message.
   */
  private def postMessage(channelId: String,
                          message: String,
                          attachments: Seq[BinaryAttachment] = Seq.empty): Unit = {
    val payload = if (attachments.isEmpty) {
      new StringEntity(Serialization.write(Map(
        "channel_id" -> channelId,
        "message" -> message
      )))
    } else {
      val fileIds = attachments.flatMap { attachment =>
        uploadFile(channelId, attachment.name, attachment.content)
      }
      new StringEntity(Serialization.write(Map(
        "channel_id" -> channelId,
        "message" -> message,
        "file_ids" -> fileIds
      )))
    }

    val endPoint = s"$host/${MMEndPoint.Posts.entryName}"
    executePostRequest(endPoint, Seq(payload)) match {
      case Right(_) => ()
      case Left(err) => throw new HttpException(err)
    }
  }

  /**
   * Sends message to the list of the recipients: to both channels and direct messages.
   * @param message Message to sent.
   */
  def send(message: MMMessage): Result[String] = Try {
    val toChannels = message.recipients.filter(_.startsWith("#")).map(_.tail)
    val toUsers = message.recipients.filter(_.startsWith("@")).map(_.tail)

    if (toChannels.exists(!channels.contains(_))) throw new IllegalArgumentException(
      "User which is used to send message to mattermost channel is not a member of this channel."
    )

    val allChannels: Seq[String] = toChannels.flatMap(channels.get) ++
      getUserIds(toUsers).flatMap(getDirectChannelId)

    allChannels.foreach(postMessage(_, message.body, message.attachments))
    "Success"
  }.toResult(preMsg = "Unable to send mattermost message due to following error: ")
}
