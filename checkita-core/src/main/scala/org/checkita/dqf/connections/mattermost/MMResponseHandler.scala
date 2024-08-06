package org.checkita.dqf.connections.mattermost

import org.apache.http.HttpResponse
import org.apache.http.client.ResponseHandler
import org.apache.http.util.EntityUtils
import org.json4s.jackson.JsonMethods
import org.json4s.{JNothing, JValue}

import scala.util.{Failure, Success, Try}

/**
 * Response handler which parses response entity into JsonValue.
 */
private class MMResponseHandler extends ResponseHandler[Either[String, JValue]] {

  /**
   * Converts response text into JsonValue.
   * @param content response text.
   * @return Either JsonValue with response content or an error message.
   */
  private def fromJson(content: String): Either[String, JValue] = {
    Try(JsonMethods.parse(content)) match {
      case Success(data) => Right(data)
      case Failure(err) => Left(s"Failed to parse response content with error: ${err.toString}")
    }
  }

  /**
   * Handles request response.
   * @param response HTTP Request Response
   * @return Either JsonValue with response content or an error message.
   */
  override def handleResponse(response: HttpResponse): Either[String, JValue] = {
    val statusCode = response.getStatusLine.getStatusCode
    val statusMsg = response.getStatusLine.getReasonPhrase
    if (statusCode >= 200 && statusCode <= 300) {
      val entity = response.getEntity
      if (entity != null) fromJson(EntityUtils.toString(entity))
      else Right(JNothing)
    } else {
      Left(
        s"Failed request with status $statusCode and following message: $statusMsg.\n" +
          s"Response text: ${EntityUtils.toString(response.getEntity)}"
      )
    }
  }
}
