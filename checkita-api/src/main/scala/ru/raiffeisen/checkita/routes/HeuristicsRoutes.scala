package ru.raiffeisen.checkita.routes

import cats.effect.IO
import com.typesafe.config.ConfigRenderOptions
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.io._
import org.http4s.HttpRoutes

import ru.raiffeisen.checkita.configGenerator.HeuristicsGenerator.heuristics
import ru.raiffeisen.checkita.utils.Logging

object HeuristicsRoutes extends Logging {
  private object ConnTypeQueryParamMatcher extends QueryParamDecoderMatcher[String]("conn_type")


  /** API route for generate Job Config.
    *
    * @return
    *   HTTP Routes
    */
  def dqHeuristicsRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @POST -> Root / "heuristics" :? ConnTypeQueryParamMatcher(connType) =>
      for {
        ddl <- req.as[String]
        response <- heuristics(ddl, connType) match {
          case Right(config) =>
            Ok(config.root().render(ConfigRenderOptions.defaults().setComments(false).setOriginComments(false)))
          case Left(err) => NotAcceptable(err.mkString("\n"))
        }
      } yield response
  }
}
