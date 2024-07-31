package ru.raiffeisen.checkita.routes

import cats.effect.IO
import io.circe.syntax._

import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.io._
import org.http4s.HttpRoutes

import ru.raiffeisen.checkita.configGenerator.HeuristicsGenerator.heuristics
import ru.raiffeisen.checkita.utils.Logging

object HeuristicsRoutes extends Logging {
  private object DdlQueryParamMatcher  extends QueryParamDecoderMatcher[String]("ddl")
  private object ConnTypeQueryParamMatcher extends QueryParamDecoderMatcher[String]("conn_type")


  /** API route for generate Job Config.
    *
    * @return
    *   HTTP Routes
    */
  def dqHeuristicsRoutes: HttpRoutes[IO] = HttpRoutes.of {
    case POST -> Root / "heuristics" :? DdlQueryParamMatcher(ddl) +& ConnTypeQueryParamMatcher(connType) =>
      heuristics(ddl, connType) match {
        case Right(config) => Ok(config.asJson)
        case Left(err) => NotAcceptable(err.mkString("\n"))
      }
  }
}
