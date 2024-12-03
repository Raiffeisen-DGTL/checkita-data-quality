package org.checkita.api.routes

import cats.effect.IO
import com.typesafe.config.ConfigRenderOptions
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.io._
import org.http4s.HttpRoutes

import org.checkita.api.configGenerator.HeuristicsGenerator.heuristics
import org.checkita.dqf.utils.Logging

object HeuristicsRoutes extends Logging {

  private val validConnTypes = Set(
    "mssql",
    "oracle",
    "postgres",
    "postgresql",
    "greenplum",
    "sqlite",
    "hive",
    "mysql",
    "h2",
    "clickhouse"
  )

  private object ConnTypeQueryParamMatcher extends QueryParamDecoderMatcher[String]("conn_type") {
    override def unapply(params: Map[String, collection.Seq[String]]): Option[String] = {
      params.get("conn_type").flatMap(_.headOption).map(_.toLowerCase).filter(validConnTypes.contains)
    }
  }


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

    case _@POST -> Root / "heuristics" =>
      BadRequest(f"Invalid or missing conn_type parameter. Allowed values: ${validConnTypes.mkString(", ")}.")
  }
}
