package ru.raiffeisen.checkita.routes

import cats.effect.IO
import cats.implicits._
import org.http4s.dsl.io._
import org.http4s.{HttpRoutes, ParseFailure, QueryParamDecoder, Response}
import ru.raiffeisen.checkita.config.IO.{readAppConfig, readJobConfig}
import ru.raiffeisen.checkita.config.Parsers._
import ru.raiffeisen.checkita.config.appconf.AppConfig
import ru.raiffeisen.checkita.config.jobconf.JobConfig
import ru.raiffeisen.checkita.utils.ResultUtils.Result

import scala.util.Try

object ValidationRoutes {

  private implicit val extraVariablesDecoder: QueryParamDecoder[Seq[String]] =
    QueryParamDecoder[String].emap { s =>
      Try(s.split(",").toSeq).toEither.leftMap { e =>
        ParseFailure("Unable to split extra variables string into list of variables.", e.getMessage)
      }.flatMap { vars =>
        val rgx = "^[a-zA-Z0-9_-]+$"
        if (vars.forall(_.matches(rgx))) Right(vars)
        else Left(ParseFailure(
          s"All variables must match following regex expression: '$rgx'",
          vars.filter(!_.matches(rgx)).mkString("\n")
        ))
      }
    }

  private object OptionalExtraVarsQueryParamMatcher extends
    OptionalValidatingQueryParamDecoderMatcher[Seq[String]]("extra-vars")

  private def prependExtraVars(config: String, extraVars: Seq[String]): String = {
    val prependStr = (extraVars.toSet + "referenceDate" + "executionDate")
      .map(v => v + ": \"" + v + "\"").mkString("", "\n", "\n")
    prependStr + config
  }

  private def validateConfig[T](f: String => Result[T])
                               (config: String, extraVars: Seq[String]): IO[Response[IO]] =
    f(prependExtraVars(config, extraVars)) match {
      case Right(_) => Ok("Configuration is valid")
      case Left(errors) => NotAcceptable(errors.mkString("\n"))
    }


  def configValidationRoutes: HttpRoutes[IO] = HttpRoutes.of {
    case jobConfig@POST -> Root / "job-config" :? OptionalExtraVarsQueryParamMatcher(extraVars) =>
      jobConfig.as[String].flatMap { jc =>
        extraVars match {
          case Some(vars) => vars.fold(
            errs => BadRequest(
              s"Unable to parse argument 'extra-vars':\n" + errs.map(_.sanitized).mkString_("\n")
            ),
            v => validateConfig[JobConfig](readJobConfig)(jc, v)
          )
          case None => validateConfig[JobConfig](readJobConfig)(jc, Seq.empty)
        }
      }
    case appConfig@POST -> Root / "app-config" :? OptionalExtraVarsQueryParamMatcher(extraVars) =>
      appConfig.as[String].flatMap { ac =>
        extraVars match {
          case Some(vars) => vars.fold(
            errs => BadRequest(
              s"Unable to parse argument 'extra-vars':\n" + errs.map(_.sanitized).mkString_("\n")
            ),
            v => validateConfig[AppConfig](readAppConfig)(ac, v)
          )
          case None => validateConfig[AppConfig](readAppConfig)(ac, Seq.empty)
        }
      }
  }
}
