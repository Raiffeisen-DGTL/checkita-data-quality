package org.checkita.api.routes

import cats.effect.IO
import cats.implicits._
import org.checkita.api.common.ImplicitOps._
import org.checkita.dqf.config.IO.{readAppConfig, readJobConfig}
import org.checkita.dqf.config.Parsers._
import org.checkita.dqf.config.appconf.AppConfig
import org.checkita.dqf.config.jobconf.JobConfig
import org.checkita.dqf.utils.Logging
import org.checkita.dqf.utils.ResultUtils._
import org.http4s.dsl.io._
import org.http4s.{HttpRoutes, ParseFailure, QueryParamDecoder, Response}

import scala.util.Try

object ValidationRoutes extends Logging {

  /**
   * Decoder for extra variables.
   * Decodes list of extra variables' names into a sequence with additional check 
   * if each variable name matches required regex pattern.
   */
  private implicit val extraVariablesDecoder: QueryParamDecoder[Seq[String]] =
    QueryParamDecoder[String].emap { s =>
      Try(s.split(",").toSeq).toEither.leftMap { e =>
        ParseFailure("Unable to split extra variables string into list of variables.", e.getStackTraceAsString)
      }.flatMap { vars =>
        val rgx = "^[a-zA-Z0-9_-]+$"
        if (vars.forall(_.matches(rgx))) Right(vars)
        else Left(ParseFailure(
          s"All variables must match following regex expression: '$rgx'",
          vars.filter(!_.matches(rgx)).mkString("\n")
        ))
      }
    }

  /**
   * Query param matcher for extra variables
   */
  private object ExtraVarsOptValQPM extends
    OptionalValidatingQueryParamDecoderMatcher[Seq[String]]("extra-vars")

  /**
   * Assign dummy values to provided extra variables and prepend resultant key-value pairs to 
   * configuration for further parsing.
   *
   * @param config    Config string that was sent with POST request for validation
   * @param extraVars List of extra variables' names sent with POST request.
   * @return Updated config string with extra variables being prepended.
   */
  private def prependExtraVars(config: String, extraVars: Seq[String]): String = {
    val prependStr = (extraVars.toSet + "referenceDate" + "executionDate")
      .map(v => v + ": \"" + v + "\"").mkString("", "\n", "\n")
    prependStr + config
  }

  /**
   * Validates configuration that was sent with POST request.
   *
   * @param f         Configuration validation function for config of type T.
   * @param config    Config string that was sent with POST request.
   * @param extraVars List of extra variables' names sent with POST request.
   * @tparam T Type of configuration.
   * @return Configuration validation response.
   */
  private def validateConfig[T](f: String => Result[T])
                               (config: String, extraVars: Seq[String]): IO[Response[IO]] =
    f(prependExtraVars(config, extraVars)) match {
      case Right(_) => 
        log.info("Success. Configuration is valid.")
        Ok("Configuration is valid")
      case Left(errors) => 
        log.error("Unable to validate configuration due to following errors:\n" + errors.mkString("\n"))
        NotAcceptable(errors.mkString("\n"))
    }

  /**
   * Configuration validation API routes.
   */
  def configValidationRoutes: HttpRoutes[IO] = HttpRoutes.of {
    case jobConfig @ POST -> Root / "job-config" :? ExtraVarsOptValQPM(extraVars) =>
      jobConfig.as[String].flatMap( 
        jc => extraVars.withDefault(Seq.empty).produceResponse(
          vars => validateConfig[JobConfig](readJobConfig)(jc, vars)
        )(log)
      )
    case appConfig@POST -> Root / "app-config" :? ExtraVarsOptValQPM(extraVars) =>
      appConfig.as[String].flatMap(
        ac => extraVars.withDefault(Seq.empty).produceResponse(
          vars => validateConfig[AppConfig](readAppConfig)(ac, vars)
        )(log)
      )
  }
}
