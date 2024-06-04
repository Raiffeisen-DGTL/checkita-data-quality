package ru.raiffeisen.checkita.common

import cats.data.{Validated, ValidatedNel}
import cats.effect.IO
import cats.implicits._
import org.apache.logging.log4j.Logger
import org.http4s.dsl.io._
import org.http4s.{ParseFailure, Response}

import scala.concurrent.Future

/** Implicit conversions providing some useful extension methods */
object ImplicitOps {

  /**
   * Implicit conversion for futures containing IOs providing method to unwrap it into IO[_]
   *
   * @param value Future containing IO.
   * @tparam T Type if the value within IO.
   */
  implicit class FutureIOOps[T](value: Future[IO[T]]){
    
    def unwrap: IO[T] = IO.fromFuture(IO(value)).flatMap(identity)
  }

  /**
   * Implicit conversion for validated query parameters. 
   * Provides a method to generate responses depending on whether
   * parameters are valid or some parsing failures occured.
   *
   * @param value Validated parameter(s)
   * @tparam T Type of parameter(s)
   */
  implicit class ValParamOps[T](value: ValidatedNel[ParseFailure, T]) {
    
    def produceResponse(f: T => IO[Response[IO]])(logger: Logger): IO[Response[IO]] = value.fold(
      errs => {
        logger.error(s"Unable to parse arguments. Got following errors:\n" + errs.map(_.message).mkString_("\n"))
        BadRequest(s"Unable to parse arguments:\n" + errs.map(_.sanitized).mkString_("\n"))
      },
      param => {
        logger.info("Arguments are valid. Generating response.") 
        f(param)
      }
    )
  }

  /**
   * Implicit conversion for optional validated query parameters.
   * Provides methods to unwrap optional validated parameter or to
   * provide a default value.
   *
   * @param value Optional validated parameter(s)
   * @tparam T Type of parameter(s)
   */
  implicit class OptValParamOps[T](value: Option[ValidatedNel[ParseFailure, T]]) {
    
    def unwrap: ValidatedNel[ParseFailure, Option[T]] = value match {
      case Some(vParam) => vParam.map(Option(_))
      case None => Validated.valid(Option.empty)
    }
    
    def withDefault(default: T): ValidatedNel[ParseFailure, T] = 
      value.getOrElse(Validated.valid(default))
  }
  
}
