package ru.raiffeisen.checkita.routes

import cats.data.Validated._
import cats.data.{Validated, ValidatedNel}
import cats.effect.IO
import cats.implicits._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.io._
import org.http4s.{HttpRoutes, ParseFailure, QueryParamDecoder, Response}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.RefinedTypes
import ru.raiffeisen.checkita.dbmanager.APIJdbcStorageManager
import ru.raiffeisen.checkita.utils.EnrichedDT
import ru.raiffeisen.checkita.utils.ResultUtils._

import java.time.ZoneId
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

class StorageRoutes(dbManager: APIJdbcStorageManager, settings: AppSettings) {

  private val dateFmt: RefinedTypes.DateFormat = settings.referenceDateTime.dateFormat
  private val dateTz: ZoneId = settings.referenceDateTime.timeZone
  private implicit val ec: ExecutionContextExecutor = dbManager.ec

  private def unwrapFuture(f: Future[IO[Response[IO]]]): IO[Response[IO]] = IO.fromFuture(IO(f)).flatMap(io => io)

  private implicit val enrichedDtDecoder: QueryParamDecoder[EnrichedDT] =
    QueryParamDecoder[String].emap { s =>
      Try(EnrichedDT(dateFmt, dateTz, Some(s))).toEither.leftMap { e =>
        ParseFailure(
          s"Cannot parse datetime argument with following error:\n ${e.getMessage}",
          e.getStackTrace.mkString("\n")
        )
      }
    }

  def validatedParamToResult[T](validated: ValidatedNel[ParseFailure, T]): Result[T] = validated match {
    case Valid(v) => liftToResult(v)
    case Invalid(e) => Left(e).toResult(_.map(_.sanitized).toList.toVector)
  }


  private object OptStartDtQPM extends OptionalValidatingQueryParamDecoderMatcher[EnrichedDT]("start-dt")

  private object OptEndDtQPM extends OptionalValidatingQueryParamDecoderMatcher[EnrichedDT]("end-dt")

  private object OptJobFilterQPM extends OptionalValidatingQueryParamDecoderMatcher[String]("job-filter")


  def getNumberOfJobsResponse(startDtArg: Option[ValidatedNel[ParseFailure, EnrichedDT]],
                              endDtArg: Option[ValidatedNel[ParseFailure, EnrichedDT]],
                              jobFilterArg: Option[ValidatedNel[ParseFailure, String]]): IO[Response[IO]] = {
    val startDt = validatedParamToResult(
      startDtArg.getOrElse(Validated.valid(EnrichedDT(dateFmt, dateTz).withOffset(Duration("7d"))))
    )
    val endDt = validatedParamToResult(
      endDtArg.getOrElse(Validated.valid(EnrichedDT(dateFmt, dateTz)))
    )
    val jobFilter = validatedParamToResult(
      jobFilterArg.map(v => v.map(Some(_))).getOrElse(Validated.valid(None))
    )
    startDt.combineT2(endDt, jobFilter)(dbManager.getNumberOfJobs) match {
      case Right(res) => unwrapFuture(res.map(data => Ok(data.toString)).recover {
        case e => InternalServerError(e.getMessage)
      })
      case Left(errs) => BadRequest("Unable to parse arguments:\n" + errs.mkString("\n"))
    }
  }

  def dqStorageRoutes: HttpRoutes[IO] = HttpRoutes.of {
    case GET -> Root / "num-jobs" :? OptStartDtQPM(startDtOpt)
                                  +& OptEndDtQPM(endDtOpt)
                                  +& OptJobFilterQPM(jobFilterOpt) =>
      getNumberOfJobsResponse(startDtOpt, endDtOpt, jobFilterOpt)
  }
}
object StorageRoutes {
  def apply(dbManager: APIJdbcStorageManager, settings: AppSettings): StorageRoutes =
    new StorageRoutes(dbManager, settings)
}