package ru.raiffeisen.checkita.routes

import cats.data.ValidatedNel
import cats.effect.IO
import cats.implicits._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.io._
import org.http4s.{HttpRoutes, ParseFailure, QueryParamDecoder, Response}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.common.Helpers._
import ru.raiffeisen.checkita.config.RefinedTypes
import ru.raiffeisen.checkita.dbmanager.APIJdbcStorageManager
import ru.raiffeisen.checkita.utils.{EnrichedDT, Logging}
import ru.raiffeisen.checkita.utils.ResultUtils._
import io.circe.generic.auto._
import io.circe.syntax._

import java.time.ZoneId
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Try

class StorageRoutes(dbManager: APIJdbcStorageManager, settings: AppSettings) extends Logging {

  private val dateFmt: RefinedTypes.DateFormat = settings.referenceDateTime.dateFormat
  private val dateTz: ZoneId = settings.referenceDateTime.timeZone
  private implicit val ec: ExecutionContextExecutor = dbManager.ec
  
  private implicit val enrichedDtDecoder: QueryParamDecoder[EnrichedDT] =
    QueryParamDecoder[String].emap { s =>
      Try(EnrichedDT(dateFmt, dateTz, Some(s))).toEither.leftMap { e =>
        ParseFailure(
          s"Cannot parse datetime argument with following error:\n ${e.getMessage}",
          e.getStackTraceAsString
        )
      }
    }
  
  private object DtOptValQPM extends OptionalValidatingQueryParamDecoderMatcher[EnrichedDT]("dt")
  private object StartDtOptValQPM extends OptionalValidatingQueryParamDecoderMatcher[EnrichedDT]("start-dt")
  private object EndDtOptValQPM extends OptionalValidatingQueryParamDecoderMatcher[EnrichedDT]("end-dt")
  private object JobFilterOptValQPM extends OptionalValidatingQueryParamDecoderMatcher[String]("job-filter")


  def getNumberOfJobsResponse(startDtArg: Option[ValidatedNel[ParseFailure, EnrichedDT]],
                              endDtArg: Option[ValidatedNel[ParseFailure, EnrichedDT]],
                              jobFilterArg: Option[ValidatedNel[ParseFailure, String]]): IO[Response[IO]] = {
    val startDt = startDtArg.withDefault(EnrichedDT(dateFmt, dateTz).withOffset(Duration("7d")))
    val endDt = endDtArg.withDefault(EnrichedDT(dateFmt, dateTz))
    val jobFilter = jobFilterArg.unwrap
    
    (startDt, endDt, jobFilter).mapN(dbManager.getNumberOfJobs).produceResponse(
      res => res.map{ data => 
        log.info(s"Success. Got $data number of jobs")
        Ok(data.toString)
      }.recover { case e => 
        log.error(s"Unable to get number of jobs due to following error:\n${e.getStackTraceAsString}")
        InternalServerError(e.getMessage)
      }.unwrap
    )(log)
  }

  def getJobsInfoResponse(startDtArg: Option[ValidatedNel[ParseFailure, EnrichedDT]],
                          endDtArg: Option[ValidatedNel[ParseFailure, EnrichedDT]]): IO[Response[IO]] = {
    val startDt = startDtArg.withDefault(EnrichedDT(dateFmt, dateTz).withOffset(Duration("7d")))
    val endDt = endDtArg.withDefault(EnrichedDT(dateFmt, dateTz))

    (startDt, endDt).mapN(dbManager.getJobsInfo).produceResponse(
      res => res.map{ data =>
        log.info(s"Success. Got ${data.size} job results")
        Ok(data.asJson)
      }.recover { case e =>
        log.error(s"Unable to get jobs summary results due to following error:\n${e.getStackTraceAsString}")
        InternalServerError(e.getMessage)
      }.unwrap
    )(log)
  }
  
  def dqStorageRoutes: HttpRoutes[IO] = HttpRoutes.of {
    case GET -> Root / "num-jobs" :? StartDtOptValQPM(startDtOpt)
                                  +& EndDtOptValQPM(endDtOpt)
                                  +& JobFilterOptValQPM(jobFilterOpt) => 
      getNumberOfJobsResponse(startDtOpt, endDtOpt, jobFilterOpt)
    case GET -> Root / "jobs" :? StartDtOptValQPM(startDtOpt)
                              +& EndDtOptValQPM(endDtOpt) =>
      getJobsInfoResponse(startDtOpt, endDtOpt)
    case GET -> Root / "jobs" / jobId / "state" :? DtOptValQPM(dt) => ???
    case GET -> Root / "jobs" / jobId / "results" :? StartDtOptValQPM(startDtOpt)
                                                  +& EndDtOptValQPM(endDtOpt) => ???
  }
}
object StorageRoutes {
  def apply(dbManager: APIJdbcStorageManager, settings: AppSettings): StorageRoutes =
    new StorageRoutes(dbManager, settings)
}