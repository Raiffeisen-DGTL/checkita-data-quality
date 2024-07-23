package org.checkita.routes

import cats.data.ValidatedNel
import cats.effect.IO
import cats.implicits._
import io.circe.generic.auto._
import io.circe.syntax._
import org.checkita.dbmanager.APIJdbcStorageManager
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.io._
import org.http4s.{HttpRoutes, ParseFailure, QueryParamDecoder, Response}
import org.checkita.appsettings.AppSettings
import org.checkita.common.ImplicitOps._
import org.checkita.config.IO.{RenderOptions, readEncryptedJobConfig, writeJobConfig}
import org.checkita.config.{ConfigEncryptor, RefinedTypes}
import org.checkita.models.CustomEncoders.dqEntityEncoder
import org.checkita.utils.ResultUtils._
import org.checkita.utils.{EnrichedDT, Logging}

import java.time.ZoneId
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Try

class StorageRoutes(dbManager: APIJdbcStorageManager, implicit val settings: AppSettings) extends Logging {

  private val dateFmt: RefinedTypes.DateFormat = settings.referenceDateTime.dateFormat
  private val dateTz: ZoneId = settings.referenceDateTime.timeZone
  private implicit val ec: ExecutionContextExecutor = dbManager.ec

  /**
   * Query parameter decoder for dates.
   * Dates are decoded as instances of EnrichedDT.
   */
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


  /**
   * Function to generate response for job info request.
   *
   * @param startDtArg Optional start date argument
   * @param endDtArg   Optional end date argument
   * @return Response containing job info for given time interval.
   */
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

  /**
   * Function to generate response for job state request
   *
   * @param jobId Job ID for which to retrieve state.
   * @param dtArg Data at which the state is retrieved. 
   *              If empty, then job state is retrieved for latest available reference date.
   * @return Response containing job state at given reference date.
   */
  def getJobStateResponse(jobId: String, dtArg: Option[ValidatedNel[ParseFailure, EnrichedDT]]): IO[Response[IO]] = 
    dtArg.unwrap
      .map(dt => dbManager.getJobState(jobId, dt))
      .produceResponse(
        res => res.map{ jobStateRes =>
          if (jobStateRes.isEmpty) {
            log.info(s"Empty response. Query job state for jobID '$jobId' returned empty result.")
            NotFound(s"No job state for jobId '$jobId' was found.")
          } else {
            val js = jobStateRes.head
            log.info(s"Success. Got job state for jobId '$jobId'.")
            val finalJobState = settings.encryption
              .map { enCfg =>
                implicit val enc: ConfigEncryptor = new ConfigEncryptor(enCfg.secret, enCfg.keyFields)
                for {
                  decrypted <- readEncryptedJobConfig(js.config)
                  written <- writeJobConfig(decrypted)
                } yield js.copy(config = written.root().render(RenderOptions.COMPACT))
              }.getOrElse(liftToResult(js))
            
            finalJobState match {
              case Right(jobState) => Ok(jobState.asJson)
              case Left(errors) =>
                log.error(s"Unable to decrypt job state due to following error:\n${errors.mkString("\n")}")
                InternalServerError("Job state decryption error.")
            }
          }
        }.recover { case e =>
          log.error(s"Unable to get job state for jobId '$jobId' due to following error:\n${e.getStackTraceAsString}")
          InternalServerError(e.getMessage)
        }.unwrap
      )(log)

  /**
   * Function to generate response for job results request
   *
   * @param jobId Job ID for which to retrieve results.
   * @param dtArg Data at which the results are retrieved. 
   *              If empty, then job results are retrieved for latest available reference date.
   * @return Response containing job state at given reference date.
   */
  def getJobResultsResponse(jobId: String, dtArg: Option[ValidatedNel[ParseFailure, EnrichedDT]]): IO[Response[IO]] =
    dtArg.unwrap
      .map(dt => dbManager.getJobResults(jobId, dt))
      .produceResponse(
        res => res.map { jobResults =>
          if (jobResults.isEmpty) {
            log.info(s"Empty response. Query job results for jobID '$jobId' are empty.")
            NotFound(s"No job results for jobId '$jobId' were found.")
          } else {
            log.info(s"Success. Got job results for jobId '$jobId'.")
            val finalJobResults = settings.encryption
              .map(enCfg => new ConfigEncryptor(enCfg.secret, enCfg.keyFields))
              .map { enc =>
                val decryptedErrors = Try {
                  jobResults.errors.map { err =>
                    val decryptedRowData = enc.decrypt(err.rowData)
                    err.copy(rowData = decryptedRowData)
                  }
                }.toResult(preMsg = "Unable to decrypt metric errors rowData due to following error:")
                decryptedErrors.map(dErrs => jobResults.copy(errors = dErrs))
              }.getOrElse(liftToResult(jobResults))

            finalJobResults match {
              case Right(jobRes) => Ok(jobRes.asJson)
              case Left(errors) =>
                log.error(s"Unable to decrypt metric errors due to following error:\n${errors.mkString("\n")}")
                InternalServerError("Metric errors decryption error.")
            }
          }
        }.recover{ case e =>
          log.error(s"Unable to get job results for jobId '$jobId' due to following error:\n${e.getStackTraceAsString}")
          InternalServerError(e.getMessage)        
        }.unwrap
      )(log)
    
  
  /**
   * List of API routes for interaction DQ Storage.
   *
   * @return HTTP Routes
   */
  def dqStorageRoutes: HttpRoutes[IO] = HttpRoutes.of {
    case GET -> Root / "jobs" :? StartDtOptValQPM(startDtOpt)
                              +& EndDtOptValQPM(endDtOpt) =>
      getJobsInfoResponse(startDtOpt, endDtOpt)
    case GET -> Root / "jobs" / jobId / "state" :? DtOptValQPM(dt) => getJobStateResponse(jobId, dt)
    case GET -> Root / "jobs" / jobId / "results" :? DtOptValQPM(dt) => getJobResultsResponse(jobId, dt)
  }
}

object StorageRoutes {
  def apply(dbManager: APIJdbcStorageManager, settings: AppSettings): StorageRoutes =
    new StorageRoutes(dbManager, settings)
}