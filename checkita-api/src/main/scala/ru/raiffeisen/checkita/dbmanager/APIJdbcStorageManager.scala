package ru.raiffeisen.checkita.dbmanager


import io.circe.Json
import io.circe.parser._
import ru.raiffeisen.checkita.models.StorageModels._
import ru.raiffeisen.checkita.storage.Connections.DqStorageJdbcConnection
import ru.raiffeisen.checkita.storage.Managers.DqJdbcStorageManager
import ru.raiffeisen.checkita.storage.Models._
import ru.raiffeisen.checkita.utils.{EnrichedDT, Logging}

import java.sql.Timestamp
import java.time.ZoneId
import scala.concurrent.Future
import scala.reflect.runtime.universe.TypeTag

/**
 * Enhanced version of DqJdbcStorageManger by providing series of methods
 * to fetch various results from DQ Storage.
 * @param ds Instance of Jdbc connection to DQ Storage
 */
class APIJdbcStorageManager(ds: DqStorageJdbcConnection) 
  extends DqJdbcStorageManager(ds) 
    with Logging
    with Serializable {

  import profile.api._
  import tables.TableImplicits._
  
  log.debug(s"Default system timezone is: ${ZoneId.systemDefault()}")
  
  private def getTable[R <: DQEntity : TypeTag]
                      (implicit ops: tables.DQTableOps[R]): tables.profile.api.TableQuery[ops.T] =
    ops.getTableQuery(ds.getSchema)

  /**
   * Loads results of particular type from DQ storage.
   * Results are loaded for partical job and provided reference date.
   * If reference date is not provided, then results are loaded for
   * latest available reference date.
   *
   * @param jobId        Job ID to load results for.
   * @param dt           Optional reference date to load results for.
   * @param singleRecord Boolean flag indicating whether a single record should be retrieved from storage.
   * @param ops          Implicit table extension methods used to retrieve instance of Slick table that matches the result type.
   * @tparam R Type of result to be loaded.
   * @return Future containing loaded sequence of results.
   */
  private def getResults[R <: DQEntity : TypeTag](jobId: String, dt: Option[EnrichedDT], singleRecord: Boolean = false)
                                                 (implicit ops: tables.DQTableOps[R]): Future[Seq[R]] = {
    val table = ops.getTableQuery(ds.getSchema)
    val preQuery = table.filter(_.jobId === jobId)
    val maxRefDate = table.filter(_.jobId === jobId).map(_.referenceDate).max
    val finalQuery = dt.map(d => preQuery.filter(_.referenceDate === d.getUtcTS))
      .getOrElse(preQuery.filter(_.referenceDate === maxRefDate)).result
    
    finalQuery.statements.foreach(log.debug)
    
    if (singleRecord) db.run(finalQuery.headOption).map(_.toSeq)
    else db.run(finalQuery)
  }

  /**
   * Parses configuration JSON string and retrieves job description
   * if it is present.
   *
   * @param config Job configuration JSON string.
   * @return Job description or empty string if job description is missing.
   */
  private def getJobDescription(config: String): String = {
    val json = parse(config).getOrElse(Json.Null)
    val cursor = json.hcursor
    cursor.downField("jobConfig").get[String]("jobDescription").getOrElse("")
  }
  
  private lazy val resMetRegTbl: tables.profile.api.TableQuery[tables.ResultMetricRegularTable] = getTable[ResultMetricRegular]
  private lazy val resMetCompTbl: tables.profile.api.TableQuery[tables.ResultMetricComposedTable] = getTable[ResultMetricComposed]
  private lazy val resChkTbl: tables.profile.api.TableQuery[tables.ResultCheckTable] = getTable[ResultCheck]
  private lazy val resChkLoadTbl: tables.profile.api.TableQuery[tables.ResultCheckLoadTable] = getTable[ResultCheckLoad]
  private lazy val jobStateTbl: tables.profile.api.TableQuery[tables.JobStateTable] = getTable[JobState]

  /**
   * Loads jobs summary information fro DQ storage. Result is a sequence of all available jobs in
   * the DQ storage with following information for each job:
   *   - Job ID
   *   - Job description
   *   - reference date of latest run
   *   - sequence of results in interval of [startDT, endDT].
   *     Results are a map of reference date to number of failed checks.
   *     If no checks are avaialbe for a particular reference date then -1 is returned.
   *
   * @param startDT Start of the interval for pulling check results.
   * @param endDT   End of the interval for pulling check results.
   * @return Job summary information.
   */
  def getJobsInfo(startDT: EnrichedDT, endDT: EnrichedDT): Future[Seq[JobInfo]] = {
    
    val renderTS = (ts: Timestamp) => EnrichedDT(startDT.dateFormat, startDT.timeZone, ts).render
    
    // get jobId and last job run referenceDate.
    val lastRun = jobStateTbl.groupBy(_.jobId).map {
      case (jobId, group) => (jobId, group.map(_.referenceDate).max)
    }
    
    // get check results grouped by jobId and referenceDate.
    // Then collapse results by computing number of failed checks per each run.
    val checks = resChkTbl
      .filter(t => t.referenceDate >= startDT.getUtcTS && t.referenceDate <= endDT.getUtcTS)
      .groupBy(t => (t.jobId, t.referenceDate))
      .map{
        case ((jobId, refDate), group) => 
          val failCnt = group.map(t => Case If (t.status === "Success") Then 0 Else 1).sum
          (jobId, refDate, failCnt)
      }

    // join job information with fail counts:
    val finalQuery = jobStateTbl.joinLeft(checks)
      .on((t1, t2) => t1.jobId === t2._1 && t1.referenceDate === t2._2)
      .join(lastRun)
      .on((t12, t3) => t12._1.jobId === t3._1)
      .filter{
        case ((t1, t2), t3) => t1.referenceDate === t3._2 || t2.nonEmpty  
      }
      .map{
        case ((t1, t2), t3) => (
          t1.jobId,
          Case If (t1.referenceDate === t3._2) Then t1.config Else "N/A",
          t3._2,
          t2.map(_._2),
          t2.map(_._3)
        )
      }.result
    
    finalQuery.statements.foreach(log.debug)
    
    // run query and process results.
    db.run(finalQuery).map{ data =>
      data.groupBy(d => (d._1, d._3))
        .map { case (k, v) => 
          val failCounts = v.flatMap{
            case (_, _, _, optDt, optFc) => for {
              dt <- optDt
              fc <- optFc
            } yield JobFailCount(renderTS(dt), fc.getOrElse(-1))
          }
          val jobDesc = getJobDescription(v.filter(_._2 != "N/A").head._2)
          k -> (jobDesc, failCounts)
        }.map{
          case (jobInfo, results) => JobInfo(
            jobInfo._1,
            results._1,
            jobInfo._2.map(renderTS).getOrElse(""),
            results._2
          )
        }.toSeq
    }
  }

  /**
   * Loads job state information from DQ storage.
   *
   * @param jobId Job ID to load state for.
   * @param dt    Reference date for which state is loaded. 
   *              If empty, then latest available job state is loaded.
   * @return Job state: sequence containing single record 
   *         or empty sequence in case if job state was not found 
   *         for given jobId and for provided reference date.
   */
  def getJobState(jobId: String, dt: Option[EnrichedDT] = None): Future[Seq[JobState]] = 
    getResults[JobState](jobId, dt, singleRecord = true)
  
  /**
   * Loads job results from DQ storage.
   * Results include following information:
   *   - sequence of regular metric results
   *   - sequence of composed metric results
   *   - sequence of load checks results
   *   - sequence of check results
   *   - sequence of collected metric errors
   * 
   * @param jobId Job ID to load results for.
   * @param dt Reference date for which results are loaded.
   *           If empty then results are loaded for latest avaiable reference date.
   * @return Job results.
   */
  def getJobResults(jobId: String, dt: Option[EnrichedDT] = None): Future[JobResults] = for {
    regMetRes <- getResults[ResultMetricRegular](jobId, dt)
    compMetRes <- getResults[ResultMetricComposed](jobId, dt)
    errMetRes <- getResults[ResultMetricError](jobId, dt)
    loadChkRes <- getResults[ResultCheckLoad](jobId, dt)
    chkRes <- getResults[ResultCheck](jobId, dt)
  } yield JobResults(regMetRes, compMetRes, loadChkRes, chkRes, errMetRes)
  
}
