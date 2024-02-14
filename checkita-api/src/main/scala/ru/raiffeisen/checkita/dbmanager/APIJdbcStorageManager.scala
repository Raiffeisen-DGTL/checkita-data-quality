package ru.raiffeisen.checkita.dbmanager


import ru.raiffeisen.checkita.storage.Connections.DqStorageJdbcConnection
import ru.raiffeisen.checkita.storage.Managers.DqJdbcStorageManager
import ru.raiffeisen.checkita.storage.Models._
import ru.raiffeisen.checkita.utils.EnrichedDT

import scala.concurrent.Future
import scala.reflect.runtime.universe.TypeTag

/**
 * Enhances version of DqJdbcStorageManger by providing series of methods
 * to fetch various results from DQ Storage.
 * @param ds Instance of Jdbc connection to DQ Storage
 */
class APIJdbcStorageManager(ds: DqStorageJdbcConnection) extends DqJdbcStorageManager(ds) with Serializable {

  import profile.api._
  import tables.TableImplicits._

  private def getTable[R <: DQEntity : TypeTag]
                      (implicit ops: tables.DQTableOps[R]): tables.profile.api.TableQuery[ops.T] =
    ops.getTableQuery(ds.getSchema)

  private lazy val resMetRegTbl: tables.profile.api.TableQuery[tables.ResultMetricRegularTable] = getTable[ResultMetricRegular]
  private lazy val resMetCompTbl: tables.profile.api.TableQuery[tables.ResultMetricComposedTable] = getTable[ResultMetricComposed]
  private lazy val resChkTbl: tables.profile.api.TableQuery[tables.ResultCheckTable] = getTable[ResultCheck]
  private lazy val resChkLoadTbl: tables.profile.api.TableQuery[tables.ResultCheckLoadTable] = getTable[ResultCheckLoad]


  def getNumberOfJobs(startDT: EnrichedDT, endDT: EnrichedDT, jobFilter: Option[String]): Future[Int] = db.run(
    resChkTbl.filter{ t =>
      val dateOnlyFilter = t.referenceDate >= startDT.getUtcTS && t.referenceDate <= endDT.getUtcTS
      jobFilter.map(jf => t.jobId.like(jf) && dateOnlyFilter).getOrElse(dateOnlyFilter)
    }.map(_.jobId).distinct.length.result
  )


  /*
    QUERIES

    1) FACTOIDS
      a) get number of jobs
         Args:
          - startDate
          - endDate
          - jobFilter
      b) get number of regular sources
         Args:
          - start_date
          - end_date
          - jobId
          - jobFilter
          - sourceFilter
      c) get number of virtual sources
         Args:
          - start_date
          - end_date
          - jobId
          - jobFilter
          - sourceFilter
      d) tbd..
   */

}
