package org.checkita.dqf.core.metrics.trend

import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.Results.{MetricCalculatorResult, ResultType}
import org.checkita.dqf.core.metrics.ErrorCollection.{ErrorRow, MetricErrors}
import org.checkita.dqf.core.metrics.TrendMetric
import org.checkita.dqf.storage.Managers.DqStorageManager
import org.checkita.dqf.storage.Models._

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

object TrendMetricCalculator {

  /**
   * Returns metric calculator result with error status and corresponding error message
   * wrapped into metric errors.
   *
   * @param tm        Trend metric that yielded calculation error
   * @param errMsg    Error message
   * @param sourceIds Source IDs on which metric was calculated
   * @return Metric calculator result with Error Status.
   */
  def resultOnError(tm: TrendMetric, errMsg: String, sourceIds: Seq[String]): MetricCalculatorResult = {
    val shortMsg = "Metric calculation failed with error."
    val errRow = ErrorRow(CalculatorStatus.Error, errMsg, Seq.empty)
    MetricCalculatorResult(
      tm.metricId,
      tm.metricName.entryName,
      Double.NaN,
      Some(shortMsg),
      sourceIds,
      Seq.empty,
      Seq.empty,
      Some(MetricErrors(Seq.empty, Seq(errRow))),
      ResultType.TrendMetric
    )
  }
  
  
  def loadHistoricalResults(tm: TrendMetric,
                            lookupMetResultType: ResultType)
                           (implicit jobId: String,
                            manager: Option[DqStorageManager],
                            settings: AppSettings): Seq[(Timestamp, Double)] = {
    require(
      manager.nonEmpty,
      s"In order to perform trend metric calculation it is required to have a valid connection to results storage."
    )
    
    val mgr = manager.get
    import mgr.tables.TableImplicits._
    
    val historyResults = lookupMetResultType match {
      case ResultType.RegularMetric => mgr.loadMetricResults[ResultMetricRegular](
        jobId, Seq(tm.lookupMetricId), tm.rule, settings.referenceDateTime, tm.wSize, tm.wOffset
      )
      case ResultType.ComposedMetric => mgr.loadMetricResults[ResultMetricComposed](
        jobId, Seq(tm.lookupMetricId), tm.rule, settings.referenceDateTime, tm.wSize, tm.wOffset
      )
      case other => throw new IllegalArgumentException(
        s"Trend metric requires that either regular or composed metric results to be provided." +
          s" Got following results instead: '${other.entryName}'."
      )
    }
    
    val resultVals = historyResults
      .filterNot(_.result.isNaN)
      .map(r => (r.referenceDate, r.result))
    
    if (resultVals.isEmpty) throw new IllegalArgumentException(
      s"Unable to perform calculation of trend metric '${tm.metricId}' due to historical results were not found " +
        s"for lookup metric '${tm.lookupMetricId}'."
    )

    resultVals
  }
  
  /** Safely runs trend metric. 
   * Trend metric calculator do not interact with actual data but rather uses historical metric results
   * to calculate required statistic.
   * Thus, trend metrics cannot yield Failure status and only two scenarios are considered:
   *  - Trend metric evaluates normally:
   *    metric results with Success status and evaluated metric value is returned.
   *  - Trend metric evaluation throws runtime error:
   *    metric result with Error status and corresponding error message is returned.
   *
   * @param tm                  Trend metric to evaluate
   * @param lookupMetResultType Result type of lookup metric. 
   *                            Required to fetch results from appropriate table in DQ storage.
   * @param lookupMetSourceIds  List of source IDs for lookup metric. 
   *                            Trend metric inherits list of source IDs from lookup metric.
   * @param jobId               Current Job ID
   * @param manager             Implicit storage manager used to load historical results
   * @param settings            Implicit application settings
   * @return Trend metric result
   */
  def run(tm: TrendMetric,
          lookupMetResultType: ResultType,
          lookupMetSourceIds: Seq[String])(implicit jobId: String,
                                           manager: Option[DqStorageManager],
                                           settings: AppSettings): MetricCalculatorResult = Try {
    val historicalResults = loadHistoricalResults(tm, lookupMetResultType)
    tm.model.infer(historicalResults, settings.referenceDateTime.getUtcTS)
  } match {
    case Success(result) => MetricCalculatorResult(
      tm.metricId,
      tm.metricName.entryName,
      result._1,
      result._2,
      lookupMetSourceIds,
      Seq.empty,
      Seq.empty,
      None,
      ResultType.TrendMetric
    )
    case Failure(e) => resultOnError(tm,
      s"Failed to calculate trend metric '${tm.metricId}' due to following error:\n" + e.getMessage,
      lookupMetSourceIds
    )
  }
}
