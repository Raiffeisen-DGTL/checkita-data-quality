package org.checkita.core.metrics.trend

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType
import org.checkita.appsettings.AppSettings
import org.checkita.core.CalculatorStatus
import org.checkita.core.Results.{MetricCalculatorResult, ResultType}
import org.checkita.core.metrics.ErrorCollection.{ErrorRow, MetricErrors}
import org.checkita.core.metrics.TrendMetric
import org.checkita.storage.Managers.DqStorageManager
import org.checkita.storage.Models._

import scala.util.{Failure, Success, Try}

object TrendMetricCalculator {
  
  /**
   * Function which aggregates historical metric results and computes a desired statistic.
   *
   * @param data Historical metric results
   * @return Statistic computed over historical metric results.
   *
   * @note Linear method is used to compute quantiles (method #7 in [1]).
   *
   * [1] R. J. Hyndman and Y. Fan, "Sample quantiles in statistical packages,
   *     "The American Statistician, 50(4), pp. 361-365, 1996
   */
  private def aggregate(data: Seq[Double], f: DescriptiveStatistics => Double): Double = {
    val stats = new DescriptiveStatistics
    stats.setPercentileImpl(new Percentile().withEstimationType(EstimationType.R_7))
    data.foreach(stats.addValue)
    f(stats)
  }

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
                            settings: AppSettings): Seq[Double] = {
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
    
    val resultVals = historyResults.map(_.result).filterNot(_.isNaN)
    
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
    aggregate(historicalResults, tm.aggFunc)
  } match {
    case Success(result) => MetricCalculatorResult(
      tm.metricId,
      tm.metricName.entryName,
      result,
      None,
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
