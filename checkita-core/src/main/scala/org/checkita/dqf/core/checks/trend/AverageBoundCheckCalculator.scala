package org.checkita.dqf.core.checks.trend

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.Results.{CheckCalculatorResult, MetricCalculatorResult, ResultType}
import org.checkita.dqf.core.checks.CheckCalculator
import org.checkita.dqf.storage.Managers.DqStorageManager
import org.checkita.dqf.storage.Models._

/** Base for all `average bound` trend checks */
abstract class AverageBoundCheckCalculator extends CheckCalculator with WindowParams {

  val lThreshold: Option[Double]
  val uThreshold: Option[Double]
  
  val compareMetric: Option[String] = None
  
  protected val lBound: Double => Option[Double] =
    avgResult => lThreshold.map(t => avgResult * (1 - t))
  protected val uBound: Double => Option[Double] =
    avgResult => uThreshold.map(t => avgResult * (1 + t))

  private val compareFunc: (Double, Double) => Boolean = (baseResult, avgResult) => {
    val lBoundCheck = lBound(avgResult).forall(bound => baseResult >= bound)
    val uBoundCheck = uBound(avgResult).forall(bound => baseResult <= bound)
    lBoundCheck && uBoundCheck
  }
  
  private def onErrorResult(errMsg: String, sourceIds: Seq[String]): CheckCalculatorResult = CheckCalculatorResult(
    checkId,
    checkName.entryName,
    sourceIds,
    baseMetric,
    compareMetric.toSeq,
    lThreshold.orElse(uThreshold),
    None,
    None,
    status = CalculatorStatus.Error,
    message = errMsg,
    isCritical = isCritical,
    resultType = ResultType.Check
  )
  
  protected val compareFuncSuccessRepr: (Double, Double) => String
  protected val compareFuncFailureRepr: (Double, Double) => String

  /**
   * Runs the check for the given metric results.
   * @param jobId Current Job ID
   * @param baseMetricResults Sequence of base metric result for metric ID referenced by this check
   * @param compareMetricResults Sequence of compare metric result for compareMetric ID referenced by this check
   * @param manager Implicit storage manager used to load historical results
   * @param settings Implicit application settings 
   * @param spark Implicit spark session object
   * @param fs Implicit hadoop filesystem object
   * @note TopN metric yields multiple results
   * @return Check evaluation result with either Success or Failure status
   */
  protected def tryToRun(baseMetricResults: Seq[MetricCalculatorResult],
                         compareMetricResults: Option[Seq[MetricCalculatorResult]])
                        (implicit jobId: String,
                         manager: Option[DqStorageManager],
                         settings: AppSettings,
                         spark: SparkSession,
                         fs: FileSystem): CheckCalculatorResult = {

    require(
      baseMetricResults.size == 1,
      s"Exactly one base metric result is expected in ${checkName.entryName} check."
    )

    require(manager.nonEmpty,
      s"In order to perform ${checkName.entryName} check it is required to have a valid connection to results storage."
    )

    val mgr = manager.get
    import mgr.tables.TableImplicits._
    
    val baseMetricCalcRes = baseMetricResults.head
    
    val historyResults = baseMetricCalcRes.resultType match {
      case ResultType.RegularMetric => mgr.loadMetricResults[ResultMetricRegular](
        jobId, Seq(baseMetricCalcRes.metricId), rule, settings.referenceDateTime, windowSize, windowOffset
      )
      case ResultType.ComposedMetric => mgr.loadMetricResults[ResultMetricComposed](
        jobId, Seq(baseMetricCalcRes.metricId), rule, settings.referenceDateTime, windowSize, windowOffset
      )
      case other => throw new IllegalArgumentException(
        s"Trend check '${checkName.entryName}' requires that either source or composed metric results to be provided." + 
          s" Got following results instead: '${other.entryName}'."
      )
    }

    val resultVals = historyResults.map(_.result).filterNot(_.isNaN)
    
    if (resultVals.isEmpty) throw new IllegalArgumentException(
      s"There ara no historical results found to perform ${checkName.entryName} check '$checkId'."
    )
    
    val baseResult = baseMetricCalcRes.result
    val avgResult = resultVals.sum / resultVals.length

    val (status, statusString) =
      if (compareFunc(baseResult, avgResult))
        (CalculatorStatus.Success, compareFuncSuccessRepr(baseResult, avgResult))
      else
        (CalculatorStatus.Failure, compareFuncFailureRepr(baseResult, avgResult))

    CheckCalculatorResult(
      checkId,
      checkName.entryName,
      baseMetricCalcRes.sourceIds,
      baseMetric,
      compareMetric.toSeq,
      lThreshold.orElse(uThreshold),
      lBound(avgResult),
      uBound(avgResult),
      status = status,
      message = getMessage(baseMetricCalcRes, None, status, statusString),
      isCritical = isCritical,
      resultType = ResultType.Check
    )

  }

  /**
   * Callback method that process possible runtime error that can be thrown during check evaluation.
   *
   * @param err                  Error thrown during check evaluation
   * @param baseMetricResults    Sequence of base metric result for metric ID referenced by this check
   * @param compareMetricResults Sequence of compare metric result for compareMetric ID referenced by this check
   * @note TopN metric yields multiple results
   * @return Check result with Error status and captured error message
   */
  protected def resultOnError(err: Throwable,
                              baseMetricResults: Seq[MetricCalculatorResult],
                              compareMetricResults: Option[Seq[MetricCalculatorResult]]): CheckCalculatorResult = {

    val compareMetricCalcRes = getOptionHead(compareMetricResults)
    val msg = getMessage(
      baseMetricResults.head,
      compareMetricCalcRes,
      CalculatorStatus.Error,
      errorMsg(err)
    )
    
    onErrorResult(msg, baseMetricResults.head.sourceIds)
  }

  /**
   * Callback method that is used when metric results are not found for metric ID referenced in this check
   *
   * @return Check result with Error status and 'not found' error message
   */
  protected def resultOnMetricNotFound: CheckCalculatorResult = onErrorResult(notFoundErrMsg, Seq.empty)
}
