package org.checkita.dqf.core.checks.snapshot

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.Results.{CheckCalculatorResult, MetricCalculatorResult, ResultType}
import org.checkita.dqf.core.checks.CheckCalculator
import org.checkita.dqf.storage.Managers.DqStorageManager

/**
 * Base class for simple comparison checks (EQUAL_TO, LESS_THAN, GREATER_THAN)
 */
abstract class CompareCheckCalculator extends CheckCalculator {
  
  val compareThreshold: Option[Double]
  
  protected val compareFunc: (Double, Double) => Boolean
  protected val compareFuncSuccessRepr: (Double, Double) => String
  protected val compareFuncFailureRepr: (Double, Double) => String
  protected val compareFuncString: String
  protected val lBound: (Option[MetricCalculatorResult], Option[Double]) => Option[Double]
  protected val uBound: (Option[MetricCalculatorResult], Option[Double]) => Option[Double]
  
  protected val compareResOption: (Option[MetricCalculatorResult], Option[Double]) => Option[Double] =
    (x, y) => x.map(_.result).orElse(y)
  protected val noneBound: (Option[MetricCalculatorResult], Option[Double]) => Option[Double] = (_, _) => None
    
  protected val windowString: Option[String] = None
  
  /**
   * Gets check details message to insert into final check message.
   *
   * @param compareMetricResult Compare metric result
   * @return Check details message
   */
  protected def getDetailsMsg(compareMetricResult: Option[MetricCalculatorResult]): Option[String] =
    compareMetricResult.map(r => s"(compareMetricResult) ${r.result}")
      .orElse(compareThreshold.map(t => s"(threshold) $t"))
      .map(s => s"$compareFuncString $s.")


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
    require(
      (compareMetricResults.nonEmpty && compareMetricResults.get.size == 1 && compareThreshold.isEmpty) ||
        (compareMetricResults.isEmpty && compareThreshold.nonEmpty),
      s"Compare threshold or exactly one compare metric result is expected in ${checkName.entryName} check."
    )

    val baseMetricCalcRes = baseMetricResults.head
    val compareMetricCalcRes = getOptionHead(compareMetricResults)
    val baseResult = baseMetricCalcRes.result
    val compareResult = compareResOption(compareMetricCalcRes, compareThreshold).get

    val (status, statusString) =
      if (compareFunc(baseResult, compareResult))
        (CalculatorStatus.Success, compareFuncSuccessRepr(baseResult, compareResult))
      else
        (CalculatorStatus.Failure, compareFuncFailureRepr(baseResult, compareResult))

    CheckCalculatorResult(
      checkId,
      checkName.entryName,
      baseMetricCalcRes.sourceIds,
      baseMetric,
      compareMetric.toSeq,
      Some(compareResult),
      Some(compareResult),
      Some(compareResult),
      status = status,
      message = getMessage(baseMetricCalcRes, compareMetricCalcRes, status, statusString),
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

    CheckCalculatorResult(
      checkId,
      checkName.entryName,
      baseMetricResults.head.sourceIds,
      baseMetric,
      compareMetric.toSeq,
      compareResOption(compareMetricCalcRes, compareThreshold),
      lBound(compareMetricCalcRes, compareThreshold),
      uBound(compareMetricCalcRes, compareThreshold),
      status = CalculatorStatus.Error,
      message = msg,
      isCritical = isCritical,
      resultType = ResultType.Check
    )
  }

  /**
   * Callback method that is used when metric results are not found for metric ID referenced in this check
   *
   * @return Check result with Error status and 'not found' error message
   */
  protected def resultOnMetricNotFound: CheckCalculatorResult =
    CheckCalculatorResult(
      checkId,
      checkName.entryName,
      Seq.empty,
      baseMetric,
      compareMetric.toSeq,
      compareThreshold, // if metric value is compared to a threshold its value is used, 
      compareThreshold, // ...
      compareThreshold, // otherwise None as we can't get result for compareMetric
      status = CalculatorStatus.Error,
      message = notFoundErrMsg,
      isCritical = isCritical,
      resultType = ResultType.Check
    )
}
