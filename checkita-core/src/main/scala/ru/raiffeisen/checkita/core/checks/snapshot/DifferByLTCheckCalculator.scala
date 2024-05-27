package ru.raiffeisen.checkita.core.checks.snapshot

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Results.{CheckCalculatorResult, MetricCalculatorResult, ResultType}
import ru.raiffeisen.checkita.core.checks.{CheckCalculator, CheckName}
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager

/**
 * `Differs by less than` check calculator:
 * verifies that difference between base metric result and compare metric result
 * is less than a given threshold.
 * @param checkId Check ID
 * @param baseMetric Base metric to check
 * @param compareMetric Metric to compare with
 * @param compareThreshold Maximum difference threshold
 */
case class DifferByLTCheckCalculator(checkId: String,
                                     baseMetric: String,
                                     compareMetric: Option[String],
                                     compareThreshold: Double
                                    ) extends CheckCalculator {
  val checkName: CheckName = CheckName.DifferByLT
  protected val windowString: Option[String] = None

  /**
   * Gets check details message to insert into final check message.
   *
   * @param compareMetricResult Compare metric result
   * @return Check details message
   */
  protected def getDetailsMsg(compareMetricResult: Option[MetricCalculatorResult]): Option[String] =
    compareMetricResult.map(r =>
      s"differs from (compareMetricResult) ${r.result} by less than (threshold) $compareThreshold."
    )
  
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
      compareMetricResults.nonEmpty && compareMetricResults.get.size == 1,
      s"Exactly one compare metric result is expected in ${checkName.entryName} check."
    )
    
    val baseMetricCalcRes = baseMetricResults.head
    val compareMetricCalcRes = compareMetricResults.map(_.head).get
    
    val baseResult = baseMetricCalcRes.result
    val compareResult = compareMetricCalcRes.result
    val metricDiff = 
      if (compareResult == 0 && baseResult == 0) 0
      else if (compareResult == 0) 1
      else Math.abs(baseResult - compareResult) / compareResult
    
    val (status, statusString) = 
      if (metricDiff < compareThreshold)
        (CalculatorStatus.Success, s"Metrics difference of $metricDiff < $compareThreshold.")
      else
        (CalculatorStatus.Failure, s"Metrics difference of $metricDiff >= $compareThreshold.")

    CheckCalculatorResult(
      checkId,
      checkName.entryName,
      baseMetricCalcRes.sourceIds,
      baseMetric,
      compareMetric,
      Some(compareThreshold),
      Some(lBound(compareMetricCalcRes)),
      Some(uBound(compareMetricCalcRes)),
      status = status,
      message = getMessage(baseMetricCalcRes, Some(compareMetricCalcRes), status, statusString),
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
      compareMetric,
      Some(compareThreshold),
      compareMetricCalcRes.map(lBound),
      compareMetricCalcRes.map(uBound),
      status = CalculatorStatus.Error,
      message = msg,
      resultType = ResultType.Check
    )
  }
  
  /**
   * Callback method that is used when metric results are not found for metric ID referenced in this check
   *
   * @return Check result with Error status and 'not found' error message
   */
  protected def resultOnMetricNotFound: CheckCalculatorResult = CheckCalculatorResult(
    checkId,
    checkName.entryName,
    Seq.empty,
    baseMetric,
    compareMetric,
    Some(compareThreshold),
    None,
    None,
    status = CalculatorStatus.Error,
    message = notFoundErrMsg,
    resultType = ResultType.Check
  )
  
  private val lBound = (r: MetricCalculatorResult) => (1 - compareThreshold) * r.result
  private val uBound = (r: MetricCalculatorResult) => (1 + compareThreshold) * r.result
}

