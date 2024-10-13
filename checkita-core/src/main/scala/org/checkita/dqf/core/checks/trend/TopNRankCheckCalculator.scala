package org.checkita.dqf.core.checks.trend

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.Enums.TrendCheckRule
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.Results.{CheckCalculatorResult, MetricCalculatorResult, ResultType}
import org.checkita.dqf.core.checks.{CheckCalculator, CheckName}
import org.checkita.dqf.core.metrics.MetricName
import org.checkita.dqf.storage.Managers.DqStorageManager
import org.checkita.dqf.storage.Models.ResultMetricRegular

import scala.util.Try

/**
 * `Top N Rank` check calculator: compares top target number of records for referenced TOP_N metric result
 * and its previous result and verifies if Jacquard distance between these sets of values is less than
 * a given threshold
 * @param checkId Check ID
 * @param baseMetric Base metric ID
 * @param compareThreshold Jacquard distance threshold
 * @param isCritical Flag if check is critical
 */
case class TopNRankCheckCalculator(checkId: String,
                                   baseMetric: String,
                                   targetNumber: Int,
                                   compareThreshold: Double,
                                   isCritical: Boolean
                                  ) extends CheckCalculator with WindowParams {
  override val checkName: CheckName = CheckName.TopNRank
  override val compareMetric: Option[String] = None
  override val rule: TrendCheckRule = TrendCheckRule.Record
  override val windowSize: String = "1"
  override val windowOffset: Option[String] = None

  /**
   * Calculates Jacquard distance betwen two sets
   * @param set1 Set 1
   * @param set2 Set 2
   * @return Jacquard distance
   */
  private def calculateJacquardDistance(set1: Set[String], set2: Set[String]): Double =
    1 - set1.intersect(set2).size.toFloat / set1.union(set2).size

  /**
   * Returns check check result in case of runtime error
   * @param errMsg Captured error message
   * @param sourceIds Base metric source IDs
   * @return Check result with Error status
   */
  private def onErrorResult(errMsg: String, sourceIds: Seq[String]): CheckCalculatorResult = CheckCalculatorResult(
    checkId,
    checkName.entryName,
    sourceIds,
    baseMetric,
    compareMetric.toSeq,
    Some(compareThreshold),
    Some(compareThreshold),
    Some(compareThreshold),
    status = CalculatorStatus.Error,
    message = errMsg,
    isCritical = isCritical,
    resultType = ResultType.Check
  )

  /**
   * Generates comprehensive check message
   * @param baseMetricResult Base metric result
   * @param compareMetricResult Compare metric result
   * @param status Check evaluation status
   * @param statusString Check evaluation status string
   * @return Check message
   */
  override protected def getMessage(baseMetricResult: MetricCalculatorResult,
                                    compareMetricResult: Option[MetricCalculatorResult],
                                    status: CalculatorStatus,
                                    statusString: String): String = {
    val common = s"Check '$checkId' for TOP_N metric '${baseMetricResult.metricId}'"
    val sId = Try(baseMetricResult.sourceIds.head).getOrElse("undefined")
    val onSource = s" on source '$sId' and columns ${baseMetricResult.columns.mkString("[", ",", "]")}"

    common + onSource + windowString.getOrElse("") + getDetailsMsg(compareMetricResult).getOrElse(".") + 
      s" Result: ${status.toString}. " + statusString
  }
  
  /**
   * Gets check details message to insert into final check message.
   *
   * @param compareMetricResult Compare metric result
   * @return Check details message
   */
  protected def getDetailsMsg(compareMetricResult: Option[MetricCalculatorResult]): Option[String] = 
    Some(s": check if Jacquard distance between top $targetNumber values of this metric result and a previous one " +
      s"is less than (or equals to) threshold of $compareThreshold.")

  /**
   * Runs the check for the given metric results.
   *
   * @param jobId                Current Job ID
   * @param baseMetricResults    Sequence of base metric result for metric ID referenced by this check
   * @param compareMetricResults Sequence of compare metric result for compareMetric ID referenced by this check
   * @param manager           Implicit storage manager used to load historical results
   * @param settings             Implicit application settings 
   * @param spark                Implicit spark session object
   * @param fs                   Implicit hadoop filesystem object
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
      baseMetricResults.nonEmpty && 
        baseMetricResults.forall(r => r.resultType == ResultType.RegularMetric) &&
        baseMetricResults.forall(r => r.metricName.startsWith(MetricName.TopN.entryName)),
      s"Non empty sequence of TopN metric results is expected in ${checkName.entryName} check."
    )
    
    require(manager.nonEmpty, 
      "In order to perform TOP_N_RANK check it is required to have a valid connection to results storage."
    )
    
    val mgr = manager.get
    import mgr.tables.TableImplicits._

    val baseMetricIds = baseMetricResults.map(r => r.metricId).distinct

    val baseResults = baseMetricResults.flatMap(r => r.additionalResult).toSet
    
    // actual window size should be equal to targetNumber as we need to pull
    // several records with TopN metric results
    val historyResults = mgr.loadMetricResults[ResultMetricRegular](
      jobId, baseMetricIds, rule, settings.referenceDateTime, targetNumber.toString, windowOffset
    ).map(r => r.referenceDate -> r.additionalResult)
      .groupBy(_._1) // grouping results by referenceDate
      .map { case (k, v) => k -> v.flatMap(t => t._2) }.toSeq
      .maxBy(t => t._1.getTime)._2.toSet // collect results for latest referenceDate
    
    if (historyResults.isEmpty) throw new IllegalArgumentException(
      s"There ara no historical results found to perform ${checkName.entryName} check '$checkId'."
    )
    
    val dist = calculateJacquardDistance(baseResults, historyResults)

    val (status, statusString) =
      if (dist <= compareThreshold)
        (CalculatorStatus.Success, s"Jacquard distance of $dist <= $compareThreshold")
      else
        (CalculatorStatus.Failure, s"Jacquard distance of $dist > $compareThreshold")

    CheckCalculatorResult(
      checkId,
      checkName.entryName,
      baseMetricResults.head.sourceIds,
      baseMetric,
      compareMetric.toSeq,
      Some(compareThreshold),
      Some(compareThreshold),
      Some(compareThreshold),
      status = status,
      message = getMessage(baseMetricResults.head, None, status, statusString),
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
    val msg = getMessage(baseMetricResults.head, None, CalculatorStatus.Error, errorMsg(err))
    onErrorResult(msg, baseMetricResults.head.sourceIds)
  }

  /**
   * Callback method that is used when metric results are not found for metric ID referenced in this check
   *
   * @return Check result with Error status and 'not found' error message
   */
  protected def resultOnMetricNotFound: CheckCalculatorResult = onErrorResult(notFoundErrMsg, Seq.empty)
}
