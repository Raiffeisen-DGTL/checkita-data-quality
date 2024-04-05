package ru.raiffeisen.checkita.core.checks

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Results.{CheckCalculatorResult, MetricCalculatorResult, ResultType}
import ru.raiffeisen.checkita.core.metrics.BasicMetricProcessor.MetricResults
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager

import scala.util.{Failure, Success, Try}

/** Base Check Calculator */
abstract class CheckCalculator {
  
  val checkId: String
  val checkName: CheckName
  val baseMetric: String
  val compareMetric: Option[String]
  
  protected val windowString: Option[String]
  protected val notFoundErrMsg: String = 
    s"Check $checkId for metric '$baseMetric' cannot be run: metric results were not found."

  /**
   * Safely gets head out of optional sequence of metric calculator results.
   * @param resSeq Optional sequence of metric calculator results
   * @return Optional metric calculator result
   */
  protected def getOptionHead(resSeq: Option[Seq[MetricCalculatorResult]]): Option[MetricCalculatorResult] =
    resSeq.flatMap(r => Try(r.head).toOption)
    
  /**
   * Generates comprehensive check message
   * @param baseMetricResult Base metric result
   * @param compareMetricResult Compare metric result
   * @param status Check evaluation status
   * @param statusString Check evaluation status string
   * @return Check message
   */
  protected def getMessage(baseMetricResult: MetricCalculatorResult,
                           compareMetricResult: Option[MetricCalculatorResult],
                           status: CalculatorStatus,
                           statusString: String): String = {
    val common = s"Check '$checkId' for ${baseMetricResult.metricName} metric '${baseMetricResult.metricId}'"

    val onSource = baseMetricResult.resultType match {
      case ResultType.RegularMetric =>
        val sId = Try(baseMetricResult.sourceIds.head).getOrElse("undefined")
        s" on source '$sId' and columns ${baseMetricResult.columns.mkString("[", ",", "]")}"
      case ResultType.ComposedMetric =>
        s" derived from sources ${baseMetricResult.sourceIds.mkString("[", ",", "]")}"
      case _ => ""
    }
    
    val details = getDetailsMsg(compareMetricResult)
      .map(s => s": check if (metricResult) ${baseMetricResult.result} $s").getOrElse(".")
    
    common + onSource + windowString.getOrElse("") + details + s" Result: ${status.toString}. " + statusString
  }

  /**
   * Gets check details message to insert into final check message.
   * @param compareMetricResult Compare metric result
   * @return Check details message
   */
  protected def getDetailsMsg(compareMetricResult: Option[MetricCalculatorResult]): Option[String]

  /**
   * Generates error message provided with caught error.
   * @param e Error that has been caught
   * @return Error message
   */
  protected def errorMsg(e: Throwable): String = s"Unable to perform check due to following error: ${e.getMessage}"

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
                         fs: FileSystem): CheckCalculatorResult

  /**
   * Callback method that process possible runtime error that can be thrown during check evaluation.
   * @param err Error thrown during check evaluation
   * @param baseMetricResults Sequence of base metric result for metric ID referenced by this check
   * @param compareMetricResults Sequence of compare metric result for compareMetric ID referenced by this check
   * @note TopN metric yields multiple results
   * @return Check result with Error status and captured error message
   */
  protected def resultOnError(err: Throwable,
                              baseMetricResults: Seq[MetricCalculatorResult],
                              compareMetricResults: Option[Seq[MetricCalculatorResult]]): CheckCalculatorResult

  /**
   * Callback method that is used when metric results are not found for metric ID referenced in this check
   * @return Check result with Error status and 'not found' error message
   */
  protected def resultOnMetricNotFound: CheckCalculatorResult

  /** Safely runs check provided with all the metric calculators results. There are three scenarios covered:
   *  - Check evaluates normally and returns either Success or Failure status
   *    (depending on whether check condition was met)
   *  - Check evaluation throws runtime error:
   *    check result with Error status and corresponding error message is returned.
   *  - Metric results are not found for metric ID defined in the check:
   *    check cannot be run at all and check result with Error status and corresponding message is returned.
   * @param jobId Current Job ID
   * @param metricResults All computed metrics
   * @param manager Implicit storage manager used to load historical results
   * @param settings Implicit application settings 
   * @param spark Implicit spark session object
   * @param fs Implicit hadoop filesystem object
   * @return Check result
   */  
  def run(metricResults: MetricResults)
         (implicit jobId: String,
          manager: Option[DqStorageManager],
          settings: AppSettings,
          spark: SparkSession,
          fs: FileSystem): CheckCalculatorResult = metricResults.get(baseMetric) match {
    case Some(baseMetricResults) if baseMetricResults.nonEmpty =>
      val compareMetricResults = compareMetric.flatMap(metricResults.get)
      Try(tryToRun(baseMetricResults, compareMetricResults)) match {
        case Success(result) => result
        case Failure(err) => resultOnError(err, baseMetricResults, compareMetricResults)
      }
    case _ => resultOnMetricNotFound
  }

}
