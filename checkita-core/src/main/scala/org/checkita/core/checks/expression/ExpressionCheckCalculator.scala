package org.checkita.core.checks.expression

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.checkita.appsettings.AppSettings
import org.checkita.core.CalculatorStatus
import org.checkita.core.Results.{CheckCalculatorResult, MetricCalculatorResult, ResultType}
import org.checkita.core.checks.{CheckCalculator, CheckName}
import org.checkita.core.metrics.BasicMetricProcessor.MetricResults
import org.checkita.storage.Managers.DqStorageManager
import org.checkita.utils.FormulaParser
import org.checkita.utils.ResultUtils._
import org.checkita.utils.Templating.{getTokens, renderTemplate}

import scala.util.Try

/**
 * Expression check calculator. Verifies if boolean expression defined in check
 * evalutes to true (success) or false (failure).
 *
 * @param checkId Check ID
 * @param formula Boolean expression to evaluate
 *                
 * @todo Expression check calculator is quite different from other check calculators and,
 *       therefore, does not conform to basic CheckCalculator class. Thus, implementation
 *       of Expression check calculator requires that most of the abstract methods from
 *       CheckCalculator be overridden to throw UnsupportedOperationException.
 *       In addition, there are plans to deprecate Trend Checks in the future due to
 *       their functionality now is moved (and enhanced) to trend metrics.
 *       Concluding the above, it is quite likely, that check calculators will be refactored
 *       later without changing the external API.
 */
case class ExpressionCheckCalculator(checkId: String, formula: String) extends CheckCalculator with FormulaParser {

  private lazy val formulaMetrics: Seq[String] = getTokens(formula).distinct
  
  override val checkName: CheckName = CheckName.Expression
  override val baseMetric: String = "not-found"
  override val compareMetric: Option[String] = None
  
  override protected def windowString: Option[String] = None

  /**
   * Collect metric results required to resolve check expression.
   * Any errors along this process are collected.
   *
   * @param metricResults Map of metric calculator results
   * @return Map of metric calculator results required for check expression.
   */
  private def collectResultMap(metricResults: MetricResults): Result[Map[String, MetricCalculatorResult]] = 
    Either.cond(
      formulaMetrics.nonEmpty, 
      formulaMetrics, 
      "In order to evaluate expression check formula must refer to at least one metric result."
    ).toResult(Vector(_)).flatMap { metricIds => metricIds.map{ mId =>
        metricResults.get(mId).toRight(s"Unable to find results for metricId '$mId'").toResult(Vector(_)).flatMap{ r =>
          Try {
            require(r.size == 1,
              s"Exactly one metric results is expected to perform expression check, but metric '$mId' yielded ${r.size} results."
            )
            require(!r.head.result.isNaN,
              s"Metric '$mId' yielded NaN result and cannot be used to evaluate check expression."
            )
            r.head
          }.toResult(includeStackTrace = false)
        }
      }.foldLeft(liftToResult(Map.empty[String, MetricCalculatorResult]))(
        (m, r) => m.combine(r)((mm, rr) => mm + (rr.metricId -> rr))
      )
    }
  
  /**
   * Expression checks have completely different evaluation approach and, therefore,
   * this method is unsupported.
   */
  override protected def getDetailsMsg(compareMetricResult: Option[MetricCalculatorResult]): Option[String] =
    throw new UnsupportedOperationException(
      s"Unsupported method call during evaluation of expression check '$checkId'."
    )

  /**
   * Expression checks have completely different evaluation approach and, therefore,
   * this method is unsupported.
   */
  override protected def tryToRun(baseMetricResults: Seq[MetricCalculatorResult], 
                                  compareMetricResults: Option[Seq[MetricCalculatorResult]])
                                 (implicit jobId: String, 
                                  manager: Option[DqStorageManager], 
                                  settings: AppSettings, 
                                  spark: SparkSession, 
                                  fs: FileSystem): CheckCalculatorResult = throw new UnsupportedOperationException(
    s"Unsupported method call during evaluation of expression check '$checkId'."
  )

  /**
   * Expression checks have completely different evaluation approach and, therefore,
   * this method is unsupported.
   */
  override protected def resultOnError(err: Throwable, 
                                       baseMetricResults: Seq[MetricCalculatorResult], 
                                       compareMetricResults: Option[Seq[MetricCalculatorResult]]): CheckCalculatorResult =
    throw new UnsupportedOperationException(
      s"Unsupported method call during evaluation of expression check '$checkId'."
    )

  /**
   * Expression checks have completely different evaluation approach and, therefore,
   * this method is unsupported.
   */
  override protected def resultOnMetricNotFound: CheckCalculatorResult = throw new UnsupportedOperationException(
    s"Unsupported method call during evaluation of expression check '$checkId'."
  )

  /**
   * Callback method that is used when metric results are not found for some of the metric IDs
   * referenced in check expression.
   *
   * @param errors List of metric results lookup errors.
   * @return Check result with Error status and error message.
   */
  private def resultOnError(errors: Vector[String]): CheckCalculatorResult = CheckCalculatorResult(
    checkId,
    checkName.entryName,
    Seq.empty,
    Try(formulaMetrics.head).getOrElse(baseMetric),
    Try(formulaMetrics.tail).getOrElse(Seq.empty),
    None,
    None,
    None,
    status = CalculatorStatus.Error,
    message = (
      s"Evaluation of check '$checkId' for expression '$formula' completed with errors. " +
        s"Result: ${CalculatorStatus.Error.toString}. " +
        s"Unable to perform check due to following errors: ${errors.mkString(" ").replace("\n", "")}"
    ),
    resultType = ResultType.Check
  )

  private def resultOnError(e: Throwable): CheckCalculatorResult = resultOnError(Vector(e.getMessage))
  
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
  override def run(metricResults: MetricResults)
                  (implicit jobId: String,
                   manager: Option[DqStorageManager],
                   settings: AppSettings,
                   spark: SparkSession,
                   fs: FileSystem): CheckCalculatorResult =
    collectResultMap(metricResults) match {
      case Left(errs) => resultOnError(errs)
      case Right(results) => 
        val resMap = results.map{ case (k, v) => k -> v.result.toString }
        val srcIds = results.values.flatMap(_.sourceIds).toSeq
        Try {
          val formulaWithValues = renderTemplate(formula, resMap)
          evalBoolean(formulaWithValues)
        } match {
          case scala.util.Success(result) => 
            val status = if (result) CalculatorStatus.Success else CalculatorStatus.Failure
            val message = s"Evaluation of check '$checkId' for expression '$formula' completed with value '$result'. " +
              s"Result: ${status.toString}"
            CheckCalculatorResult(
              checkId,
              checkName.entryName,
              srcIds,
              Try(formulaMetrics.head).getOrElse(baseMetric),
              Try(formulaMetrics.tail).getOrElse(Seq.empty),
              None,
              None,
              None,
              status = status,
              message = message,
              resultType = ResultType.Check
            )
          case scala.util.Failure(e) => resultOnError(e)
        }
    }
} 
