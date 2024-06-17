package ru.raiffeisen.checkita.core.metrics.composed

import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Results.{MetricCalculatorResult, ResultType}
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.{ErrorRow, MetricErrors}
import ru.raiffeisen.checkita.core.metrics.{ComposedMetric, MetricName}
import ru.raiffeisen.checkita.utils.FormulaParser
import ru.raiffeisen.checkita.utils.Templating.{getTokens, renderTemplate}

import scala.util.Try

/**
 * Takes all metric results and then calculates new ones with formulas
 * @note TopN metric cannot be used in composed metric calculation due to it yields multiple results
 * @param primitiveMetrics Metric results to operate with
 */
case class ComposedMetricCalculator(primitiveMetrics: Seq[MetricCalculatorResult]) extends FormulaParser {

  /**
   * Builds map of metric ID to metric result. Note that result is casted to string
   * in order to build expression for evaluation from formula
   * @return Map of (metricId -> result)
   */
  private def getMetricResultMap: Map[String, String] =
    primitiveMetrics.map(m => m.metricId -> m.result.toString).toMap

  /**
   * Builds map of metric ID to metric source IDs.
   * @return Map of (metricID -> sourceIds)
   */
  private def getMetricSourcesMap: Map[String, Seq[String]] =
    primitiveMetrics.map(m => m.metricId -> m.sourceIds).toMap

  private lazy val metricsResultMap: Map[String, String] = getMetricResultMap
  private lazy val metricSourceMap: Map[String, Seq[String]] = getMetricSourcesMap
  
  /**
   * Replaces metric IDs with their results in formula
   * @param ex formula with metric IDs
   * @return formula with results
   */
  private def replaceMetricsInFormula(ex: String): String = renderTemplate(ex, metricsResultMap)
  
  /**
   * Safely processes composed metric
   * @param ex composed metric to operate with
   * @return composed metric result
   */
  def run(ex: ComposedMetric): MetricCalculatorResult = Try {
    val formulaWithParameters = ex.metricFormula
    val formulaWithValues = replaceMetricsInFormula(formulaWithParameters)
    val result = evalArithmetic(formulaWithValues)
    val sourceIds = getTokens(formulaWithParameters).flatMap(metricSourceMap).distinct
    (result, sourceIds)
  } match { // scala Parsers also have class Success which shadows scala.util.Success
    case scala.util.Success((r, sIds)) =>
      MetricCalculatorResult(
        ex.metricId, MetricName.Composed.entryName, r, None, sIds, Seq.empty, Seq.empty, None, ResultType.ComposedMetric
      )
    case scala.util.Failure(e) =>
      val msgShort = "Metric calculation failed with error."
      val errRow = ErrorRow(
        CalculatorStatus.Error,
        s"Failed to calculate composed metric ${ex.metricId} with following error:\n" + e.getMessage,
        Seq.empty
      )

      MetricCalculatorResult(
        ex.metricId, MetricName.Composed.entryName, Double.NaN, Some(msgShort), 
        Seq.empty, Seq.empty, Seq.empty, Some(MetricErrors(Seq.empty, Seq(errRow))), ResultType.ComposedMetric
      )
  }
}
