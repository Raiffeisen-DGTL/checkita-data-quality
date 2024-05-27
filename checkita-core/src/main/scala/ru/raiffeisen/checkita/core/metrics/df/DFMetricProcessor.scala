package ru.raiffeisen.checkita.core.metrics.df

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import ru.raiffeisen.checkita.core.Results.{MetricCalculatorResult, ResultType}
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.{ErrorRow, MetricErrors}
import ru.raiffeisen.checkita.core.metrics.df.Helpers.{DFMetricOutput, addColumnSuffix}
import ru.raiffeisen.checkita.core.metrics.df.regular.ApproxCardinalityDFMetrics.TopNDFMetricCalculator
import ru.raiffeisen.checkita.core.metrics.{BasicMetricProcessor, MetricName, RegularMetric}
import ru.raiffeisen.checkita.core.{CalculatorStatus, Source}
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.collection.mutable
import scala.util.Try

/**
 * Regular DF metrics processor. Works for batch applications only.
 */
object DFMetricProcessor extends BasicMetricProcessor {
  import BasicMetricProcessor._

  /**
   * Type alias for DFMetricalculator results:
   * Map of metricId -> (Seq((result, additionalResult), Seq(errors))
   */
  type CalculatorResults = Map[String, (Seq[(Double, Option[String])], Seq[mutable.WrappedArray[String]])]

  /**
   * Runs all single-pass DF metric calculators.
   *
   * @param df          Source dataframe for which metrics are calculated.
   * @param calculators Sequence of single-pass DF metric calculators to be run over this source.
   * @param dumpSize    Implicit value of maximum number of metric failure (or errors) 
   *                    to be collected per metric. Used to prevent OOM errors.
   * @param keyFields   Source key fields.
   * @return Spark DataFrame containing results of single-pass metric calculators.
   */
  private def runSinglePassCalculators(df: DataFrame,
                                       calculators: Seq[DFMetricCalculator])
                                      (implicit dumpSize: Int,
                                       keyFields: Seq[String]): DataFrame =
    df.select(calculators.flatMap(c => Seq(c.result, c.errors)): _*)

  /**
   * Run grouping DF metric calculators that are applied to the same sequence of columns.
   * Such grouping calculators can be run together.
   *
   * @param df           Source dataframe for which metrics are calculated.
   * @param groupColumns Sequence of grouping columns.
   * @param calculators  Sequence of grouping DF metric calculators to be run over this source.
   * @param dumpSize     Implicit value of maximum number of metric failure (or errors) 
   *                     to be collected per metric. Used to prevent OOM errors.
   * @param keyFields    Source key fields.
   * @return Spark DataFrame containing results of grouping metric calculators.
   */
  private def runGroupingCalculators(df: DataFrame,
                                     groupColumns: Seq[String],
                                     calculators: Seq[GroupingDFMetricCalculator])
                                    (implicit dumpSize: Int,
                                     keyFields: Seq[String]): DataFrame = {
    assert(
      calculators.forall(c => c.columns == groupColumns),
      "All grouping calculators within a group must have the same list of columns: " +
        groupColumns.mkString("[", ",", "]")
    )

    val aggExpr = calculators.flatMap(c => Seq(c.groupResult, c.groupErrors))
    val resultExpr = calculators.flatMap(c => Seq(c.result, c.errors))

    df.groupBy(groupColumns.map(col): _*)
      .agg(aggExpr.head, aggExpr.tail: _*)
      .select(resultExpr: _*)
  }

  /**
   * Process Spark DataFrame containing DF metric calculators' results and retrieves those results for
   * further finalization into MetricCalculatorResult instances.
   *
   * @param processedDF             Spark DataFrame contains DF metric calculators' results.
   * @param calculatorResultColumns Map containing names of columns containing result 
   *                                and errors for each of the processed metric.
   * @return Map of metricId to metric calculator results and errors.
   */
  private def getCalculatorResults(processedDF: DataFrame,
                                   calculatorResultColumns: Map[String, (String, String, Boolean)]): CalculatorResults = {

    val processedColumnIndexes = getColumnIndexMap(processedDF)
    val resultColumns = processedColumnIndexes.filterKeys(
      k => k.endsWith(DFMetricOutput.Result.entryName) || k.endsWith(DFMetricOutput.Result.entryName + "`")
    )
    val errorsColumns = processedColumnIndexes.filterKeys(
      k => k.endsWith(DFMetricOutput.Errors.entryName) || k.endsWith(DFMetricOutput.Errors.entryName + "`")
    )

    val processedResults: Array[Row] = processedDF.collect()

    (for {
      row <- processedResults
      calcResCols <- calculatorResultColumns
    } yield {
      val errors = row.get(errorsColumns(calcResCols._2._2))
        .asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[String]]]
      val results = if (calcResCols._2._3) row.get(resultColumns(calcResCols._2._1))
        .asInstanceOf[mutable.WrappedArray[Row]]
        .map(r => r.getAs[Double]("frequency") -> r.getAs[String]("value"))
        .sortBy(-_._1)
        .map {
          case (frequency, value) => frequency -> Some(value)
        }
      else Seq(row.get(resultColumns(calcResCols._2._1)).asInstanceOf[Double] -> None)

      calcResCols._1 -> (results, errors)
    }).toMap
  }

  /**
   * Builds final map of metric calculators results.
   *
   * @param results     Calculator results and errors.
   * @param calculators DF metric calculators that were processed.
   * @param sourceId    Source ID for which metrics were processed.
   * @param keyFields   Source key fields.
   * @return Map of metricId to a sequence of metric results for this metricId 
   *         (some metrics yield multiple results).
   */
  private def buildMetricResults(results: CalculatorResults,
                                 calculators: Map[String, DFMetricCalculator],
                                 sourceId: String)
                                (implicit keyFields: Seq[String]): MetricResults = results.map {
    case (metricId, (metricResults, metricErrors)) =>
      val calculator = calculators(metricId)
      val allColumns = (keyFields ++ calculator.columns).distinct
      val errMsg = calculator.errorMessage
      val errors: Option[MetricErrors] = metricErrors match {
        case errs if errs.nonEmpty => Some(MetricErrors(
          columns = allColumns,
          errors = errs.map(e => ErrorRow(CalculatorStatus.Failure, errMsg, e))
        ))
        case _ => None
      }
      metricId -> metricResults.zipWithIndex.map {
        case ((result, additionalResult), idx) => 
          val metricName = if (calculator.metricName == MetricName.TopN) {
            calculator.metricName.entryName + "_" + (idx + 1)
          } else calculator.metricName.entryName
          
          MetricCalculatorResult(
            metricId,
            metricName,
            result,
            additionalResult,
            Seq(sourceId),
            keyFields,
            calculator.columns,
            errors,
            ResultType.RegularMetric
          )
      }
  }

  /**
   * Process all metrics for a given source using DataFrame metric calculators.
   *   - Single-pass and grouping metric calculators are processed separately.
   *   - Grouping calculators are combined per their list of columns.
   *   - After all calculators have finished their computation, metric results are build.
   *
   * @param source        Source to process metrics for
   * @param sourceMetrics Sequence of metrics defined for the given source
   * @param dumpSize      Implicit value of maximum number of metric failure (or errors) to be collected
   *                      (per metric and per partition). Used to prevent OOM errors.
   * @param caseSensitive Implicit flag defining whether column names are case sensitive or not.
   * @return Map of metricId to a sequence of metric results for this metricId (some metrics yield multiple results).
   */
  def processRegularMetrics(source: Source,
                            sourceMetrics: Seq[RegularMetric])
                           (implicit dumpSize: Int,
                            caseSensitive: Boolean): Result[MetricResults] = Try{

    implicit val sourceKeys: Seq[String] = source.keyFields

    val df = if (caseSensitive) source.df else
      source.df.select(source.df.columns.map(c => col(c).as(c.toLowerCase)): _*)

    val columnIndexes = getColumnIndexMap(df)
    val sourceKeyIds = sourceKeys.flatMap(columnIndexes.get)

    assert(
      sourceKeys.size == sourceKeyIds.size,
      s"Some of key fields were not found for source '${source.id}'. " +
        "Following keyFields are not found within source columns: " +
        sourceKeys.filterNot(columnIndexes.contains).mkString("[`", "`, `", "`]")
    )

    // Here we traverse sequence of regular metrics in order to split them into three collections:
    //   1) Map of single-pass metric.
    //   2) Map of grouping metrics combined per their list of columns.
    //   3) Map of metricID to calculators result and errors columns to be collected from processed dataframes.
    //      Also add boolean flag to indicate if calculator is TopN calculator:
    //      this calculator yields multiple results and its results are processed in a specific way.
    val (singlePassCalculators, groupedCalculators, calculatorResultColumns) = sourceMetrics.foldLeft((
      Map.empty[String, DFMetricCalculator],
      Map.empty[Seq[String], Map[String, GroupingDFMetricCalculator]],
      Map.empty[String, (String, String, Boolean)]
    )){
      case (acc, metric) =>
        val calculator = metric.initDFMetricCalculator
        val updatedCalcResCols = acc._3 + (calculator.metricId -> (
          addColumnSuffix(calculator.metricId, DFMetricOutput.Result.entryName),
          addColumnSuffix(calculator.metricId, DFMetricOutput.Errors.entryName),
          calculator.isInstanceOf[TopNDFMetricCalculator]
        ))
        calculator match {
          case groupingCalculator: GroupingDFMetricCalculator =>
            val currentGroupCalculators = acc._2.getOrElse(groupingCalculator.columns, Map.empty)
            val updatedGroupCalculators = currentGroupCalculators + (metric.metricId -> groupingCalculator)
            (acc._1, acc._2.updated(groupingCalculator.columns, updatedGroupCalculators), updatedCalcResCols)
          case singlePassCalculator: DFMetricCalculator =>
            (acc._1 + (metric.metricId -> singlePassCalculator), acc._2, updatedCalcResCols)
        }
    }

    val allCalculators = singlePassCalculators ++ groupedCalculators.values.flatten.toMap
    // run single-pass calculators:
    val singlePassResultDF = if (singlePassCalculators.isEmpty) Seq.empty
      else Seq(runSinglePassCalculators(df, singlePassCalculators.values.toSeq))
    // run all grouping calculators:
    val groupedResultsDFs = groupedCalculators.map{
      case (columns, groupCalculators) => runGroupingCalculators(df, columns, groupCalculators.values.toSeq)
    }.toSeq
    
    assert(
      singlePassResultDF.nonEmpty || groupedResultsDFs.nonEmpty,
      s"There are no calculator results produced for source '${source.id}''"
    )
    
    // Each of the processed dataframe will contain a single row with multiple columns containing
    // metric result and collected errors.
    // In order to retrieve all metric calculator results all dataframes are concatenated into a single one.
    // As all dataframes contains single row, it is safe to use cross-join to concatenate them.
    val joinedResultDF = (singlePassResultDF ++ groupedResultsDFs).reduce((resDF, curDF) => resDF.crossJoin(curDF))
    
    // retrieve calculator results from processed dataframe:
    val metricResults = getCalculatorResults(joinedResultDF, calculatorResultColumns)

    // build final results:
    buildMetricResults(metricResults, allCalculators, source.id)

  }.toResult(preMsg = s"Unable to process metrics for source ${source.id} due to following error:")
}

