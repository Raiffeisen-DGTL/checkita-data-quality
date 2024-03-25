package ru.raiffeisen.checkita.core.dfmetrics

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.col
import ru.raiffeisen.checkita.core.Results.{MetricCalculatorResult, ResultType}
import ru.raiffeisen.checkita.core.{CalculatorStatus, Source}
import ru.raiffeisen.checkita.core.dfmetrics.Helpers._
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.{ErrorRow, MetricErrors}
import ru.raiffeisen.checkita.core.metrics.MetricProcessor.MetricResults
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.collection.mutable
import scala.util.Try

object MetricProcessor {

  /**
   * Builds map column name -> column index for given dataframe
   *
   * @param df Spark Dataframe
   * @return Map(column name -> column index)
   */
  protected def getColumnIndexMap(df: DataFrame): Map[String, Int] =
    df.schema.fieldNames.map(s => s -> df.schema.fieldIndex(s)).toMap

  /**
   * Builds map column index -> column name for given dataframe
   *
   * @param df Spark Dataframe
   * @return Map(column index -> column name)
   */
  protected def getColumnNamesMap(df: DataFrame): Map[Int, String] =
    df.schema.fieldNames.map(s => df.schema.fieldIndex(s) -> s).toMap

  def processRegularMetrics(source: Source,
                            sourceMetrics: Seq[DFRegularMetric])
                           (implicit dumpSize: Int,
                            caseSensitive: Boolean): Result[MetricResults] = Try {

    val df = if (caseSensitive) source.df else
      source.df.select(source.df.columns.map(c => col(c).as(c.toLowerCase)): _*)

    implicit val keyFields: Seq[String] = source.keyFields

    val missedKeyFields: Seq[String] = keyFields.filterNot(df.columns.contains)

    assert(
      missedKeyFields.isEmpty,
      s"Some of key fields were not found for source '${source.id}'. " +
        "Following keyFields are not found within source columns: " +
        missedKeyFields.mkString("[`", "`, `", "`]")
    )

    val metricCalculators: Map[String, DFMetricCalculator] =
      sourceMetrics.map(m => m.metricId -> m.initDFMetricCalculator).toMap

    val metricIdWithCols: Seq[(String, (String, String))] = sourceMetrics.map { m =>
      val id = m.metricId
      id -> (
        addColumnSuffix(id, DFMetricOutput.Result.entryName),
        addColumnSuffix(id, DFMetricOutput.Errors.entryName)
      )
    }

    val aggregationColumns: Seq[Column] = metricCalculators.values.flatMap(c => Seq(c.result, c.errors)).toSeq

    val processedDf = df.select(aggregationColumns: _*)

    val resultColumnIndexes = getColumnIndexMap(processedDf)
    val resultColumns = resultColumnIndexes.filterKeys(
      k => k.endsWith(DFMetricOutput.Result.entryName) || k.endsWith(DFMetricOutput.Result.entryName + "`")
    )
    val errorsColumns = resultColumnIndexes.filterKeys(
      k => k.endsWith(DFMetricOutput.Errors.entryName) || k.endsWith(DFMetricOutput.Errors.entryName + "`")
    )

    val calculatorResults: Array[Row] = processedDf.collect()

    val metricResults = calculatorResults.flatMap { row =>
      metricIdWithCols.map { m =>
        val result = row.get(resultColumns(m._2._1)).asInstanceOf[Double]
        val errors: Seq[mutable.WrappedArray[String]] = row.get(errorsColumns(m._2._2))
          .asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[String]]]
        m._1 -> (result, errors)
      }
      // metricId -> (result, errors)
    }.toMap

    sourceMetrics.map{ metric =>
      val allColumns = (keyFields ++ metric.metricColumns).distinct
      val errMsg = metricCalculators(metric.metricId).errorMessage
      val errors: Option[MetricErrors] = metricResults(metric.metricId)._2 match {
        case errs if errs.nonEmpty => Some(MetricErrors(
          columns = allColumns,
          errors = errs.map(e => ErrorRow(CalculatorStatus.Failure, errMsg, e))
        ))
        case _ => None
      }

      metric.metricId -> Seq(MetricCalculatorResult(
        metric.metricId,
        metric.metricName.entryName,
        metricResults(metric.metricId)._1,
        None,
        Seq(source.id),
        keyFields,
        metric.metricColumns,
        errors,
        ResultType.RegularMetric
      ))
    }.toMap
  }.toResult(preMsg = s"Unable to process metrics for source ${source.id} due to following error:")
}
