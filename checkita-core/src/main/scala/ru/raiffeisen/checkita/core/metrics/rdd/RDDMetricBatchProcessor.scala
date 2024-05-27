package ru.raiffeisen.checkita.core.metrics.rdd

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import ru.raiffeisen.checkita.core.Source
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.AccumulatedErrors
import ru.raiffeisen.checkita.core.metrics.{BasicMetricProcessor, RegularMetric}
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.collection.JavaConverters._
import scala.util.Try

/** Regular metrics processor for Batch Applications */
object RDDMetricBatchProcessor extends RDDMetricProcessor {

  import BasicMetricProcessor._
  import RDDMetricProcessor._

  /** Type in which metric errors are collected  */
  type AccType = AccumulatedErrors

  /**
   * Process all metrics for a given source.
   * Metrics are grouped by their columns list and metric calculators are initialized.
   * Then, to calculate metrics we are using three-step processing:
   *   - Iterating over RDD and passing values to the calculators
   *   - Updating partition calculators before merging (operations like trimming, shifting, etc)
   *   - Reducing (merging partition calculator)
   *
   * @param source        Source to process metrics for
   * @param sourceMetrics Sequence of metrics defined for the given source
   * @param spark         Implicit Spark Session object
   * @param dumpSize      Implicit value of maximum number of metric failure (or errors) to be collected
   *                      (per metric and per partition). Used to prevent OOM errors.
   * @param caseSensitive Implicit flag defining whether column names are case sensitive or not.
   * @return Map of metricId to a sequence of metric results for this metricId (some metrics yield multiple results).
   */
  def processRegularMetrics(source: Source,
                            sourceMetrics: Seq[RegularMetric])
                           (implicit spark: SparkSession,
                            dumpSize: Int,
                            caseSensitive: Boolean): Result[MetricResults] = Try {

    val df = if (caseSensitive) source.df else
      source.df.select(source.df.columns.map(c => col(c).as(c.toLowerCase)): _*)

    val columnIndexes = getColumnIndexMap(df)
    val columnNames = getColumnNamesMap(df)
    val sourceKeys = source.keyFields
    val sourceKeyIds = sourceKeys.flatMap(columnIndexes.get)

    assert(
      sourceKeys.size == sourceKeyIds.size,
      s"Some of key fields were not found for source '${source.id}'. " +
      "Following keyFields are not found within source columns: " +
        sourceKeys.filterNot(columnIndexes.contains).mkString("[`", "`, `", "`]")
    )

    val metricsByColumns = sourceMetrics.groupBy(
      m => if (caseSensitive) m.metricColumns else m.metricColumns.map(_.toLowerCase)
    )
    val groupedCalculators = getGroupedCalculators(metricsByColumns)

    // get and register metric error accumulator:
    val metricErrorAccumulator = getAndRegisterErrorAccumulator

    val processedCalculators = df.rdd.treeAggregate(groupedCalculators)(
      seqOp = {
        case (gc: GroupedCalculators, row: Row) =>
          // update calculators:
          val updatedCalculators: GroupedCalculators = updateGroupedCalculators(gc, row, columnIndexes)
          // collect errors:
          getErrorsFromGroupedCalculators(updatedCalculators, row, columnIndexes, columnNames, sourceKeyIds)
            .foreach(metricErrorAccumulator.add)
          // yield updated calculators:
          updatedCalculators
      },
      combOp = mergeGroupCalculators
    )

    val processedMetricErrors = processMetricErrors(metricErrorAccumulator.value.asScala)

    buildResults(processedCalculators, processedMetricErrors, source.id, sourceKeys)

  }.toResult(preMsg = s"Unable to process metrics for source ${source.id} due to following error:")
}
