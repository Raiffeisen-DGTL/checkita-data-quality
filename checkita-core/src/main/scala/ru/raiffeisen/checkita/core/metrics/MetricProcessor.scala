package ru.raiffeisen.checkita.core.metrics

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.raiffeisen.checkita.core.Results.{MetricCalculatorResult, ResultType}
import ru.raiffeisen.checkita.core.metrics.ErrorCollection._
import ru.raiffeisen.checkita.core.metrics.composed.ComposedMetricCalculator
import ru.raiffeisen.checkita.core.metrics.regular.AlgebirdMetrics.TopKMetricCalculator
import ru.raiffeisen.checkita.core.{CalculatorStatus, Source}
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try

/** Methods to process all regular metrics */
object MetricProcessor {

  /**
   * Type alias for grouped metrics:
   * maps Seq(columns) to Seq(metric)
   */
  type GroupedMetrics = Map[Seq[String], Seq[RegularMetric]]

  /**
   * Type alias for grouped metric calculators:
   * maps Seq(columns) to Seq(calculator, Seq(metric)).
   * Metrics that have the same calculator are grouped together.
   */
  type GroupedCalculators = Map[Seq[String], Seq[(MetricCalculator, Seq[RegularMetric])]]

  /**
   * Type alias for calculated metric results in form of:
   *  - Map of metricId to a sequence of metric results for this metricId (some metrics yield multiple results).
   */
  type MetricResults = Map[String, Seq[MetricCalculatorResult]]

  /**
   * Process all composed metrics given already calculated metrics.
   *
   * @note TopN metric cannot be used in composed metric calculation and will be filtered out.
   * @param composedMetrics Sequence of composed metrics to process
   * @param computedMetrics Sequence of computed metric results
   * @return
   */
  def processComposedMetrics(composedMetrics: Seq[ComposedMetric],
                             computedMetrics: Seq[MetricCalculatorResult]): MetricResults = {


    /**
     * Iterates over composed metric sequence and computes then.
     * Idea here is that previously computed composed metrics can be used as input for the next ones.
     *
     * @param composed Sequence of composed metrics to calculate
     * @param computed Sequence of already computed metrics (both source and composed ones)
     * @param results  Sequence of processed composed metrics
     * @return Sequence of composed metric results
     */
    @tailrec
    def loop(composed: Seq[ComposedMetric],
             computed: Seq[MetricCalculatorResult],
             results: Seq[MetricCalculatorResult] = Seq.empty): Seq[MetricCalculatorResult] = {
      if (composed.isEmpty) results
      else {
        val calculator = ComposedMetricCalculator(computed)
        val processedMetric = calculator.run(composed.head)
        loop(composed.tail, computed :+ processedMetric, results :+ processedMetric)
      }
    }

    loop(composedMetrics, computedMetrics.filterNot(_.metricName.startsWith(MetricName.TopN.entryName)))
      .groupBy(_.metricId)
  }


  /**
   * Process all metrics for a given source
   *
   * @param source        Source to process metrics for
   * @param sourceMetrics Sequence of metrics defined for the given source
   * @param spark         Implicit Spark Session object
   * @param dumpSize      Implicit value of maximum number of metric failure (or errors) to be collected
   *                      (per metric and per partition)
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
      s"Some of key fields were not found for source '${source.id}'. Please, check them."
    )

    val metricsByColumns = sourceMetrics.groupBy(
      m => if (caseSensitive) m.metricColumns else m.metricColumns.map(_.toLowerCase)
    )
    val groupedCalculators = getGroupedCalculators(metricsByColumns)

    // register metric error accumulator:
    val metricErrorAccumulator = ErrorAccumulator(mutable.ArrayBuffer.empty[AccumulatedErrors])
    spark.sparkContext.register(metricErrorAccumulator)

    val processedCalculators = processGroupedCalculators(
      df, groupedCalculators, columnIndexes, columnNames, sourceKeyIds, metricErrorAccumulator, dumpSize
    )

    val processedMetricErrors = processMetricErrors(metricErrorAccumulator)

    buildResults(processedCalculators, processedMetricErrors, source.id, sourceKeys)

  }.toResult(preMsg = s"Unable to process metrics for source ${source.id} due to following error:")

  /**
   * Builds map column name -> column index for given dataframe
   *
   * @param df Spark Dataframe
   * @return Map(column name -> column index)
   */
  private def getColumnIndexMap(df: DataFrame): Map[String, Int] =
    df.schema.fieldNames.map(s => s -> df.schema.fieldIndex(s)).toMap

  /**
   * Builds map column index -> column name for given dataframe
   *
   * @param df Spark Dataframe
   * @return Map(column index -> column name)
   */
  private def getColumnNamesMap(df: DataFrame): Map[Int, String] =
    df.schema.fieldNames.map(s => df.schema.fieldIndex(s) -> s).toMap

  /**
   * Build grouped calculator collection. Main idea here is following:
   *
   * @example You want to obtain multiple quantiles for a specific column,
   *          but calling a new instance of tdigest for each metric isn't effective.
   *          To avoid that first, we're mapping all metric to their calculator classes,
   *          then we are grouping them by the same instances of calculator classes
   *          (taking credit of all calculators being CASE classes).
   * @example "FIRST_QUANTILE" for column "A" with parameter "accuracyError=0.0001"
   *          will require an instance of TDigestMetricCalculator. "MEDIAN_VALUE" for column "A" with
   *          the same parameter "accuracyError=0.0001" will also require an instance of TDigestMetricCalculator.
   *          In our approach the instance will be the same and it will return us results like
   *          Map(("MEDIAN_VALUE:..."->result1),("FIRST_QUANTILE:..."->result2),...)
   *
   *          So in the end we are initializing only unique calculators.
   * @param groupedMetrics Collection of source metrics grouped by their list of columns.
   * @return Collection of grouped metric calculators:
   *         Map(
   *         Seq(columns) -> Seq(
   *         (MetricCalculator, Seq(SourceMetric))
   *         )
   *         )
   */
  private def getGroupedCalculators(groupedMetrics: GroupedMetrics): GroupedCalculators = groupedMetrics.map {
    case (columns, metrics) =>
      columns -> metrics.map(m => (m, m.initMetricCalculator)).groupBy(_._2).mapValues(_.map(_._1)).toSeq
  }

  /**
   * Processes grouped metric calculators. To calculate metrics we are using three-step processing:
   *   - Iterating over RDD and passing values to the calculators
   *   - Updating partition calculators before merging (operations like trimming, shifting, etc)
   *   - Reducing (merging partition calculator)
   *
   * @param df                 Spark dataframe with source data
   * @param groupedCalculators Collection of grouped calculators
   * @param columnIndexes      Map of column names to column indices
   * @param columnNames        Map of column indices to column names
   * @param sourceKeyIds       Sequence of source key fields column indices
   * @param errorAccumulator   Accumulator to collect metric failures (or errors)
   * @param dumpSize           Limits the number of failures (or errors) to be collected (preventing OOM errors)
   * @return Processed collection of grouped metric calculators.
   */
  private def processGroupedCalculators(df: DataFrame,
                                        groupedCalculators: GroupedCalculators,
                                        columnIndexes: Map[String, Int],
                                        columnNames: Map[Int, String],
                                        sourceKeyIds: Seq[Int],
                                        errorAccumulator: ErrorAccumulator,
                                        dumpSize: Int): GroupedCalculators =
    df.rdd.treeAggregate(groupedCalculators)(
      seqOp = {
        case (groupedCalculators: GroupedCalculators, row: Row) =>

          // update calculators:
          val updatedCalculators: GroupedCalculators = groupedCalculators.map { c =>
            val columnValues: Seq[Any] = c._1.map(columnIndexes).map(row.get)
            val incrementedCalculators = c._2.map {
              case (calc: MetricCalculator, metrics: Seq[RegularMetric]) => (calc.increment(columnValues), metrics)
            }
            (c._1, incrementedCalculators)
          }

          // collect errors:
          updatedCalculators.toSeq.flatMap {
            case (columns: Seq[String], calculators: Seq[(MetricCalculator, Seq[RegularMetric])]) =>
              val statuses: Seq[MetricStatus] = calculators.flatMap {
                case (calculator: MetricCalculator, metrics: Seq[RegularMetric]) =>
                  if (calculator.getStatus != CalculatorStatus.Success && calculator.getFailCounter <= dumpSize) {
                    metrics.map(m => MetricStatus(m.metricId, calculator.getStatus, calculator.getFailMessage))
                  } else Seq.empty[MetricStatus]
              }
              if (statuses.nonEmpty) {
                val colIds = sourceKeyIds ++ columns.map(columnIndexes).filterNot(sourceKeyIds.contains)
                val colNames = colIds.map(columnNames)
                val colValues = colIds.map(id => if (row.isNullAt(id)) "" else row.get(id)).map(_.toString)
                Seq(AccumulatedErrors(colNames, statuses, colValues))
              } else Seq.empty[AccumulatedErrors]
          }.foreach(errorAccumulator.add)

          // yield updated calculators:
          updatedCalculators
      },
      combOp = (r, l) => l.map { c =>
        val zippedCalculators = r(c._1) zip l(c._1)
        val mergedCalculators = zippedCalculators.map(zc => (zc._1._1.merge(zc._2._1), zc._1._2))
        (c._1, mergedCalculators)
      }
    )

  /**
   * Processes accumulated metric errors and builds 
   * map of metricID to all metric errors with corresponding row data
   *
   * @param errorAccumulator Metric error accumulator at its final state (after all metric being processed)
   * @return Map(metricId -> all metric errors)
   */
  private def processMetricErrors(errorAccumulator: ErrorAccumulator): Map[String, MetricErrors] = {
    errorAccumulator.value.flatMap { err =>
      err.metricStatues.map { s =>
        val errorRow = ErrorRow(s.status, s.message, err.rowData)
        (s.id, err.columnNames, errorRow)
      }
    }.groupBy(_._1).mapValues { t =>
      val columns = t.head._2
      val errors = Seq(t.map(_._3): _*) // convert to immutable collection
      MetricErrors(columns, errors)
    }
  }

  /**
   * Build results out of metric calculator result and metric errors.
   * TopN metric yields N-results (requested number of topN values), therefore,
   * for this metric we collect sequence of result with metric name of format: TOP_N_i,
   * where i is the number of top value.
   * For other metrics there is always a single result which correspond to metric name.
   *
   * @param groupedCalculators Processed grouped metric calculators
   * @param metricErrors       Processed metric errors
   * @param sourceId           Id of the source for which metrics are being calculated
   * @param sourceKeys          Source key fields
   * @return Map(metricId -> all metric calculator results).
   *         (Some of the metric calculators yield multiple results)
   */
  private def buildResults(groupedCalculators: GroupedCalculators,
                           metricErrors: Map[String, MetricErrors],
                           sourceId: String,
                           sourceKeys: Seq[String]): MetricResults = groupedCalculators.toSeq.flatMap {
    case (columns: Seq[String], calculators: Seq[(MetricCalculator, Seq[RegularMetric])]) =>
      calculators.flatMap {
        case (calculator: MetricCalculator, metrics: Seq[RegularMetric]) =>
          metrics.map { metric =>
            if (calculator.isInstanceOf[TopKMetricCalculator])
              metric.metricId -> calculator.result().toSeq.map { r =>
                MetricCalculatorResult(
                  metric.metricId,
                  r._1,
                  r._2._1,
                  r._2._2,
                  Seq(sourceId),
                  sourceKeys,
                  columns,
                  metricErrors.get(metric.metricId),
                  ResultType.RegularMetric
                )
              }
            else {
              val result = calculator.result()(metric.metricName.entryName)
              metric.metricId -> Seq(MetricCalculatorResult(
                metric.metricId,
                metric.metricName.entryName,
                result._1,
                result._2,
                Seq(sourceId),
                sourceKeys,
                columns,
                metricErrors.get(metric.metricId),
                ResultType.RegularMetric
              ))
            }
          }
      }
  }.toMap
}
