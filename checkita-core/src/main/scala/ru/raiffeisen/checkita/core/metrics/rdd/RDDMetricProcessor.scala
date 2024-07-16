package ru.raiffeisen.checkita.core.metrics.rdd

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Results.{MetricCalculatorResult, ResultType}
import ru.raiffeisen.checkita.core.metrics.ErrorCollection._
import ru.raiffeisen.checkita.core.metrics.rdd.regular.AlgebirdRDDMetrics.TopKRDDMetricCalculator
import ru.raiffeisen.checkita.core.metrics.{BasicMetricProcessor, RegularMetric}

/**
 * Base functionality for regular metric processor.
 * The concrete implementation of metric processor differs for batch and streaming applications.
 */
trait RDDMetricProcessor extends BasicMetricProcessor {
  import BasicMetricProcessor._
  import RDDMetricProcessor._

  /** Type in which metric errors are collected */
  type AccType

  /**
   * Creates Spark collection accumulator of required type to collect metric errors and registers it.
   *
   * @param spark    Implicit Spark Session object
   * @return Registered metric errors accumulator
   */
  protected def getAndRegisterErrorAccumulator(implicit spark: SparkSession): CollectionAccumulator[AccType] = {
    val acc = new CollectionAccumulator[AccType]
    spark.sparkContext.register(acc)
    acc
  }

  /**
   * Groups regular metrics by their sequence of columns. 
   * The `*` is expanded to actual list of columns prior grouping metrics.
   * @param metrics Sequence of regular metrics for current source
   * @param allColumns Sequence of all source columns (used to expand `*`)
   * @param caseSensitive  Implicit flag defining whether column names are case sensitive or not.
   * @return Map of sequence of columns to sequence of regular metrics that refer to these columns.
   */
  def getGroupedMetrics(metrics: Seq[RegularMetric], allColumns: Seq[String])
                       (implicit caseSensitive: Boolean): Map[Seq[String], Seq[RegularMetric]] =
    metrics.groupBy { m =>
      val mCols = if (m.metricColumns.size == 1 && m.metricColumns.head == "*") allColumns else m.metricColumns
      if (caseSensitive) mCols else mCols.map(_.toLowerCase)
    }
  
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
  protected def getGroupedCalculators(groupedMetrics: GroupedMetrics): GroupedCalculators = groupedMetrics.map {
    case (columns, metrics) =>
      columns -> metrics.map(m => (m, m.initRDDMetricCalculator))
        .groupBy(_._2)
        .map{ case (k, v) => k -> v.map(_._1) }.toSeq
  }

  /**
   * Updates grouped calculators for given row.
   * @param gc Map of grouped calculators
   * @param row Data row to process
   * @param columnIndexes Map of column names to their indices.
   * @return Updated map of grouped calculators
   */
  protected def updateGroupedCalculators(gc: GroupedCalculators,
                                         row: Row,
                                         columnIndexes: Map[String, Int]): GroupedCalculators =
    gc.map { c =>
      val columnValues: Seq[Any] = c._1.map(columnIndexes).map(row.get)
      val incrementedCalculators = c._2.map {
        case (calc: RDDMetricCalculator, metrics: Seq[RegularMetric]) => (calc.increment(columnValues), metrics)
      }
      (c._1, incrementedCalculators)
    }

  /**
   * Merges two maps of grouped calculators.
   * @param l First map of grouped calculators
   * @param r Second map of grouped calculators
   * @return Merged map of grouped calculators.
   */
  protected def mergeGroupCalculators(l: GroupedCalculators, r: GroupedCalculators): GroupedCalculators =
    l.map { c =>
      val zippedCalculators = r(c._1) zip l(c._1)
      val mergedCalculators = zippedCalculators.map(zc => (zc._1._1.merge(zc._2._1), zc._1._2))
      (c._1, mergedCalculators)
    }

  /**
   * Retrieves errors and failures from grouped calculators, maps it with row data
   * and builds a sequence of metric errors that will be added to error accumulator.
   * @param gc Map of grouped calculators
   * @param row Row for which grouped calculators were just updated.
   * @param columnIndexes Map of column names to their indices
   * @param columnNames Map of column indices to their names.
   * @param sourceKeyIds Column indices that correspond to source key fields
   * @param dumpSize Implicit value of maximum number of metric failure (or errors) to be collected.
   * @return Sequence of metric errors to be accumulated.
   */
  protected def getErrorsFromGroupedCalculators(gc: GroupedCalculators,
                                                row: Row,
                                                columnIndexes: Map[String, Int],
                                                columnNames: Map[Int, String],
                                                sourceKeyIds: Seq[Int])
                                               (implicit dumpSize: Int): Seq[AccumulatedErrors] =
    gc.toSeq.flatMap {
      case (columns: Seq[String], calculators: Seq[(RDDMetricCalculator, Seq[RegularMetric])]) =>
        val statuses: Seq[MetricStatus] = calculators.flatMap {
          case (calculator: RDDMetricCalculator, metrics: Seq[RegularMetric]) =>
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
    }

  /**
   * Processes accumulated metric errors and builds
   * map of metricID to all metric errors with corresponding row data
   *
   * @param errors   Sequence of accumulated metric errors
   * @param dumpSize Implicit value of maximum number of metric failures (or errors) to be collected (per metric).
   *                 Used to prevent OOM errors.
   * @return Map(metricId -> all metric errors)
   */
  protected def processMetricErrors(errors: Seq[AccumulatedErrors])
                                   (implicit dumpSize: Int): Map[String, MetricErrors] = {
    errors.flatMap { err =>
      err.metricStatues.map { s =>
        val errorRow = ErrorRow(s.status, s.message, err.rowData)
        (s.id, err.columnNames, errorRow)
      }
    }.groupBy(_._1).map { case (k, v) =>
      val columns = v.head._2
      // take maximum dumpSize errors and convert to immutable collection:
      val errors = Seq(v.map(_._3).take(dumpSize): _*)
      k -> MetricErrors(columns, errors)
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
   * @param sourceKeys         Source key fields
   * @return Map(metricId -> all metric calculator results).
   *         (Some of the metric calculators yield multiple results)
   */
  protected def buildResults(groupedCalculators: GroupedCalculators,
                             metricErrors: Map[String, MetricErrors],
                             sourceId: String,
                             sourceKeys: Seq[String]): MetricResults = groupedCalculators.toSeq.flatMap {
    case (columns: Seq[String], calculators: Seq[(RDDMetricCalculator, Seq[RegularMetric])]) =>
      calculators.flatMap {
        case (calculator: RDDMetricCalculator, metrics: Seq[RegularMetric]) =>
          metrics.map { metric =>
            if (calculator.isInstanceOf[TopKRDDMetricCalculator])
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
object RDDMetricProcessor {
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
  type GroupedCalculators = Map[Seq[String], Seq[(RDDMetricCalculator, Seq[RegularMetric])]]
}
