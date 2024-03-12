package ru.raiffeisen.checkita.core.metrics

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.raiffeisen.checkita.config.appconf.StreamConfig
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.AccumulatedErrors
import ru.raiffeisen.checkita.utils.Logging
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.util.Try

object MetricStreamProcessor extends MetricProcessor with Logging {

  import MetricProcessor._

  /**
   * Type in which metric errors are collected:
   * links errors to a streaming window to which record belongs that has produced these errors
   */
  type AccType = (Long, AccumulatedErrors)

  /**
   * Type alias for micro-batch state used in streaming applications only.
   * State includes following:
   *   - Maximum observed event timestamp
   *   - Map of grouped calculators per each window in a stream.
   *
   * @note window is identified by its start time as unix epoch (in seconds).
   */
  type MicroBatchState = (Long, Map[Long, GroupedCalculators])

  /**
   * Processor state buffer used in streaming applications.
   * Holds following metric calculation state:
   *   - current watermarks per each stream that is being processed.
   *   - state of metric calculators per each stream and each processing window withing a stream.
   *   - metric error accumulators per each stream and each processing window withing a stream.
   *
   * @param watermarks  Watermarks per each stream.
   * @param calculators Calculators state per each stream and each window.
   * @param errors      Metric errors per each stream and each window.
   * @note Scala concurrent Trie Map is used to store results in a buffer as it is a thread safe and
   *       allows lock free access to its contents.
   */
  case class ProcessorBuffer(
                              watermarks: concurrent.TrieMap[String, Long],
                              calculators: concurrent.TrieMap[(String, Long), GroupedCalculators],
                              errors: concurrent.TrieMap[(String, Long), Seq[AccumulatedErrors]]
                            )
  object ProcessorBuffer {
    /**
     * Creates empty processor buffer provided with sequence of streams to be processed.
     * @param streamIds Stream Ids to be processed.
     * @return Empty processor buffer
     * @note Empty processor buffer has its watermarks initialized with long minimum value
     */
    def init(streamIds: Seq[String]): ProcessorBuffer = {
      val initWatermarks = concurrent.TrieMap.empty[String, Long]
      streamIds.foreach(initWatermarks.update(_, Long.MinValue))
      ProcessorBuffer(
        initWatermarks,
        concurrent.TrieMap.empty[(String, Long), GroupedCalculators],
        concurrent.TrieMap.empty[(String, Long), Seq[AccumulatedErrors]]
      )
    }
  }


  /**
   * Processes all regular metrics for micro-batch of the given stream.
   *
   * @param streamId      Stream ID which micro-batch is processed.
   * @param streamKeys    Stream source key fields.
   * @param streamMetrics Sequence of metrics defined for the given stream source.
   * @param spark         Implicit Spark Session object.
   * @param buffer        Implicit processor buffer.
   * @param streamConf    Streaming application settings
   * @param dumpSize      Implicit value of maximum number of metric failure (or errors) to be collected
   *                      (per metric and per partition)
   * @param caseSensitive Implicit flag defining whether column names are case sensitive or not.
   * @return Does not return results but updates processor buffer instead.
   */
  def processRegularMetrics(streamId: String,
                            streamKeys: Seq[String],
                            streamMetrics: Seq[RegularMetric])
                           (implicit spark: SparkSession,
                            buffer: ProcessorBuffer,
                            streamConf: StreamConfig,
                            dumpSize: Int,
                            caseSensitive: Boolean): (DataFrame, Long) => Unit = (batchDf, batchId) =>
    if (batchDf.isEmpty)
      log.info(s"[STREAM '$streamId'] Skipping metric calculation for empty batch (batchId = $batchId).")
    else {
      val watermark = buffer.watermarks.getOrElse(streamId, throw new NoSuchElementException(
        s"Watermark for stream '$streamId' not found."
      ))

      val df = if (caseSensitive) batchDf else
        batchDf.select(batchDf.columns.map(c => col(c).as(c.toLowerCase)): _*)

      val columnIndexes = getColumnIndexMap(df)
      val columnNames = getColumnNamesMap(df)
      val streamKeyIds = streamKeys.flatMap(columnIndexes.get)

      assert(
        streamKeys.size == streamKeyIds.size,
        s"Some of key fields were not found for stream '$streamId' in batch $batchId. " +
        "Following keyFields are not found within stream columns: " +
        streamKeys.filterNot(columnIndexes.contains).mkString("[`", "`, `", "`]") +
        s". Please ensure that keyFields are always exist within streamed messages."
      )

      val metricsByColumns = streamMetrics.groupBy(
        m => if (caseSensitive) m.metricColumns else m.metricColumns.map(_.toLowerCase)
      )

      // get and register metric error accumulator:
      val metricErrorAccumulator = getAndRegisterErrorAccumulator

      val initState: MicroBatchState = (0L, Map.empty)
      val updatedState = df.rdd.treeAggregate(initState)(
        seqOp = {
          case (state: MicroBatchState, row: Row) =>
            val rowEventTime = row.get(columnIndexes(streamConf.eventTsCol)).asInstanceOf[Long]
            val rowWindowStart = row.get(columnIndexes(streamConf.windowTsCol)).asInstanceOf[Long]

            // getting either existing calculators and accumulator for this window or initializing them:
            val groupedCalculators = state._2.getOrElse(rowWindowStart, getGroupedCalculators(metricsByColumns))

            // update calculators:
            val updatedCalculators: GroupedCalculators = if (rowEventTime > watermark)
              updateGroupedCalculators(groupedCalculators, row, columnIndexes)
            else {
              log.info(s"[STREAM '$streamId'] Records is late to watermark of $watermark: $row")
              groupedCalculators
            }

            // collect errors:
            getErrorsFromGroupedCalculators(updatedCalculators, row, columnIndexes, columnNames, streamKeyIds)
              .foreach(err => metricErrorAccumulator.add(rowWindowStart, err))

            // yield updated state:
            math.max(state._1, rowEventTime) -> state._2.updated(rowWindowStart, updatedCalculators)
        },
        combOp = (l, r) => {
          val maxEventTime = math.max(l._1, r._1)

          val mergedState = (l._2.toSeq ++ r._2.toSeq)
            .groupBy(_._1)
            .mapValues(s => s.map(_._2))
            .mapValues(sq => sq.reduce((lt, rt) => mergeGroupCalculators(lt, rt)))
            .map(identity)

          maxEventTime -> mergedState
        }
      )

      // update buffer:
      buffer.watermarks.update(streamId, math.max(watermark, updatedState._1 - streamConf.watermark.toSeconds))
      updatedState._2.foreach {
        case (window, state) =>
          val bufferedCalculators = buffer.calculators.get((streamId, window))
          val updatedCalculators = bufferedCalculators
            .map(bc => mergeGroupCalculators(bc, state))
            .getOrElse(state) // if buffer doesn't contain results for this window, yield newly computed ones.
          buffer.calculators.update((streamId, window), updatedCalculators)
      }

      // update errors:
      metricErrorAccumulator.value.asScala
        .groupBy(t => t._1)
        .mapValues(v => v.map(_._2))
        .foreach {
          case (window, errors) =>
            val bufferedErrors = buffer.errors.get((streamId, window))
            val updatedErrors = bufferedErrors.map(be => be ++ errors).getOrElse(errors)
            buffer.errors.update((streamId, window), updatedErrors)
        }
      metricErrorAccumulator.reset()

      log.info(s"[STREAM '$streamId'] Regular metrics are processed for non-empty batch (batchId = $batchId).")
    }


  /**
   * Processes calculator results and accumulated errors once stream window state is finalized.
   * @param gc Final map of grouped metric calculators for stream window
   * @param errors Final sequence of metric errors accumulated for stream window
   * @param windowStart Window start datetime (string in format 'yyyy-MM-dd HH:mm:ss')
   * @param streamId Id of the stream source for which results are processed.
   * @param streamKeys Stream source key fields.
   * @return Map of metricId to a sequence of metric results for this metricId (some metrics yield multiple results).
   */
  def processWindowResults(gc: GroupedCalculators,
                           errors: Seq[AccumulatedErrors],
                           windowStart: String,
                           streamId: String,
                           streamKeys: Seq[String]): Result[MetricResults] = Try {
    buildResults(gc, processMetricErrors(errors), streamId, streamKeys)
  }.toResult(preMsg = s"Unable to process metrics for stream $streamId at window $windowStart due to following error:")
}
