package ru.raiffeisen.checkita.core.metrics.rdd

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.raiffeisen.checkita.config.appconf.StreamConfig
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.AccumulatedErrors
import ru.raiffeisen.checkita.core.metrics.{BasicMetricProcessor, RegularMetric}
import ru.raiffeisen.checkita.core.streaming.Checkpoints.Checkpoint
import ru.raiffeisen.checkita.core.streaming.ProcessorBuffer
import ru.raiffeisen.checkita.utils.Common._
import ru.raiffeisen.checkita.utils.Logging
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.jdk.CollectionConverters._
import scala.util.Try

object RDDMetricStreamProcessor extends RDDMetricProcessor with Logging {
  import BasicMetricProcessor._
  import RDDMetricProcessor._

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
  type MicroBatchState = (Long, Map[Long, GroupedCalculators], Option[Checkpoint])


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
                            streamMetrics: Seq[RegularMetric],
                            streamCheckpoint: Option[Checkpoint])
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
        batchDf.select(batchDf.columns.map(c => col(c).as(c.toLowerCase)).toSeqUnsafe: _*)

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

      val metricsByColumns = getGroupedMetrics(streamMetrics, df.schema.fieldNames)

      // get and register metric error accumulator:
      val metricErrorAccumulator = getAndRegisterErrorAccumulator

      val initState: MicroBatchState = (0L, Map.empty, streamCheckpoint)
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

            // update checkpoint:
            val updatedCheckpoint = state._3.map(_.update(row, columnIndexes))
            
            // yield updated state:
            (math.max(state._1, rowEventTime), state._2.updated(rowWindowStart, updatedCalculators), updatedCheckpoint)
        },
        combOp = (l, r) => {
          val maxEventTime = math.max(l._1, r._1)

          val mergedState = (l._2.toSeq ++ r._2.toSeq)
            .groupBy(_._1)
            .map { case (k, v) =>
              k -> v.map(_._2).reduce((lt, rt) => mergeGroupCalculators(lt, rt))
            }.map(identity)
          
          val mergedCheckpoint = for {
            lChk <- l._3
            rChk <- r._3
          } yield lChk.merge(rChk)
          
          (maxEventTime, mergedState, mergedCheckpoint)
        }
      )

      // update buffer watermarks:
      buffer.watermarks.update(streamId, math.max(watermark, updatedState._1 - streamConf.watermark.toSeconds))
      
      // update buffer calculators:
      updatedState._2.foreach {
        case (window, state) =>
          val bufferedCalculators = buffer.calculators.get((streamId, window))
          val updatedCalculators = bufferedCalculators
            .map(bc => mergeGroupCalculators(bc, state))
            .getOrElse(state) // if buffer doesn't contain results for this window, yield newly computed ones.
          buffer.calculators.update((streamId, window), updatedCalculators)
      }

      // update buffer errors:
      metricErrorAccumulator.value.asScala
        .groupBy(t => t._1)
        .map{ case (k, v) => k -> v.map(_._2)}
        .foreach {
          case (window, errors) =>
            val bufferedErrors = buffer.errors.get((streamId, window))
            val updatedErrors = bufferedErrors.map(be => be ++ errors).getOrElse(errors).toSeq
            buffer.errors.update((streamId, window), updatedErrors)
        }
      
      // update buffer checkpoints:
      updatedState._3.foreach { checkpoint =>
        val currentCheckpoint = buffer.checkpoints.get(checkpoint.id)
        val updatedCheckpoint = currentCheckpoint.map(_.merge(checkpoint)).getOrElse(checkpoint)
        buffer.checkpoints.update(checkpoint.id, updatedCheckpoint)
      }
      
      metricErrorAccumulator.reset()

      log.info(s"[STREAM '$streamId'] Regular metrics are processed for non-empty batch (batchId = $batchId).")
    }


  /**
   * Processes calculator results and accumulated errors once stream window state is finalized.
   *
   * @param gc          Final map of grouped metric calculators for stream window
   * @param errors      Final sequence of metric errors accumulated for stream window
   * @param windowStart Window start datetime (string in format 'yyyy-MM-dd HH:mm:ss')
   * @param streamId    Id of the stream source for which results are processed.
   * @param streamKeys  Stream source key fields.
   * @param dumpSize    Implicit value of maximum number of metric failures (or errors) to be collected (per metric).
   *                    Used to prevent OOM errors.
   * @return Map of metricId to a sequence of metric results for this metricId (some metrics yield multiple results).
   */
  def processWindowResults(gc: GroupedCalculators,
                           errors: Seq[AccumulatedErrors],
                           windowStart: String,
                           streamId: String,
                           streamKeys: Seq[String])(implicit dumpSize: Int): Result[MetricResults] = Try {
    buildResults(gc, processMetricErrors(errors), streamId, streamKeys)
  }.toResult(preMsg = s"Unable to process metrics for stream $streamId at window $windowStart due to following error:")
}
