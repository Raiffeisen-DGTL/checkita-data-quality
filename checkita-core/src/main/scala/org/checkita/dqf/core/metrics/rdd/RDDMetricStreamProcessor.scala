package org.checkita.dqf.core.metrics.rdd

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.CollectionAccumulator
import org.checkita.dqf.config.appconf.StreamConfig
import org.checkita.dqf.core.metrics.ErrorCollection.AccumulatedErrors
import org.checkita.dqf.core.metrics.{BasicMetricProcessor, RegularMetric}
import org.checkita.dqf.core.streaming.Checkpoints.Checkpoint
import org.checkita.dqf.core.streaming.ProcessorBuffer
import org.checkita.dqf.utils.Common._
import org.checkita.dqf.utils.Logging
import org.checkita.dqf.utils.ResultUtils._

import scala.:+
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
   *   - Map of grouped calculators per each window in a stream
   *   - Updated state of checkpoint (in case if checkpoint provided)
   *   - Number of late records that were skipped
   *
   * @note window is identified by its start time as unix epoch (in seconds).
   */
  type MicroBatchState = (Long, Map[Long, GroupedCalculators], Option[Checkpoint], Int)

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
      log.debug(s"[STREAM '$streamId'] Start processing regular metrics for non-empty batch (batchId = $batchId).")

      val watermark = buffer.watermarks.getOrElse(streamId, throw new NoSuchElementException(
        s"Watermark for stream '$streamId' not found."
      ))
      log.debug(s"[STREAM '$streamId'] Current watermark: $watermark.")

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

      // exclude special columns used for windowing and checkpointing:
      val metricsByColumns = getGroupedMetrics(
        streamMetrics,
        df.schema.fieldNames.filterNot(Seq(
          streamConf.windowTsCol,
          streamConf.eventTsCol,
          streamConf.checkpointCol
        ).contains)
      )

      // get and register metric error accumulator:
      val metricErrorAccumulator = getAndRegisterErrorAccumulator

      val initState: MicroBatchState = (0L, Map.empty, streamCheckpoint, 0)
      val updatedState = df.rdd.treeAggregate(initState)(
        seqOp = {
          case (state: MicroBatchState, row: Row) =>
            val rowEventTime = row.get(columnIndexes(streamConf.eventTsCol)).asInstanceOf[Long]
            val rowWindowStart = row.get(columnIndexes(streamConf.windowTsCol)).asInstanceOf[Long]

            if (rowEventTime > watermark) {
              // record is above watermark and will be processed

              // getting either existing calculators and accumulator for this window or initializing them:
              val groupedCalculators = state._2.getOrElse(rowWindowStart, getGroupedCalculators(metricsByColumns))

              // update calculators:
              val updatedCalculators: GroupedCalculators =
                updateGroupedCalculators(groupedCalculators, row, columnIndexes)

              // collect errors:
              getErrorsFromGroupedCalculators(updatedCalculators, row, columnIndexes, columnNames, streamKeyIds)
                .foreach(err => metricErrorAccumulator.add(rowWindowStart, err))

              // update checkpoint:
              val updatedCheckpoint = state._3.map(_.update(row, columnIndexes))

              // yield updated state:
              (math.max(state._1, rowEventTime), state._2.updated(rowWindowStart, updatedCalculators), updatedCheckpoint, state._4)
            } else {
              // record is below watermark and will be skipped.
              // Return the same state with late records count incremented.
              log.debug(s"[STREAM '$streamId'] Records is late to watermark of $watermark: $row")
              state.copy(_1 = state._4 + 1)
            }
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

          val numLateRecords = l._4 + r._4

          (maxEventTime, mergedState, mergedCheckpoint, numLateRecords)
        }
      )

      log.debug(
        s"[Stream $streamId] Finished processing with state: " +
        s"maxEventTime = ${updatedState._1}; " +
        s"numLateRecords = ${updatedState._4}; " +
        s"updatedWindows = ${updatedState._2.keySet.toSeq.sorted.mkString("[", ", ", "]")}."
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
