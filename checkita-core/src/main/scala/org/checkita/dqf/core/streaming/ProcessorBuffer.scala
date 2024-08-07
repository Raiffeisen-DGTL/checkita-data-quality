package org.checkita.dqf.core.streaming

import org.checkita.dqf.core.metrics.ErrorCollection.AccumulatedErrors
import org.checkita.dqf.core.metrics.rdd.RDDMetricProcessor.GroupedCalculators
import org.checkita.dqf.core.streaming.Checkpoints.Checkpoint

import scala.collection.concurrent

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
 * @param checkpoints Source checkpoints: map of checkpointId -> checkpoint.
 * @note Scala concurrent Trie Map is used to store results in a buffer as it is a thread safe and
 *       allows lock free access to its contents.
 */
case class ProcessorBuffer(
                            watermarks: concurrent.TrieMap[String, Long],
                            calculators: concurrent.TrieMap[(String, Long), GroupedCalculators],
                            errors: concurrent.TrieMap[(String, Long), Seq[AccumulatedErrors]],
                            checkpoints: concurrent.TrieMap[String, Checkpoint]
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
      concurrent.TrieMap.empty[(String, Long), Seq[AccumulatedErrors]],
      concurrent.TrieMap.empty[String, Checkpoint] // todo: should be initial checkpoints
    )
  }

  /**
   * Creates processor buffer from tuple. Required for buffer deserialization.
   *
   * @param t Tuple with processor buffer field values.
   * @return Processor buffer instance.
   */
  def fromTuple(t: (
    concurrent.TrieMap[String, Long],
      concurrent.TrieMap[(String, Long), GroupedCalculators],
      concurrent.TrieMap[(String, Long), Seq[AccumulatedErrors]],
      concurrent.TrieMap[String, Checkpoint]
    )): ProcessorBuffer = ProcessorBuffer(t._1, t._2, t._3, t._4)
}
