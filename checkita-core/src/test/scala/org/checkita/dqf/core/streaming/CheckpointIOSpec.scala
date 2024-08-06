package org.checkita.dqf.core.streaming

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common.{jobId, spark}
import org.checkita.dqf.config.RefinedTypes.ID
import org.checkita.dqf.config.jobconf.MetricParams._
import org.checkita.dqf.config.jobconf.Metrics._
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.metrics.ErrorCollection.{AccumulatedErrors, MetricStatus}
import org.checkita.dqf.core.metrics.rdd.RDDMetricProcessor.GroupedCalculators
import org.checkita.dqf.core.metrics.rdd.regular.AlgebirdRDDMetrics.HyperLogLogRDDMetricCalculator
import org.checkita.dqf.core.metrics.rdd.regular.BasicNumericRDDMetrics.{StdAvgNumberRDDMetricCalculator, TDigestRDDMetricCalculator}
import org.checkita.dqf.core.metrics.rdd.regular.BasicStringRDDMetrics.RegexMatchRDDMetricCalculator
import org.checkita.dqf.core.metrics.rdd.regular.FileRDDMetrics.RowCountRDDMetricCalculator
import org.checkita.dqf.core.streaming.Checkpoints._
import org.checkita.dqf.utils.Common.getStringHash
import org.checkita.dqf.utils.ResultUtils.liftToResult

import java.time.{LocalDateTime, ZoneOffset}
import scala.collection.concurrent.TrieMap
import scala.util.Random

class CheckpointIOSpec extends AnyWordSpec with Matchers {

  private val mStatSeq: Seq[MetricStatus] = Seq(
    MetricStatus("metric1", CalculatorStatus.Success, "Calculation successful"),
    MetricStatus("metric2", CalculatorStatus.Failure, "There were some metric increment failures"),
    MetricStatus("metric3", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
  )
  
  private val accErrors: Seq[AccumulatedErrors] = Seq(
    AccumulatedErrors(Seq("col1", "col2"), mStatSeq, Seq(
      "{\"col1\": 1, \"col2\": \"value1\"}",
      "{\"col1\": 2, \"col2\": \"value2\"}",
    )),
    AccumulatedErrors(Seq("col3", "col4"), mStatSeq, Seq(
      "{\"col3\": 3.14, \"col4\": \"pi\"}",
      "{\"col3\": 2.718, \"col4\": \"euler\"}",
    )),
    AccumulatedErrors(
      Seq("col5", "col6"), mStatSeq, Seq(
        "{\"col5\": 123456, \"col6\": \"debit\"}",
        "{\"col5\": 654321, \"col6\": \"credit\"}",
      ))
  )
  
  private val errors: TrieMap[(String, Long), Seq[AccumulatedErrors]] = TrieMap(
    ("streamOne", 123456L) -> accErrors.take(1),
    ("streamOne", 654321L) -> accErrors.tail
  )
  
  private val watermarks: TrieMap[String, Long] = TrieMap("streamOne" -> 123456L, "streamTwo" -> 654321L)
  
  private val calculators: TrieMap[(String, Long), GroupedCalculators] = TrieMap(
    ("streamOne", 111111111L) -> Map(Seq("col1", "col2") -> Seq(
      RowCountRDDMetricCalculator(10) -> Seq(RowCountMetricConfig(ID("row_cnt_metric"), None, "streamOne")),
      StdAvgNumberRDDMetricCalculator(123, 7654313, 10) -> Seq(
        AvgNumberMetricConfig(ID("avg_num_metric"), None, "streamOne", Refined.unsafeApply(Seq("col1", "col2"))),
        SumNumberMetricConfig(ID("sum_num_metric"), None, "streamOne", Refined.unsafeApply(Seq("col1", "col2"))),
      ),
      new HyperLogLogRDDMetricCalculator(0.001) -> Seq(ApproxDistinctValuesMetricConfig(
        ID("approx_dist"), None, "streamOne", Refined.unsafeApply(Seq("col1", "col2")),
        ApproxDistinctValuesParams(0.001)
      ))
    )),
    ("streamTwo", 222222222L) -> Map(Seq("col3") -> Seq(
      RowCountRDDMetricCalculator(25) -> Seq(RowCountMetricConfig(ID("row_cnt_metric_2"), None, "streamTwo")),
      RegexMatchRDDMetricCalculator(5, """^\w+$""", reversed = false) -> Seq(RegexMismatchMetricConfig(
        ID("some_metric"), None, "streamTwo", Refined.unsafeApply(Seq("col3")),
        RegexParams("""^\w+$""")
      )),
      new TDigestRDDMetricCalculator(0.001, 0.1) -> Seq(GetQuantileMetricConfig(
        ID("get_q_metric"), None, "streamTwo", Refined.unsafeApply(Seq("col3")),
        TDigestGeqQuantileParams(target = 0.1)
      ))
    ))
  )

  private val checkpoints: TrieMap[String, Checkpoint] = TrieMap(
    "streamOne" -> KafkaCheckpoint("streamOne", Map(("some.topic1" -> 0) -> Random.nextLong())).asInstanceOf[Checkpoint],
    "streamTwo" -> KafkaCheckpoint("streamTwo", Map(("some.topic2" -> 0) -> Random.nextLong())).asInstanceOf[Checkpoint]
  )
  
  private val buffer: ProcessorBuffer = ProcessorBuffer(watermarks, calculators, errors, checkpoints)
  
  private val jobHash = getStringHash(Random.alphanumeric.take(100).mkString)
  
  private val checkpointDir: String = "internal/tmp/checkpoints"
  private val execTime: Long = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
  
  "ProcessorBufferIO" must {
    "write processor buffer to parquet file in checkpoint directory" in {
      CheckpointIO.writeCheckpoint(buffer, execTime, checkpointDir, jobId, jobHash) shouldEqual liftToResult(())
    }
    "read latest processor buffer from checkpoint directory" in {
      val decoded = CheckpointIO.readCheckpoint(checkpointDir, jobId, jobHash)
      decoded shouldEqual Some(buffer)
    }
  }
}
