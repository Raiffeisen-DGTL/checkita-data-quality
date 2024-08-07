package org.checkita.dqf.core.serialization

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.apache.spark.sql.{Encoder, Encoders}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common.{checkSerDe, spark}
import org.checkita.dqf.config.Enums._
import org.checkita.dqf.config.RefinedTypes.ID
import org.checkita.dqf.config.jobconf.MetricParams._
import org.checkita.dqf.config.jobconf.Metrics._
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.metrics.ErrorCollection.{AccumulatedErrors, MetricStatus}
import org.checkita.dqf.core.metrics.RegularMetric
import org.checkita.dqf.core.metrics.rdd.RDDMetricProcessor.GroupedCalculators
import org.checkita.dqf.core.metrics.rdd.regular.AlgebirdRDDMetrics.HyperLogLogRDDMetricCalculator
import org.checkita.dqf.core.metrics.rdd.regular.BasicNumericRDDMetrics.{StdAvgNumberRDDMetricCalculator, TDigestRDDMetricCalculator}
import org.checkita.dqf.core.metrics.rdd.regular.BasicStringRDDMetrics.RegexMatchRDDMetricCalculator
import org.checkita.dqf.core.metrics.rdd.regular.FileRDDMetrics.RowCountRDDMetricCalculator
import org.checkita.dqf.core.serialization.Implicits._
import org.checkita.dqf.core.streaming.Checkpoints._
import org.checkita.dqf.core.streaming.ProcessorBuffer

import scala.collection.concurrent.TrieMap
import scala.util.Random

class SerializersSpec extends AnyWordSpec with Matchers {
  
  private val intSeq: Seq[Int] = Seq(0, -12345, 65543321)
  private val longSeq: Seq[Long] = intSeq.map(_.toLong)
  private val dblSeq: Seq[Double] = Seq(0.0, 3.14, -23.413)
  private val boolSeq: Seq[Boolean] = Seq(true, false)
  private val strSeq: Seq[String] = Seq("foo", "bar", "some ver long string", "s", "")
  
  private val sStat: CalculatorStatus = CalculatorStatus.Success
  private val fStat: CalculatorStatus = CalculatorStatus.Failure
  private val eStat: CalculatorStatus = CalculatorStatus.Error
  private val statSeq: Seq[CalculatorStatus] = Seq(sStat, fStat, eStat)
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
  
  "Basic SerDe" must {
    "correctly serialize integer number" in {
      intSeq.foreach(checkSerDe[Int])
    }
    "correctly serialize long number" in {
      longSeq.foreach(checkSerDe[Long])
    }
    "correctly serialize double number" in {
      dblSeq.foreach(checkSerDe[Double])
    }
    "correctly serialize boolean value" in {
      boolSeq.foreach(checkSerDe[Boolean])
    }
    "correctly serialize string value" in {
      strSeq.foreach(checkSerDe[String])
    }
  }
  
  "Tuples' SerDe" must {
    "correctly serialize tuples with two elements" in {
      val t1: (Int, Int) = (1, 2)
      val t2: (Double, String) = (3.14, "Pi")
      val t3: (String, String) = ("", "some value")
      
      checkSerDe[(Int, Int)](t1)
      checkSerDe[(Double, String)](t2)
      checkSerDe[(String, String)](t3)
    }
    "correctly serialize tuples with three elements" in {
      val t1: (Int, Int, Int) = (1, 2, 3)
      val t2: (Double, String, Boolean) = (3.14, "Pi", false)
      val t3: (String, String, String) = ("some", "-", "value")

      checkSerDe[(Int, Int, Int)](t1)
      checkSerDe[(Double, String, Boolean)](t2)
      checkSerDe[(String, String, String)](t3)
    }
  }
  
  "CalculatorStatus SerDe" must {
    "correctly serialize calculator statuses" in {
      statSeq.foreach(checkSerDe[CalculatorStatus])
    }
  }
  
  "MetricStatus SerDe" must {
    "correctly serialize metric statuses" in {
      mStatSeq.foreach(checkSerDe[MetricStatus])
    }
  }

  "AccumulatedErrors SerDe" must {
    "correctly serialize accumulated errors" in {
      accErrors.foreach(checkSerDe[AccumulatedErrors])
    }
  }
  
  "Collections' SerDe" must {
    "correctly serialize a sequence of integers" in {
      checkSerDe[Seq[Int]](intSeq)
    }
    "correctly serialize a sequence of longs" in {
      checkSerDe[Seq[Long]](longSeq)
    }
    "correctly serialize a sequence of doubles" in {
      checkSerDe[Seq[Double]](dblSeq)
    }
    "correctly serialize a sequence of booleans" in {
      checkSerDe[Seq[Boolean]](boolSeq)
    }
    "correctly serialize a list of strings" in {
      checkSerDe[List[String]](strSeq.toList)
    }
    "correctly serialize set of doubles" in {
      checkSerDe[Set[Double]](dblSeq.toSet)
    }
    "correctly serialize large collections of elements" in {
      checkSerDe[Seq[Int]](Range.inclusive(1, 100000))
      checkSerDe[Set[Int]](Range.inclusive(1, 100000).toSet)
      checkSerDe[Set[Double]](Range.inclusive(1, 100000).map(_ => Random.nextDouble()).toSet)
      checkSerDe[Map[Int, Double]](Range.inclusive(1, 100000).map(i => i -> Random.nextDouble()).toMap)
    }
    "correctly serialize map of int to string" in {
      checkSerDe[Map[Int, String]](Map(1 -> "one", 2 -> "two", 3 -> "three"))
    }
    "correctly serialize trie maps" in {
      checkSerDe[TrieMap[String, Double]](TrieMap("pi" -> 3.14, "euler" -> 2.718, "gravity" -> 9.807))
    }
    "correctly serialize complex collections" in {
      checkSerDe[TrieMap[(String, Long), Seq[AccumulatedErrors]]](errors)
    }
  }
  
  "Regular metrics' SerDe" must {
    "correctly serialize all regular metric case classes" in {
      checkSerDe[RegularMetric](RowCountMetricConfig(ID("some_metric"), None, "some_source"))
      checkSerDe[RegularMetric](DistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](DuplicateValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](ApproxDistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](NullValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](EmptyValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](CompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](SequenceCompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](ApproxSequenceCompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](MinStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](MaxStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](AvgStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](StringLengthMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        StringLengthParams(10, CompareRule.Eq)
      ))
      checkSerDe[RegularMetric](StringInDomainMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
      ))
      checkSerDe[RegularMetric](StringOutDomainMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
      ))
      checkSerDe[RegularMetric](StringValuesMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        StringValuesParams("foo")
      ))
      checkSerDe[RegularMetric](RegexMatchMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        RegexParams("""^\w+$""")
      ))
      checkSerDe[RegularMetric](RegexMismatchMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        RegexParams("""^\w+$""")
      ))
      checkSerDe[RegularMetric](FormattedDateMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](FormattedNumberMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        FormattedNumberParams(14, 8)
      ))
      checkSerDe[RegularMetric](MinNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](MaxNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](SumNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](AvgNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](StdNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](CastedNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](NumberInDomainMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
      ))
      checkSerDe[RegularMetric](NumberOutDomainMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
      ))
      checkSerDe[RegularMetric](NumberLessThanMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberCompareParams(3.14)
      ))
      checkSerDe[RegularMetric](NumberGreaterThanMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberCompareParams(3.14)
      ))
      checkSerDe[RegularMetric](NumberBetweenMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberIntervalParams(3.14, 9.8)
      ))
      checkSerDe[RegularMetric](NumberNotBetweenMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberIntervalParams(3.14, 9.8)
      ))
      checkSerDe[RegularMetric](NumberValuesMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberValuesParams(3.14)
      ))
      checkSerDe[RegularMetric](MedianValueMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](FirstQuantileMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](ThirdQuantileMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[RegularMetric](GetQuantileMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        TDigestGeqQuantileParams(target = 0.99)
      ))
      checkSerDe[RegularMetric](GetPercentileMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        TDigestGeqPercentileParams(target = 42)
      ))
      checkSerDe[RegularMetric](ColumnEqMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
      checkSerDe[RegularMetric](DayDistanceMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2")),
        DayDistanceParams(3)
      ))
      checkSerDe[RegularMetric](LevenshteinDistanceMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2")),
        LevenshteinDistanceParams(3)
      ))
      checkSerDe[RegularMetric](CoMomentMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
      checkSerDe[RegularMetric](CovarianceMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
      checkSerDe[RegularMetric](CovarianceBesselMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
      checkSerDe[RegularMetric](TopNMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
    }

    "correctly serialize generic sequence of regular metrics" in {
      val regMetrics: Seq[RegularMetric] = Seq(
        RowCountMetricConfig(ID("some_metric"), None, "some_source"),
        DistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
        DuplicateValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
        ApproxDistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")))
      )
      checkSerDe[Seq[RegularMetric]](regMetrics)
    }
  }
  
  "ProcessorBuffer SerDe" must {
    val watermarks: TrieMap[String, Long] = TrieMap("streamOne" -> 123456L, "streamTwo" -> 654321L)
    val calculators: TrieMap[(String, Long), GroupedCalculators] = TrieMap(
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
    val checkpoints = TrieMap(
      "streamOne" -> KafkaCheckpoint("streamOne", Map(("some.topic" -> 0) -> 123456)).asInstanceOf[Checkpoint]
    )
    val buffer = ProcessorBuffer(watermarks, calculators, errors, checkpoints)
    
    "correctly serialize streaming processor buffer" in {
      checkSerDe[ProcessorBuffer](buffer)
    }
    "correctly serialize streaming processor buffer using Kryo" in {
      val path = "internal/tmp/buffer"
      implicit val encoder: Encoder[ProcessorBuffer] = Encoders.kryo[ProcessorBuffer]
      val df = spark.createDataset(Seq(buffer))
      df.write.mode("overwrite").parquet(path)
      val decoded = spark.read.parquet(path).as[ProcessorBuffer].take(1).head
      print(decoded)
      buffer shouldEqual decoded
    }
  }
}
