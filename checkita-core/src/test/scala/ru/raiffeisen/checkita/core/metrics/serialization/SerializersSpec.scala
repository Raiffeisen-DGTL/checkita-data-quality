package ru.raiffeisen.checkita.core.metrics.serialization

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common.checkSerDe
import ru.raiffeisen.checkita.config.Enums._
import ru.raiffeisen.checkita.config.RefinedTypes.ID
import ru.raiffeisen.checkita.config.jobconf.MetricParams._
import ru.raiffeisen.checkita.config.jobconf.Metrics._
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.{AccumulatedErrors, MetricStatus}
import ru.raiffeisen.checkita.core.metrics.RegularMetric
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricProcessor.GroupedCalculators
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricStreamProcessor.ProcessorBuffer
import ru.raiffeisen.checkita.core.metrics.rdd.regular.BasicNumericRDDMetrics.{StdAvgNumberRDDMetricCalculator, TDigestRDDMetricCalculator}
import ru.raiffeisen.checkita.core.metrics.rdd.regular.BasicStringRDDMetrics.RegexMatchRDDMetricCalculator
import ru.raiffeisen.checkita.core.metrics.rdd.regular.FileRDDMetrics.RowCountRDDMetricCalculator
import ru.raiffeisen.checkita.core.metrics.serialization.Implicits._

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
      checkSerDe[Set[Double]](Range.inclusive(1, 100000).map(_ => Random.nextDouble).toSet)
      checkSerDe[Map[Int, Double]](Range.inclusive(1, 100000).map(i => i -> Random.nextDouble).toMap)
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
      checkSerDe[RowCountMetricConfig](RowCountMetricConfig(ID("some_metric"), None, "some_source"))
      checkSerDe[DistinctValuesMetricConfig](DistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[DuplicateValuesMetricConfig](DuplicateValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[ApproxDistinctValuesMetricConfig](ApproxDistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[NullValuesMetricConfig](NullValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[EmptyValuesMetricConfig](EmptyValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[CompletenessMetricConfig](CompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[SequenceCompletenessMetricConfig](SequenceCompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[ApproxSequenceCompletenessMetricConfig](ApproxSequenceCompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[MinStringMetricConfig](MinStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[MaxStringMetricConfig](MaxStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[AvgStringMetricConfig](AvgStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[StringLengthMetricConfig](StringLengthMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        StringLengthParams(10, CompareRule.Eq)
      ))
      checkSerDe[StringInDomainMetricConfig](StringInDomainMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
      ))
      checkSerDe[StringOutDomainMetricConfig](StringOutDomainMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
      ))
      checkSerDe[StringValuesMetricConfig](StringValuesMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        StringValuesParams("foo")
      ))
      checkSerDe[RegexMatchMetricConfig](RegexMatchMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        RegexParams("""^\w+$""")
      ))
      checkSerDe[RegexMismatchMetricConfig](RegexMismatchMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        RegexParams("""^\w+$""")
      ))
      checkSerDe[FormattedDateMetricConfig](FormattedDateMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[FormattedNumberMetricConfig](FormattedNumberMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        FormattedNumberParams(14, 8)
      ))
      checkSerDe[MinNumberMetricConfig](MinNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[MaxNumberMetricConfig](MaxNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[SumNumberMetricConfig](SumNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[AvgNumberMetricConfig](AvgNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[StdNumberMetricConfig](StdNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[CastedNumberMetricConfig](CastedNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[NumberInDomainMetricConfig](NumberInDomainMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
      ))
      checkSerDe[NumberOutDomainMetricConfig](NumberOutDomainMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
      ))
      checkSerDe[NumberLessThanMetricConfig](NumberLessThanMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberCompareParams(3.14)
      ))
      checkSerDe[NumberGreaterThanMetricConfig](NumberGreaterThanMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberCompareParams(3.14)
      ))
      checkSerDe[NumberBetweenMetricConfig](NumberBetweenMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberIntervalParams(3.14, 9.8)
      ))
      checkSerDe[NumberNotBetweenMetricConfig](NumberNotBetweenMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberIntervalParams(3.14, 9.8)
      ))
      checkSerDe[NumberValuesMetricConfig](NumberValuesMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        NumberValuesParams(3.14)
      ))
      checkSerDe[MedianValueMetricConfig](MedianValueMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[FirstQuantileMetricConfig](FirstQuantileMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[ThirdQuantileMetricConfig](ThirdQuantileMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[GetQuantileMetricConfig](GetQuantileMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        TDigestGeqQuantileParams(target = 0.99)
      ))
      checkSerDe[GetPercentileMetricConfig](GetPercentileMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
        TDigestGeqPercentileParams(target = 42)
      ))
      checkSerDe[ColumnEqMetricConfig](ColumnEqMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
      checkSerDe[DayDistanceMetricConfig](DayDistanceMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2")),
        DayDistanceParams(3)
      ))
      checkSerDe[LevenshteinDistanceMetricConfig](LevenshteinDistanceMetricConfig(
        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2")),
        LevenshteinDistanceParams(3)
      ))
      checkSerDe[CoMomentMetricConfig](CoMomentMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
      checkSerDe[CovarianceMetricConfig](CovarianceMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
      checkSerDe[CovarianceBesselMetricConfig](CovarianceBesselMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
      checkSerDe[TopNMetricConfig](TopNMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
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
    "correctly serialize streaming processor buffer" in {
      val watermarks: TrieMap[String, Long] = TrieMap("streamOne" -> 123456L, "streamTwo" -> 654321L)
      val calculators: TrieMap[(String, Long), GroupedCalculators] = TrieMap(
        ("streamOne", 111111111L) -> Map(Seq("col1", "col2") -> Seq(
          RowCountRDDMetricCalculator(10) -> Seq(RowCountMetricConfig(ID("row_cnt_metric"), None, "streamOne")),
          StdAvgNumberRDDMetricCalculator(123, 7654313, 10) -> Seq(
            AvgNumberMetricConfig(ID("avg_num_metric"), None, "streamOne", Refined.unsafeApply(Seq("col1", "col2"))),
            SumNumberMetricConfig(ID("sum_num_metric"), None, "streamOne", Refined.unsafeApply(Seq("col1", "col2"))),
          )
        )),
        ("streamTwo", 222222222L) -> Map(Seq("col3") -> Seq(
          RowCountRDDMetricCalculator(25) -> Seq(RowCountMetricConfig(ID("row_cnt_metric_2"), None, "streamTwo")),
          RegexMatchRDDMetricCalculator(5, """^\w+$""", reversed = false) -> Seq(RegexMismatchMetricConfig(
            ID("some_metric"), None, "streamTwo", Refined.unsafeApply(Seq("col3")),
            RegexParams("""^\w+$""")
          ))
        ))
      )
      val buffer = ProcessorBuffer(watermarks, calculators, errors)
      checkSerDe[ProcessorBuffer](buffer)
    }
  }
}
