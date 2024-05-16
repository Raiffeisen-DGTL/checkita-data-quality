package ru.raiffeisen.checkita.core.metrics.serialization

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.config.RefinedTypes.ID
import ru.raiffeisen.checkita.config.jobconf.Metrics._
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.{AccumulatedErrors, MetricStatus}
import ru.raiffeisen.checkita.core.metrics.RegularMetric
import ru.raiffeisen.checkita.core.metrics.serialization.API._

import scala.collection.concurrent.TrieMap

class SerializersSpec extends AnyWordSpec with Matchers {
  
  def checkSerDe[T](input: T)(implicit serDe: SerDe[T]): Unit = {
    val bytes = encode(input)
    val decoded = decode[T](bytes)
    decoded shouldEqual input
//    println(input)
//    println(decoded)
  }
  
  
  "IntSerDe" must {
    "correctly serialize integer number" in {
      val numbers: Seq[Int] = Seq(0, -12345, 65543321)
      numbers.foreach(checkSerDe[Int])
    }
  }

  "LongSerDe" must {
    "correctly serialize long number" in {
      val numbers: Seq[Long] = Seq(0, -12345, 65543321).map(_.toLong)
      numbers.foreach(checkSerDe[Long])
    }
  }

  "DoubleSerDe" must {
    "correctly serialize double number" in {
      val numbers: Seq[Double] = Seq(0.0, 3.14, -23.413)
      numbers.foreach(checkSerDe[Double])
    }
  }
  
  "BooleanSerDe" must {
    "correctly serialize boolean value" in {
      val booleans: Seq[Boolean] = Seq(true, false)
      booleans.foreach(checkSerDe[Boolean])
    }
  }
  
  "StringSerDe" must {
    "correctly serialize string value" in {
      val strings: Seq[String] = Seq("foo", "bar", "some ver long string", "s", "")
      strings.foreach(checkSerDe[String])
    }
  }
  
  "TupleSerDe" must {
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
  
  "CalculatorStatusSerDe" must {
    "correctly serialize calculator statuses" in {
      val statuses: Seq[CalculatorStatus] = Seq(
        CalculatorStatus.Success, 
        CalculatorStatus.Failure, 
        CalculatorStatus.Error
      )
      statuses.foreach(checkSerDe[CalculatorStatus])
    }
  }
  
  "MetricStatusSerDe" must {
    "correctly serialize metric statuses" in {
      val statuses: Seq[MetricStatus] = Seq(
        MetricStatus("metric1", CalculatorStatus.Success, "Calculation successful"),
        MetricStatus("metric2", CalculatorStatus.Failure, "There were some metric increment failures"),
        MetricStatus("metric3", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
      )
      statuses.foreach(checkSerDe[MetricStatus])
    }
  }

  "AccumulatedErrorsSerDe" must {
    "correctly serialize accumulated errors" in {
      val accErrors: Seq[AccumulatedErrors] = Seq(
        AccumulatedErrors(
          Seq("col1", "col2"),
          Seq(
            MetricStatus("metric1", CalculatorStatus.Success, "Calculation successful"),
            MetricStatus("metric2", CalculatorStatus.Failure, "There were some metric increment failures"),
            MetricStatus("metric3", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
          ),
          Seq(
            "{\"col1\": 1, \"col2\": \"value1\"}",
            "{\"col1\": 2, \"col2\": \"value2\"}",
          )
        ),
        AccumulatedErrors(
          Seq("col3", "col4"),
          Seq(
            MetricStatus("metric4", CalculatorStatus.Success, "Calculation successful"),
            MetricStatus("metric5", CalculatorStatus.Failure, "There were some metric increment failures"),
            MetricStatus("metric6", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
          ),
          Seq(
            "{\"col3\": 3.14, \"col4\": \"pi\"}",
            "{\"col3\": 2.718, \"col4\": \"euler\"}",
          )
        )
      )
      accErrors.foreach(checkSerDe[AccumulatedErrors])
    }
  }
  
  "CollectionsSerDe" must {
    "correctly serialize a sequence of integers" in {
      checkSerDe[Seq[Int]](Seq(0, -12345, 65543321))
    }
    "correctly serialize a sequence of longs" in {
      checkSerDe[Seq[Long]](Seq(0, -12345, 65543321).map(_.toLong))
    }
    "correctly serialize a sequence of doubles" in {
      checkSerDe[Seq[Double]](Seq(0.0, 3.14, -23.413))
    }
    "correctly serialize a sequence of booleans" in {
      checkSerDe[Seq[Boolean]](Seq(true, false))
    }
    "correctly serialize a list of strings" in {
      checkSerDe[List[String]](List("foo", "bar", "some ver long string", "s", ""))
    }
    "correctly serialize set of doubles" in {
      checkSerDe[Set[Double]](Set(3.14, 2.77, 9.8))
    }
    "correctly serialize map of int to string" in {
      checkSerDe[Map[Int, String]](Map(1 -> "one", 2 -> "two", 3 -> "three"))
    }
    "correctly serialize trie maps" in {
      checkSerDe[TrieMap[String, Double]](TrieMap("pi" -> 3.14, "euler" -> 2.718, "gravity" -> 9.807))
    }
    "correctly serialize complex collections" in {
      val errors: TrieMap[(String, Long), Seq[AccumulatedErrors]] = TrieMap(
        ("streamOne", 123456L) -> Seq(
          AccumulatedErrors(
            Seq("col1", "col2"),
            Seq(
              MetricStatus("metric1", CalculatorStatus.Success, "Calculation successful"),
              MetricStatus("metric2", CalculatorStatus.Failure, "There were some metric increment failures"),
              MetricStatus("metric3", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
            ),
            Seq(
              "{\"col1\": 1, \"col2\": \"value1\"}",
              "{\"col1\": 2, \"col2\": \"value2\"}",
            )
          ),
          AccumulatedErrors(
            Seq("col3", "col4"),
            Seq(
              MetricStatus("metric4", CalculatorStatus.Success, "Calculation successful"),
              MetricStatus("metric5", CalculatorStatus.Failure, "There were some metric increment failures"),
              MetricStatus("metric6", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
            ),
            Seq(
              "{\"col3\": 3.14, \"col4\": \"pi\"}",
              "{\"col3\": 2.718, \"col4\": \"euler\"}",
            )
          )
        ),
        ("streamOne", 654321L) -> Seq(
          AccumulatedErrors(
            Seq("col5", "col6"),
            Seq(
              MetricStatus("metric7", CalculatorStatus.Success, "Calculation successful"),
              MetricStatus("metric8", CalculatorStatus.Failure, "There were some metric increment failures"),
              MetricStatus("metric9", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
            ),
            Seq(
              "{\"col5\": 123456, \"col6\": \"debit\"}",
              "{\"col5\": 654321, \"col6\": \"credit\"}",
            )
          )
        )
      )
      checkSerDe[TrieMap[(String, Long), Seq[AccumulatedErrors]]](errors)
    }
  }
  
  "Regular metrics SerDe" must {
    
    "correctly serialize sequence of regular metrics" in {
      val regMetrics: Seq[RegularMetric] = Seq(
        RowCountMetricConfig(ID("some_metric"), None, "some_source"),
        DistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
        DuplicateValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
        ApproxDistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")))
      )
      checkSerDe[Seq[RegularMetric]](regMetrics)
    }

    "correctly serialize all regular metric case classes" in {
      checkSerDe[RowCountMetricConfig](RowCountMetricConfig(ID("some_metric"), None, "some_source"))
      checkSerDe[DistinctValuesMetricConfig](DistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[DuplicateValuesMetricConfig](DuplicateValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      checkSerDe[ApproxDistinctValuesMetricConfig](ApproxDistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
      
      
//        NullValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        EmptyValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        CompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        SequenceCompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        ApproxSequenceCompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        MinStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        MaxStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        AvgStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        StringLengthMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          StringLengthParams(10, CompareRule.Eq)
//        ),
//        StringInDomainMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
//        ),
//        StringOutDomainMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
//        ),
//        StringValuesMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          StringValuesParams("foo")
//        ),
//        RegexMatchMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          RegexParams("""^\w+$""")
//        ),
//        RegexMismatchMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          RegexParams("""^\w+$""")
//        ),
//        FormattedDateMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        FormattedNumberMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          FormattedNumberParams(14, 8)
//        ),
//        MinNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        MaxNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        SumNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        AvgNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        StdNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        CastedNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        NumberInDomainMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
//        ),
//        NumberOutDomainMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
//        ),
//        NumberLessThanMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          NumberCompareParams(3.14)
//        ),
//        NumberGreaterThanMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          NumberCompareParams(3.14)
//        ),
//        NumberBetweenMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          NumberIntervalParams(3.14, 9.8)
//        ),
//        NumberNotBetweenMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          NumberIntervalParams(3.14, 9.8)
//        ),
//        NumberValuesMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          NumberValuesParams(3.14)
//        ),
//        MedianValueMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        FirstQuantileMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        ThirdQuantileMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
//        GetQuantileMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          TDigestGeqQuantileParams(target = 0.99)
//        ),
//        GetPercentileMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
//          TDigestGeqPercentileParams(target = 42)
//        ),
//        ColumnEqMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))),
//        DayDistanceMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2")),
//          DayDistanceParams(3)
//        ),
//        LevenshteinDistanceMetricConfig(
//          ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2")),
//          LevenshteinDistanceParams(3)
//        ),
//        CoMomentMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))),
//        CovarianceMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))),
//        CovarianceBesselMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))),
//        TopNMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2")))
    }

  }
}
