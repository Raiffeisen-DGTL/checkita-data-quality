//package ru.raiffeisen.checkita.core.metrics.serializationv2
//
//import eu.timepit.refined.api.Refined
//import eu.timepit.refined.auto._
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import ru.raiffeisen.checkita.Common.checkSerDeV2
//import ru.raiffeisen.checkita.config.Enums._
//import ru.raiffeisen.checkita.config.RefinedTypes.ID
//import ru.raiffeisen.checkita.config.jobconf.MetricParams._
//import ru.raiffeisen.checkita.config.jobconf.Metrics._
//import ru.raiffeisen.checkita.core.CalculatorStatus
//import ru.raiffeisen.checkita.core.metrics.ErrorCollection.{AccumulatedErrors, MetricStatus}
//import ru.raiffeisen.checkita.core.metrics.RegularMetric
//import ru.raiffeisen.checkita.core.metrics.serialization.API._
//
//import scala.collection.concurrent.TrieMap
//import scala.util.Random
//
//class SerializersSpecV2 extends AnyWordSpec with Matchers {
//
//  "Int SerDe" must {
//    "correctly serialize integer number" in {
//      val numbers: Seq[Int] = Seq(0, -12345, 65543321)
//      numbers.foreach(checkSerDeV2[Int])
//    }
//  }
//
//  "Long SerDe" must {
//    "correctly serialize long number" in {
//      val numbers: Seq[Long] = Seq(0, -12345, 65543321).map(_.toLong)
//      numbers.foreach(checkSerDeV2[Long])
//    }
//  }
//
//  "DoubleSerDe" must {
//    "correctly serialize double number" in {
//      val numbers: Seq[Double] = Seq(0.0, 3.14, -23.413)
//      numbers.foreach(checkSerDeV2[Double])
//    }
//  }
//
//  "Boolean SerDe" must {
//    "correctly serialize boolean value" in {
//      val booleans: Seq[Boolean] = Seq(true, false)
//      booleans.foreach(checkSerDeV2[Boolean])
//    }
//  }
//
//  "StringSerDe" must {
//    "correctly serialize string value" in {
//      val strings: Seq[String] = Seq("foo", "bar", "some ver long string", "s", "")
//      strings.foreach(checkSerDeV2[String])
//    }
//  }
//
////  "Tuples' SerDe" must {
////    "correctly serialize tuples with two elements" in {
////      val t1: (Int, Int) = (1, 2)
////      val t2: (Double, String) = (3.14, "Pi")
////      val t3: (String, String) = ("", "some value")
////
////      checkSerDeV2[(Int, Int)](t1)
////      checkSerDeV2[(Double, String)](t2)
////      checkSerDeV2[(String, String)](t3)
////    }
////    "correctly serialize tuples with three elements" in {
////      val t1: (Int, Int, Int) = (1, 2, 3)
////      val t2: (Double, String, Boolean) = (3.14, "Pi", false)
////      val t3: (String, String, String) = ("some", "-", "value")
////
////      checkSerDeV2[(Int, Int, Int)](t1)
////      checkSerDeV2[(Double, String, Boolean)](t2)
////      checkSerDeV2[(String, String, String)](t3)
////    }
////  }
////
////  "CalculatorStatus SerDe" must {
////    "correctly serialize calculator statuses" in {
////      val statuses: Seq[CalculatorStatus] = Seq(
////        CalculatorStatus.Success,
////        CalculatorStatus.Failure,
////        CalculatorStatus.Error
////      )
////      statuses.foreach(checkSerDeV2[CalculatorStatus])
////    }
////  }
////
////  "MetricStatus SerDe" must {
////    "correctly serialize metric statuses" in {
////      val statuses: Seq[MetricStatus] = Seq(
////        MetricStatus("metric1", CalculatorStatus.Success, "Calculation successful"),
////        MetricStatus("metric2", CalculatorStatus.Failure, "There were some metric increment failures"),
////        MetricStatus("metric3", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
////      )
////      statuses.foreach(checkSerDeV2[MetricStatus])
////    }
////  }
////
////  "AccumulatedErrors SerDe" must {
////    "correctly serialize accumulated errors" in {
////      val accErrors: Seq[AccumulatedErrors] = Seq(
////        AccumulatedErrors(
////          Seq("col1", "col2"),
////          Seq(
////            MetricStatus("metric1", CalculatorStatus.Success, "Calculation successful"),
////            MetricStatus("metric2", CalculatorStatus.Failure, "There were some metric increment failures"),
////            MetricStatus("metric3", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
////          ),
////          Seq(
////            "{\"col1\": 1, \"col2\": \"value1\"}",
////            "{\"col1\": 2, \"col2\": \"value2\"}",
////          )
////        ),
////        AccumulatedErrors(
////          Seq("col3", "col4"),
////          Seq(
////            MetricStatus("metric4", CalculatorStatus.Success, "Calculation successful"),
////            MetricStatus("metric5", CalculatorStatus.Failure, "There were some metric increment failures"),
////            MetricStatus("metric6", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
////          ),
////          Seq(
////            "{\"col3\": 3.14, \"col4\": \"pi\"}",
////            "{\"col3\": 2.718, \"col4\": \"euler\"}",
////          )
////        )
////      )
////      accErrors.foreach(checkSerDeV2[AccumulatedErrors])
////    }
////  }
////
//  "Collections' SerDe" must {
//    "correctly serialize a sequence of integers" in {
//      checkSerDeV2[Seq[Int]](Seq(0, -12345, 65543321))
//    }
//    "correctly serialize a sequence of longs" in {
//      checkSerDeV2[Seq[Long]](Seq(0, -12345, 65543321).map(_.toLong))
//    }
//    "correctly serialize a sequence of doubles" in {
//      checkSerDeV2[Seq[Double]](Seq(0.0, 3.14, -23.413))
//    }
//    "correctly serialize a sequence of booleans" in {
//      checkSerDeV2[Seq[Boolean]](Seq(true, false))
//    }
//    "correctly serialize a list of strings" in {
//      checkSerDeV2[List[String]](List("foo", "bar", "some ver long string", "s", ""))
//    }
//    "correctly serialize set of doubles" in {
//      checkSerDeV2[Set[Double]](Set(3.14, 2.77, 9.8))
//    }
//    "correctly serialize large collections of elements" in {
//      checkSerDeV2[Seq[Int]](Range.inclusive(1, 100000))
//      checkSerDeV2[Set[Int]](Range.inclusive(1, 100000).toSet)
//      checkSerDeV2[Set[Double]](Range.inclusive(1, 100000).map(_ => Random.nextDouble).toSet)
//      checkSerDeV2[Map[Int, Double]](Range.inclusive(1, 100000).map(i => i -> Random.nextDouble).toMap)
//    }
//    "correctly serialize map of int to string" in {
//      checkSerDeV2[Map[Int, String]](Map(1 -> "one", 2 -> "two", 3 -> "three"))
//    }
//    "correctly serialize trie maps" in {
//      checkSerDeV2[TrieMap[String, Double]](TrieMap("pi" -> 3.14, "euler" -> 2.718, "gravity" -> 9.807))
//    }
////    "correctly serialize complex collections" in {
////      val errors: TrieMap[(String, Long), Seq[AccumulatedErrors]] = TrieMap(
////        ("streamOne", 123456L) -> Seq(
////          AccumulatedErrors(
////            Seq("col1", "col2"),
////            Seq(
////              MetricStatus("metric1", CalculatorStatus.Success, "Calculation successful"),
////              MetricStatus("metric2", CalculatorStatus.Failure, "There were some metric increment failures"),
////              MetricStatus("metric3", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
////            ),
////            Seq(
////              "{\"col1\": 1, \"col2\": \"value1\"}",
////              "{\"col1\": 2, \"col2\": \"value2\"}",
////            )
////          ),
////          AccumulatedErrors(
////            Seq("col3", "col4"),
////            Seq(
////              MetricStatus("metric4", CalculatorStatus.Success, "Calculation successful"),
////              MetricStatus("metric5", CalculatorStatus.Failure, "There were some metric increment failures"),
////              MetricStatus("metric6", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
////            ),
////            Seq(
////              "{\"col3\": 3.14, \"col4\": \"pi\"}",
////              "{\"col3\": 2.718, \"col4\": \"euler\"}",
////            )
////          )
////        ),
////        ("streamOne", 654321L) -> Seq(
////          AccumulatedErrors(
////            Seq("col5", "col6"),
////            Seq(
////              MetricStatus("metric7", CalculatorStatus.Success, "Calculation successful"),
////              MetricStatus("metric8", CalculatorStatus.Failure, "There were some metric increment failures"),
////              MetricStatus("metric9", CalculatorStatus.Error, "Unexpected runtime exception occurred."),
////            ),
////            Seq(
////              "{\"col5\": 123456, \"col6\": \"debit\"}",
////              "{\"col5\": 654321, \"col6\": \"credit\"}",
////            )
////          )
////        )
////      )
////      checkSerDeV2[TrieMap[(String, Long), Seq[AccumulatedErrors]]](errors)
////    }
//  }
////
////  "Regular metrics' SerDe" must {
////    "correctly serialize all regular metric case classes" in {
////      checkSerDeV2[RowCountMetricConfig](RowCountMetricConfig(ID("some_metric"), None, "some_source"))
////      checkSerDeV2[DistinctValuesMetricConfig](DistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[DuplicateValuesMetricConfig](DuplicateValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[ApproxDistinctValuesMetricConfig](ApproxDistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[NullValuesMetricConfig](NullValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[EmptyValuesMetricConfig](EmptyValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[CompletenessMetricConfig](CompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[SequenceCompletenessMetricConfig](SequenceCompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[ApproxSequenceCompletenessMetricConfig](ApproxSequenceCompletenessMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[MinStringMetricConfig](MinStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[MaxStringMetricConfig](MaxStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[AvgStringMetricConfig](AvgStringMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[StringLengthMetricConfig](StringLengthMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        StringLengthParams(10, CompareRule.Eq)
////      ))
////      checkSerDeV2[StringInDomainMetricConfig](StringInDomainMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
////      ))
////      checkSerDeV2[StringOutDomainMetricConfig](StringOutDomainMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
////      ))
////      checkSerDeV2[StringValuesMetricConfig](StringValuesMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        StringValuesParams("foo")
////      ))
////      checkSerDeV2[RegexMatchMetricConfig](RegexMatchMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        RegexParams("""^\w+$""")
////      ))
////      checkSerDeV2[RegexMismatchMetricConfig](RegexMismatchMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        RegexParams("""^\w+$""")
////      ))
////      checkSerDeV2[FormattedDateMetricConfig](FormattedDateMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[FormattedNumberMetricConfig](FormattedNumberMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        FormattedNumberParams(14, 8)
////      ))
////      checkSerDeV2[MinNumberMetricConfig](MinNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[MaxNumberMetricConfig](MaxNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[SumNumberMetricConfig](SumNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[AvgNumberMetricConfig](AvgNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[StdNumberMetricConfig](StdNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[CastedNumberMetricConfig](CastedNumberMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[NumberInDomainMetricConfig](NumberInDomainMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
////      ))
////      checkSerDeV2[NumberOutDomainMetricConfig](NumberOutDomainMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
////      ))
////      checkSerDeV2[NumberLessThanMetricConfig](NumberLessThanMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        NumberCompareParams(3.14)
////      ))
////      checkSerDeV2[NumberGreaterThanMetricConfig](NumberGreaterThanMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        NumberCompareParams(3.14)
////      ))
////      checkSerDeV2[NumberBetweenMetricConfig](NumberBetweenMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        NumberIntervalParams(3.14, 9.8)
////      ))
////      checkSerDeV2[NumberNotBetweenMetricConfig](NumberNotBetweenMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        NumberIntervalParams(3.14, 9.8)
////      ))
////      checkSerDeV2[NumberValuesMetricConfig](NumberValuesMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        NumberValuesParams(3.14)
////      ))
////      checkSerDeV2[MedianValueMetricConfig](MedianValueMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[FirstQuantileMetricConfig](FirstQuantileMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[ThirdQuantileMetricConfig](ThirdQuantileMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))))
////      checkSerDeV2[GetQuantileMetricConfig](GetQuantileMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        TDigestGeqQuantileParams(target = 0.99)
////      ))
////      checkSerDeV2[GetPercentileMetricConfig](GetPercentileMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")),
////        TDigestGeqPercentileParams(target = 42)
////      ))
////      checkSerDeV2[ColumnEqMetricConfig](ColumnEqMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
////      checkSerDeV2[DayDistanceMetricConfig](DayDistanceMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2")),
////        DayDistanceParams(3)
////      ))
////      checkSerDeV2[LevenshteinDistanceMetricConfig](LevenshteinDistanceMetricConfig(
////        ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2")),
////        LevenshteinDistanceParams(3)
////      ))
////      checkSerDeV2[CoMomentMetricConfig](CoMomentMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
////      checkSerDeV2[CovarianceMetricConfig](CovarianceMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
////      checkSerDeV2[CovarianceBesselMetricConfig](CovarianceBesselMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
////      checkSerDeV2[TopNMetricConfig](TopNMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1", "col2"))))
////    }
////
////    "correctly serialize generic sequence of regular metrics" in {
////      val regMetrics: Seq[RegularMetric] = Seq(
////        RowCountMetricConfig(ID("some_metric"), None, "some_source"),
////        DistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
////        DuplicateValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1"))),
////        ApproxDistinctValuesMetricConfig(ID("some_metric"), None, "some_source", Refined.unsafeApply(Seq("col1")))
////      )
////      checkSerDeV2[Seq[RegularMetric]](regMetrics)
////    }
////  }
//}
