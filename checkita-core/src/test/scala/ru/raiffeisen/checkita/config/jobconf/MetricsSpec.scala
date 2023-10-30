package ru.raiffeisen.checkita.config.jobconf

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.config.Enums.CompareRule
import ru.raiffeisen.checkita.config.RefinedTypes.ID
import ru.raiffeisen.checkita.config.jobconf.MetricParams._
import ru.raiffeisen.checkita.config.jobconf.Metrics._

import scala.util.Try

class MetricsSpec extends AnyWordSpec with Matchers {
  
  private def checkSerialization[T <: Serializable](value: T): Boolean = 
    Try(SerializationUtils.serialize(value)).isSuccess
  
  "All metrics" must {
    "be serializable for Spark" in {
      val allMetrics = Seq(
        ComposedMetricConfig(ID("some_metric"), None, "$met1 + $met2"),
        RowCountMetricConfig(ID("some_metric"), "some_source", None),
        DistinctValuesMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        DuplicateValuesMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        ApproxDistinctValuesMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        NullValuesMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        EmptyValuesMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        CompletenessMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        SequenceCompletenessMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        ApproxSequenceCompletenessMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        MinStringMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        MaxStringMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        AvgStringMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        StringLengthMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          StringLengthParams(10, CompareRule.Eq)
        ),
        StringInDomainMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
        ),
        StringOutDomainMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          StringDomainParams(Refined.unsafeApply(Seq("foo", "bar")))
        ),
        StringValuesMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          StringValuesParams("foo")
        ),
        RegexMatchMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          RegexParams("""^\w+$""")
        ),
        RegexMismatchMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          RegexParams("""^\w+$""")
        ),
        FormattedDateMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        FormattedNumberMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          FormattedNumberParams(14, 8)
        ),
        MinNumberMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        MaxNumberMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        SumNumberMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        AvgNumberMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        StdNumberMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        CastedNumberMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        NumberInDomainMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
        ),
        NumberOutDomainMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          NumberDomainParams(Refined.unsafeApply(Seq(1, 2, 3)))
        ),
        NumberLessThanMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          NumberCompareParams(3.14)
        ),
        NumberGreaterThanMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          NumberCompareParams(3.14)
        ),
        NumberBetweenMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          NumberIntervalParams(3.14, 9.8)
        ),
        NumberNotBetweenMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          NumberIntervalParams(3.14, 9.8)
        ),
        NumberValuesMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          NumberValuesParams(3.14)
        ),
        MedianValueMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        FirstQuantileMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        ThirdQuantileMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1"))),
        GetQuantileMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          TDigestGeqQuantileParams(target = 0.99)
        ),
        GetPercentileMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1")),
          TDigestGeqPercentileParams(target = 42)
        ),
        ColumnEqMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1", "col2"))),
        DayDistanceMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1", "col2")),
          DayDistanceParams(3)
        ),
        LevenshteinDistanceMetricConfig(
          ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1", "col2")),
          LevenshteinDistanceParams(3)
        ),
        CoMomentMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1", "col2"))),
        CovarianceMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1", "col2"))),
        CovarianceBesselMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1", "col2"))),
        TopNMetricConfig(ID("some_metric"), "some_source", None, Refined.unsafeApply(Seq("col1", "col2")))
      )
      allMetrics.foreach(m => checkSerialization(m) shouldEqual true)
    }
  }
}
