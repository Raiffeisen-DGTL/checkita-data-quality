package ru.raiffeisen.checkita.metrics.column

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.metrics.{CalculatorStatus, MetricCalculator, StatusableCalculator}
import ru.raiffeisen.checkita.metrics.MetricProcessor.ParamMap
import ru.raiffeisen.checkita.metrics.column.MultiColumnMetrics._
import ru.raiffeisen.checkita.utils.getParametrizedMetricTail

class MultiColumnMetricsSpec extends AnyWordSpec with Matchers {
  private val testValues = Seq(
    Seq(
      Seq("Gpi2C7", "xTOn6x"), Seq("xTOn6x", "3xGSz0"), Seq("Gpi2C7", "Gpi2C7"),
      Seq("Gpi2C7", "xTOn6x"), Seq("3xGSz0", "xTOn6x"), Seq("M66yO0", "M66yO0")
    ),
    Seq(
      Seq(5.94, 1.72), Seq(1.72, 5.87), Seq(5.94, 5.94),
      Seq(5.94, 1.72), Seq(5.87, 1.72), Seq(8.26, 8.26)
    ),
    Seq(
      Seq("2.54", "7.71"), Seq("7.71", "2.16"), Seq("2.54", "2.54"),
      Seq("2.54", "7.71"), Seq("2.16", "7.71"), Seq("6.85", "6.85")
    ),
    Seq(
      Seq("4", 3.14), Seq("foo", 3.0), Seq(-25.321, "-25.321"),
      Seq("[12, 35]", true), Seq(3, "3"), Seq("bar", "3123dasd")
    )
  )

  "CovarianceMetricCalculator" must {
    val params: ParamMap = Map.empty

    "return correct metric value for sequence of two columns with numbers" in {
      val results = Seq(2.5552499999999956, -14.837100000000001)
      val values = Seq(testValues(1), testValues(2)) zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new CovarianceMetricCalculator(params))(
          (m, v) => m.increment(v)).result(),
        t._2
      ))
      metricResults.foreach { t =>
        t._1("CO_MOMENT")._1 shouldEqual t._2
        t._1("COVARIANCE")._1 shouldEqual t._2 / testValues.head.length
        t._1("COVARIANCE_BESSEL")._1 shouldEqual t._2 / (testValues.head.length - 1)
      }
    }

    "return 'OK' status and zero fail count for sequence of two columns with numbers" in {
      val metricResults = Seq(testValues(1), testValues(2)).map(
        _.foldLeft[MetricCalculator](new CovarianceMetricCalculator(params))((m, v) => m.increment(v))
      )
      metricResults.foreach { t =>
        t.asInstanceOf[StatusableCalculator].getStatus shouldEqual CalculatorStatus.OK
        t.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual 0
      }
    }

    "return NaN values when sequence contains non-number values" in {
      val metricResults = Seq(testValues.head, testValues(3)).map(s => s.foldLeft[MetricCalculator](
        new CovarianceMetricCalculator(params))((m, v) => m.increment(v)).result()
      )
      metricResults.foreach { v =>
        v("CO_MOMENT")._1.isNaN shouldEqual true
        v("COVARIANCE")._1.isNaN shouldEqual true
        v("COVARIANCE_BESSEL")._1.isNaN shouldEqual true
      }
    }

    "return fail status and correct fail counts when sequence contains non-number values" in {
      val metricResults = Seq(testValues.head, testValues(3)).map(s => s.foldLeft[MetricCalculator](
        new CovarianceMetricCalculator(params))((m, v) => m.increment(v))
      )
      (metricResults zip Seq(6, 3)).foreach { t =>
        t._1.asInstanceOf[StatusableCalculator].getStatus shouldEqual CalculatorStatus.FAILED
        t._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual t._2
      }
    }

    "return NaN values when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new CovarianceMetricCalculator(params))((m, v) => m.increment(v)).result()

      metricResult("CO_MOMENT")._1.isNaN shouldEqual true
      metricResult("COVARIANCE")._1.isNaN shouldEqual true
      metricResult("COVARIANCE_BESSEL")._1.isNaN shouldEqual true
    }

    "throw assertion error when sequence has one or more than two columns" in {
      val values = Seq(
        Seq(Seq("foo"), Seq("bar")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s =>
        an [AssertionError] should be thrownBy s.foldLeft[MetricCalculator](
          new CovarianceMetricCalculator(params))((m, v) => m.increment(v))
      }
    }
  }

  "EqualStringColumnsMetricCalculator" must {
    val params: ParamMap = Map.empty

    "return correct metric value and fail status and counts for multi-column sequence" in {
      val results = Seq.fill(4)(2)
      val statuses = Seq.fill(3)(CalculatorStatus.OK) :+ CalculatorStatus.FAILED
      val failCounts = Seq.fill(4)(4)
      val metricResults = testValues.map(
        _.foldLeft[MetricCalculator](new EqualStringColumnsMetricCalculator(params))((m, v) => m.increment(v))
      )
      (metricResults zip results).foreach(v => v._1.result()("COLUMN_EQ")._1 shouldEqual v._2)
      (metricResults zip statuses).foreach(v => v._1.asInstanceOf[StatusableCalculator].getStatus shouldEqual v._2)
      (metricResults zip failCounts).foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }

    "return zero values when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new EqualStringColumnsMetricCalculator(params))((m, v) => m.increment(v)).result()

      metricResult("COLUMN_EQ")._1 shouldEqual 0
    }
  }

  "DayDistanceMetric" must {
    val params: ParamMap = Map("dateFormat" -> "yyyy-MM-dd", "threshold" -> 3)
    val values = Seq(
      Seq(Seq("2022-01-01", "2022-01-01"), Seq("1999-12-31", "2000-01-01"), Seq("2005-03-03", "2005-03-01"), Seq("2010-10-21", "2010-10-18")),
      Seq(Seq("2022-01-01", "2022-01-01"), Seq("foo", "bar"), Seq(123, 123), Seq("2022-01-01 12:31:48", "2022-01-01 07:12:34"))
    )
    val results = Seq(3, 1)
    val failCounts = Seq(1, 3)

    "return correct metric value and fail status and counts for sequence of two columns with dates" in {
      val metricResults = values.map(
        _.foldLeft[MetricCalculator](new DayDistanceMetric(params))((m, v) => m.increment(v))
      )

      (metricResults zip results).foreach { t =>
        t._1.result()("DAY_DISTANCE" + getParametrizedMetricTail(params))._1 shouldEqual t._2
      }
      metricResults.foreach(_.asInstanceOf[StatusableCalculator].getStatus shouldEqual CalculatorStatus.FAILED)
      (metricResults zip failCounts).foreach { t =>
        t._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual t._2
      }
    }

    "return zero when applied to an empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new DayDistanceMetric(params))((m, v) => m.increment(v)).result()
      metricResult("DAY_DISTANCE" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw assertion error when sequence has one or more than two columns" in {
      val values = Seq(
        Seq(Seq("foo"), Seq("bar")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s =>
        an [AssertionError] should be thrownBy s.foldLeft[MetricCalculator](
          new DayDistanceMetric(params))((m, v) => m.increment(v))
      }
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty,
        Map("dateFormat" -> "yyyy-MM-dd", "notThreshold" -> 3),
        Map("dateFormat" -> "yyyy-MM-dd"),
        Map("dateFormat" -> "yyyy-MM-dd", "threshold" -> "not-a-number")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testValues.head.foldLeft[MetricCalculator](
          new DayDistanceMetric(params))((m, v) => m.increment(v))
      }
    }
  }

  "LevenshteinDistanceMetric" must {
    val paramList: Seq[ParamMap] = Seq(
      Map("threshold" -> 3),
      Map("threshold" -> 0.5, "normalize" -> true),
      Map("threshold" -> 0.75, "normalize" -> true),
      Map("threshold" -> 2)
    )
    val results = Seq(2, 2, 6, 2)
    val statuses = Seq.fill(3)(CalculatorStatus.OK) :+ CalculatorStatus.FAILED
    val failCounts = Seq(4, 4, 0, 4)

    "return correct metric value for sequence of two columns" in {
      val values = (testValues, paramList, results).zipped.toList
        .zip((statuses, failCounts).zipped.toList).map(x => (x._1._1, x._1._2, x._1._3, x._2._1, x._2._2))
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new LevenshteinDistanceMetric(t._2))((m, v) => m.increment(v)),
        t._2, t._3, t._4, t._5
      ))
      metricResults.foreach { t =>
        t._1.result()("LEVENSHTEIN_DISTANCE" + getParametrizedMetricTail(t._2))._1 shouldEqual t._3
      }
      metricResults.foreach { t =>
        t._1.asInstanceOf[StatusableCalculator].getStatus shouldEqual t._4
      }
      metricResults.foreach { t =>
        t._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual t._5
      }
    }

    "return zero when applied to an empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new LevenshteinDistanceMetric(paramList.head))((m, v) => m.increment(v)).result()
      metricResult("LEVENSHTEIN_DISTANCE" + getParametrizedMetricTail(paramList.head))._1 shouldEqual 0
    }

    "throw assertion error when sequence has one or more than two columns" in {
      val values = Seq(
        Seq(Seq("foo"), Seq("bar")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s =>
        an [AssertionError] should be thrownBy s.foldLeft[MetricCalculator](
          new LevenshteinDistanceMetric(paramList.head))((m, v) => m.increment(v))
      }
    }

    "throw assertion error when result is normalized and threshold > 1" in {
      val params: ParamMap = Map("threshold" -> 3, "normalize" -> true)
      an [AssertionError] should be thrownBy testValues.head.foldLeft[MetricCalculator](
        new LevenshteinDistanceMetric(params))((m, v) => m.increment(v))
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty,
        Map("notThreshold" -> 0.5, "normalize" -> true),
        Map("normalize" -> true),
        Map("threshold" -> "not-a-number")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testValues.head.foldLeft[MetricCalculator](
          new LevenshteinDistanceMetric(params))((m, v) => m.increment(v))
      }
    }
  }
}
