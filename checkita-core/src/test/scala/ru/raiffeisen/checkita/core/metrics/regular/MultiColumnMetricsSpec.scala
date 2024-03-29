package ru.raiffeisen.checkita.core.metrics.regular

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.metrics.regular.MultiColumnMetrics._
import ru.raiffeisen.checkita.core.metrics.{MetricCalculator, MetricName}

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

    "return correct metric value for sequence of two columns with numbers" in {
      val results = Seq(2.5552499999999956, -14.837100000000001)
      val values = Seq(testValues(1), testValues(2)) zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new CovarianceMetricCalculator())(
          (m, v) => m.increment(v)).result(),
        t._2
      ))
      metricResults.foreach { t =>
        t._1(MetricName.CoMoment.entryName)._1 shouldEqual t._2
        t._1(MetricName.Covariance.entryName)._1 shouldEqual t._2 / testValues.head.length
        t._1(MetricName.CovarianceBessel.entryName)._1 shouldEqual t._2 / (testValues.head.length - 1)
      }
    }

    "return 'Success' status and zero fail count for sequence of two columns with numbers" in {
      val metricResults = Seq(testValues(1), testValues(2)).map(
        _.foldLeft[MetricCalculator](new CovarianceMetricCalculator())((m, v) => m.increment(v))
      )
      metricResults.foreach { t =>
        t.getStatus shouldEqual CalculatorStatus.Success
        t.getFailCounter shouldEqual 0
      }
    }

    "return NaN values when sequence contains non-number values" in {
      val metricResults = Seq(testValues.head, testValues(3)).map(s => s.foldLeft[MetricCalculator](
        new CovarianceMetricCalculator())((m, v) => m.increment(v)).result()
      )
      metricResults.foreach { v =>
        v(MetricName.CoMoment.entryName)._1.isNaN shouldEqual true
        v(MetricName.Covariance.entryName)._1.isNaN shouldEqual true
        v(MetricName.CovarianceBessel.entryName)._1.isNaN shouldEqual true
      }
    }

    "return fail status and correct fail counts when sequence contains non-number values" in {
      val metricResults = Seq(testValues.head, testValues(3)).map(s => s.foldLeft[MetricCalculator](
        new CovarianceMetricCalculator())((m, v) => m.increment(v))
      )
      (metricResults zip Seq(6, 3)).foreach { t =>
        t._1.getStatus shouldEqual CalculatorStatus.Failure
        t._1.getFailCounter shouldEqual t._2
      }
    }

    "return NaN values when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new CovarianceMetricCalculator())((m, v) => m.increment(v)).result()

      metricResult(MetricName.CoMoment.entryName)._1.isNaN shouldEqual true
      metricResult(MetricName.Covariance.entryName)._1.isNaN shouldEqual true
      metricResult(MetricName.CovarianceBessel.entryName)._1.isNaN shouldEqual true
    }

    "return error calculator status when sequence has one or more than two columns" in {
      val values = Seq(
        Seq(Seq("foo"), Seq("bar")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s =>
        s.foldLeft[MetricCalculator](new CovarianceMetricCalculator()) {
          (m, v) =>
            val mc = m.increment(v)
            mc.getStatus shouldEqual CalculatorStatus.Error
            mc
        }
      }
    }
  }

  "EqualStringColumnsMetricCalculator" must {

    "return correct metric value and fail status and counts for multi-column sequence" in {
      val results = Seq.fill(4)(2)
      val statuses = Seq.fill(3)(CalculatorStatus.Success) :+ CalculatorStatus.Failure
      val statusesRev = Seq.fill(3)(CalculatorStatus.Failure) :+ CalculatorStatus.Success
      val failCounts = Seq.fill(4)(4)
      val failCountsRev = Seq.fill(4)(2)
      val metricResults = testValues.map(
        _.foldLeft[MetricCalculator](new EqualStringColumnsMetricCalculator(false))((m, v) => m.increment(v))
      )
      val metricResultsRev = testValues.map(
        _.foldLeft[MetricCalculator](new EqualStringColumnsMetricCalculator(true))((m, v) => m.increment(v))
      )
      (metricResults zip results).foreach(v => v._1.result()(MetricName.ColumnEq.entryName)._1 shouldEqual v._2)
      (metricResultsRev zip results).foreach(v => v._1.result()(MetricName.ColumnEq.entryName)._1 shouldEqual v._2)
      (metricResults zip statuses).foreach(v => v._1.getStatus shouldEqual v._2)
      (metricResultsRev zip statusesRev).foreach(v => v._1.getStatus shouldEqual v._2)
      (metricResults zip failCounts).foreach(v => v._1.getFailCounter shouldEqual v._2)
      (metricResultsRev zip failCountsRev).foreach(v => v._1.getFailCounter shouldEqual v._2)
    }

    "return zero values when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new EqualStringColumnsMetricCalculator(false))((m, v) => m.increment(v)).result()

      metricResult(MetricName.ColumnEq.entryName)._1 shouldEqual 0
    }
  }

  "DayDistanceMetric" must {
    val dateFormat = "yyyy-MM-dd"
    val threshold = 3
    val values = Seq(
      Seq(Seq("2022-01-01", "2022-01-01"), Seq("1999-12-31", "2000-01-01"), Seq("2005-03-03", "2005-03-01"), Seq("2010-10-21", "2010-10-18")),
      Seq(Seq("2022-01-01", "2022-01-01"), Seq("foo", "bar"), Seq(123, 123), Seq("2022-01-01 12:31:48", "2022-01-01 07:12:34"))
    )
    val results = Seq(3, 1)
    val failCounts = Seq(1, 3)
    val failCountsRev = Seq(3, 4)

    "return correct metric value and fail status and counts for sequence of two columns with dates" in {
      val metricResults = values.map(
        _.foldLeft[MetricCalculator](
          new DayDistanceMetricCalculator(dateFormat, threshold, false)
        )((m, v) => m.increment(v))
      )
      val metricResultsRev = values.map(
        _.foldLeft[MetricCalculator](
          new DayDistanceMetricCalculator(dateFormat, threshold, true)
        )((m, v) => m.increment(v))
      )

      (metricResults zip results).foreach { t =>
        t._1.result()(MetricName.DayDistance.entryName)._1 shouldEqual t._2
      }
      (metricResultsRev zip results).foreach { t =>
        t._1.result()(MetricName.DayDistance.entryName)._1 shouldEqual t._2
      }
      metricResults.foreach(_.getStatus shouldEqual CalculatorStatus.Failure)
      metricResultsRev.zip(Seq(CalculatorStatus.Success, CalculatorStatus.Failure))
        .foreach(t => t._1.getStatus shouldEqual t._2)
      (metricResults zip failCounts).foreach { t =>
        t._1.getFailCounter shouldEqual t._2
      }
      (metricResultsRev zip failCountsRev).foreach { t =>
        t._1.getFailCounter shouldEqual t._2
      }
    }

    "return zero when applied to an empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new DayDistanceMetricCalculator(dateFormat, threshold, false))((m, v) => m.increment(v)).result()
      metricResult(MetricName.DayDistance.entryName)._1 shouldEqual 0
    }

    "return error calculator status when sequence has one or more than two columns" in {
      val values = Seq(
        Seq(Seq("foo"), Seq("bar")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s => Seq(false, true).map { reversed =>
          s.foldLeft[MetricCalculator](new DayDistanceMetricCalculator(dateFormat, threshold, reversed)) {
            (m, v) =>
              val mc = m.increment(v)
              mc.getStatus shouldEqual CalculatorStatus.Error
              mc
          }
        }
      }
    }
  }

  "LevenshteinDistanceMetric" must {
    val paramList: Seq[(Double, Boolean, Boolean)] = Seq(
      (3, false, false), (0.501, true, false), (0.751, true, false), (2, false, false),
      (3, false, true), (0.501, true, true), (0.751, true, true), (2, false, true),
    )
    val results = Seq(2, 2, 6, 2, 2, 2, 6, 2)
    val statuses =
      Seq.fill(3)(CalculatorStatus.Success) ++ Seq.fill(4)(CalculatorStatus.Failure) :+ CalculatorStatus.Success
    val failCounts = Seq(4, 4, 0, 4, 2, 2, 6, 2)

    "return correct metric value for sequence of two columns" in {
      val values = (testValues, paramList, results).zipped.toList
        .zip((statuses, failCounts).zipped.toList).map(x => (x._1._1, x._1._2, x._1._3, x._2._1, x._2._2))
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](
          new LevenshteinDistanceMetricCalculator(t._2._1, t._2._2, t._2._3)
        )((m, v) => m.increment(v)),
        t._2, t._3, t._4, t._5
      ))
      metricResults.foreach(t => t._1.result()(MetricName.LevenshteinDistance.entryName)._1 shouldEqual t._3)
      metricResults.foreach(t => t._1.getStatus shouldEqual t._4)
      metricResults.foreach(t => t._1.getFailCounter shouldEqual t._5)
    }

    "return zero when applied to an empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new LevenshteinDistanceMetricCalculator(paramList.head._1, paramList.head._2, false)
      )((m, v) => m.increment(v)).result()
      metricResult(MetricName.LevenshteinDistance.entryName)._1 shouldEqual 0
    }

    "return error calculator status when sequence has one or more than two columns" in {
      val values = Seq(
        Seq(Seq("foo"), Seq("bar")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s => Seq(false, true).map { reversed =>
          s.foldLeft[MetricCalculator](
            new LevenshteinDistanceMetricCalculator(paramList.head._1, paramList.head._2, reversed)
          ) {
            (m, v) =>
              val mc = m.increment(v)
              mc.getStatus shouldEqual CalculatorStatus.Error
              mc
          }
        }
      }
    }
    
    "return error calculator status when result is normalized and threshold > 1" in {
      val params = (3, true)
      testValues.head.foldLeft[MetricCalculator](
        new LevenshteinDistanceMetricCalculator(params._1, params._2, false)
      ){ (m, v) =>
        val mc = m.increment(v)
        mc.getStatus shouldEqual CalculatorStatus.Error
        mc
      }
    }
  }
}
