package ru.raiffeisen.checkita.core.metrics.regular

import org.isarnproject.sketches.TDigest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Casting.tryToDouble
import ru.raiffeisen.checkita.core.metrics.regular.BasicNumericMetrics._
import ru.raiffeisen.checkita.core.metrics.{MetricCalculator, MetricName}

class BasicNumericMetricsSpec extends AnyWordSpec with Matchers {
  
  private val testSingleColSeq = Seq(
    Seq(0, 3, 8, 4, 0, 5, 5, 8, 9, 3, 2, 2, 6, 2, 6),
    Seq(7.28, 6.83, 3.0, 2.0, 6.66, 9.03, 3.69, 2.76, 4.64, 7.83, 9.19, 4.0, 7.5, 3.87, 1.0),
    Seq("7.24", "9.74", "8.32", "9.15", "5.0", "8.38", "2.0", "3.42", "3.0", "6.04", "1.0", "8.37", "0.9", "1.0", "6.54"),
    Seq(4, 3.14, "foo", 3.0, -25.321, "bar", "[12, 35]", true, 'd', '3', "34.12", "2.0", "3123dasd", 42, "4")
  )
  private val testMultiColSeq = testSingleColSeq.map(s => (0 to 4).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_))))

  "TDigestMetricCalculator" must {
    val accuracyError = 0.005
    val targetSideNumber = 0.1
    val results = testSingleColSeq.map(
      s => s.flatMap(tryToDouble).foldLeft(TDigest.empty(0.005))((t, v) => t + v)).map(
      t => (t.cdfInverse(0.5), t.cdfInverse(0.25), t.cdfInverse(0.75), t.cdfInverse(0.1), t.cdf(0.1))
    )

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new TDigestMetricCalculator(accuracyError, targetSideNumber))(
          (m, v) => m.increment(Seq(v))).result(),
        t._2
      ))
      metricResults.foreach { t =>
        t._1(MetricName.MedianValue.entryName)._1 shouldEqual t._2._1
        t._1(MetricName.FirstQuantile.entryName)._1 shouldEqual t._2._2
        t._1(MetricName.ThirdQuantile.entryName)._1 shouldEqual t._2._3
        t._1(MetricName.GetQuantile.entryName)._1 shouldEqual t._2._4
        t._1(MetricName.GetPercentile.entryName)._1 shouldEqual t._2._5
      }
    }

    "return error calculator status for multi column sequence" in {
      testSingleColSeq.foldLeft[MetricCalculator](new TDigestMetricCalculator(accuracyError, targetSideNumber)) {
        (m, v) => 
          val mc = m.increment(v)
          mc.getStatus shouldEqual CalculatorStatus.Error
          mc
      }
    }

    "return zero percentile and NaN for other metrics when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new TDigestMetricCalculator(accuracyError, targetSideNumber))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.MedianValue.entryName)._1.isNaN shouldEqual true
      metricResult.result()(MetricName.FirstQuantile.entryName)._1.isNaN shouldEqual true
      metricResult.result()(MetricName.ThirdQuantile.entryName)._1.isNaN shouldEqual true
      metricResult.result()(MetricName.GetQuantile.entryName)._1.isNaN shouldEqual true
      metricResult.result()(MetricName.GetPercentile.entryName)._1 shouldEqual 0
    }

    "throw NoSuchElementException for quantile result when targetSideNumber is greater than 1" in {
      val specialTargetSideNumber = 15
      an [NoSuchElementException] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
        new TDigestMetricCalculator(accuracyError, specialTargetSideNumber))((m, v) => m.increment(Seq(v)))
        .result()(MetricName.GetQuantile.entryName)
    }
  }
  
  "MinNumericValueMetricCalculator" must {
    val results = Seq(0.0, 1.0, 0.9, -25.321)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MinNumericValueMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.MinNumber.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MinNumericValueMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.MinNumber.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return max double value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new MinNumericValueMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.MinNumber.entryName)._1 shouldEqual Double.MaxValue
    }
  }

  "MaxNumericValueMetricCalculator" must {
    val results = Seq(9.0, 9.19, 9.74, 42.0)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MaxNumericValueMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.MaxNumber.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MaxNumericValueMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.MaxNumber.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return min double value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new MaxNumericValueMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.MaxNumber.entryName)._1 shouldEqual Double.MinValue
    }
  }

  "SumNumericValueMetricCalculator" must {

    "return correct metric value for single column sequence" in {
      val results = Seq(63.0, 79.28, 80.10000000000002, 69.939)
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new SumNumericValueMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.SumNumber.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return correct metric value for multi column sequence" in {
      val results = Seq(63.0, 79.28, 80.1, 69.939)
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new SumNumericValueMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.SumNumber.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new SumNumericValueMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.SumNumber.entryName)._1 shouldEqual 0
    }
  }

  "StdAvgNumericValueCalculator" must {

    "return correct metric value for single column sequence" in {
      val avg_results = testSingleColSeq.map(s => s.flatMap(tryToDouble)).map(s => s.sum / s.length)
      val std_results = testSingleColSeq.map(s => s.flatMap(tryToDouble).map(v => v * v))
        .map(s => s.sum / s.length).zip(avg_results).map(t => math.sqrt(t._1 - t._2 * t._2))
      val results = avg_results zip std_results
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StdAvgNumericValueCalculator())(
          (m, v) => m.increment(Seq(v))).result(),
        t._2
      ))
      metricResults.foreach(v => v._1(MetricName.AvgNumber.entryName)._1 shouldEqual v._2._1)
      metricResults.foreach(v => v._1(MetricName.StdNumber.entryName)._1 shouldEqual v._2._2)
    }

    "return error calculator status for multi column sequence" in {
      testSingleColSeq.foldLeft[MetricCalculator](new StdAvgNumericValueCalculator()) {
        (m, v) =>
          val mc = m.increment(v)
          mc.getStatus shouldEqual CalculatorStatus.Error
          mc
      }
    }
    
    "return NaN values when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new StdAvgNumericValueCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.StdNumber.entryName)._1.isNaN shouldBe true
      metricResult.result()(MetricName.AvgNumber.entryName)._1.isNaN shouldBe true
    }
  }

  "NumberFormattedValuesMetricCalculator" must {
    val paramsList = Seq(
      (Map("precision" -> 5, "scale" -> 3, "compareRule" -> "inbound"), 4, 21),
      (Map("precision" -> 5, "scale" -> 3, "compareRule" -> "outbound"), 18, 7)
    ) // map expected result vs parameters and fail counts

    val values = Seq(
      43.113, 39.2763, 21.1248, 94.8884, 96.997, 8.7525, 2.1505, 79.6918, 25.5519, 11.8093, 97.7182, 6.7502, 95.5276,
      57.2292, 16.4476, 67.8032, 68.8456, 57.617, 26.8743, 57.2209, 24.14, 32.7863, 35.7226, 46.2913, 41.1243
    ) // inbound = 4, outbound = 18


    "return correct metric value and fail counts for single column sequence" in {
      val typedValues = Seq(
        values, values.map(java.math.BigDecimal.valueOf), values.map(_.toString)
      )
      val metricResults = for {
        (params, result, failCount) <- paramsList
        values <- typedValues
        precision <- params.get("precision").map(_.asInstanceOf[Int])
        scale <- params.get("scale").map(_.asInstanceOf[Int])
        compareRule <- params.get("compareRule").map(_.asInstanceOf[String])
      } yield (
        values.foldLeft[MetricCalculator](new NumberFormattedValuesMetricCalculator(precision, scale, compareRule))(
          (m, v) => m.increment(Seq(v))
        ),
        result,
        failCount
      )

      metricResults.foreach(v => v._1.result()(MetricName.FormattedNumber.entryName)._1 shouldEqual v._2)
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._3)
    }

    "return correct metric value and fail counts for multi column sequence" in {
      val multiColValues = (0 to 4).map(c => (0 to 4).map(r => c*5 + r)).map(_.map(values(_)))
      val typedValues = Seq(
        multiColValues,
        multiColValues.map(s => s.map(java.math.BigDecimal.valueOf)),
        multiColValues.map(s => s.map(_.toString))
      )
      val metricResults = for {
        (params, result, failCount) <- paramsList
        values <- typedValues
        precision <- params.get("precision").map(_.asInstanceOf[Int])
        scale <- params.get("scale").map(_.asInstanceOf[Int])
        compareRule <- params.get("compareRule").map(_.asInstanceOf[String])
      } yield (
        values.foldLeft[MetricCalculator](new NumberFormattedValuesMetricCalculator(precision, scale, compareRule))(
          (m, v) => m.increment(v)
        ),
        result,
        failCount
      )

      metricResults.foreach(v => v._1.result()(MetricName.FormattedNumber.entryName)._1 shouldEqual v._2)
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._3)
    }

    "return zero when applied to empty sequence" in {
      val emptyValues = Seq.empty
      val precision = paramsList.head._1("precision").asInstanceOf[Int]
      val scale = paramsList.head._1("scale").asInstanceOf[Int]
      val compareRule = paramsList.head._1("compareRule").asInstanceOf[String]
      
      val metricResult = emptyValues.foldLeft[MetricCalculator](
        new NumberFormattedValuesMetricCalculator(precision, scale, compareRule)
      )((m, v) => m.increment(v))
      
      metricResult.result()(MetricName.FormattedNumber.entryName)._1 shouldEqual 0
    }

    "return zero when applied to string sequence which values are not convertable to numbers" in {
      val strValues = Seq("foo", "bar", "baz")
      val precision = paramsList.head._1("precision").asInstanceOf[Int]
      val scale = paramsList.head._1("scale").asInstanceOf[Int]
      val compareRule = paramsList.head._1("compareRule").asInstanceOf[String]
      val metricResult = strValues.foldLeft[MetricCalculator](
        new NumberFormattedValuesMetricCalculator(precision, scale, compareRule)
      )((m, v) => m.increment(Seq(v)))
      metricResult.result()(MetricName.FormattedNumber.entryName)._1 shouldEqual 0
    }
  }

  "NumberCastValuesMetricCalculator" must {

    "return correct metric value and fail counts for single column sequence" in {
      val values = testSingleColSeq(3)
      val metricResult = values.foldLeft[MetricCalculator](new NumberCastValuesMetricCalculator())(
        (m, v) => m.increment(Seq(v))
      )
      metricResult.result()(MetricName.CastedNumber.entryName)._1 shouldEqual 9
      metricResult.getFailCounter shouldEqual 6
    }

    "return correct metric value and fail counts for multi column sequence" in {
      val values = testMultiColSeq(3)
      val metricResult = values.foldLeft[MetricCalculator](new NumberCastValuesMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.CastedNumber.entryName)._1 shouldEqual 9
      metricResult.getFailCounter shouldEqual 6
    }

    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberCastValuesMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.CastedNumber.entryName)._1 shouldEqual 0
    }
  }

  "NumberInDomainValuesMetricCalculator" must {
    val domain = Seq(1, 2, 3, 4, 5).map(_.asInstanceOf[Double]).toSet
    val results = Seq(8, 4, 5, 5)
    val failCounts = Seq(7, 11, 10, 10)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(domain))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.NumberInDomain.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(domain))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(domain))(
          (m, v) => m.increment(v)).result()(MetricName.NumberInDomain.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(domain))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }

    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(domain))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.NumberInDomain.entryName)._1 shouldEqual 0
    }
  }

  "NumberOutDomainValuesMetricCalculator" must {
    val domain = Seq(1, 2, 3, 4, 5).map(_.asInstanceOf[Double]).toSet
    val results = Seq(7, 11, 10, 10)
    val failCounts = Seq(8, 4, 5, 5)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(domain))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.NumberOutDomain.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(domain))(
          (m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(domain))(
          (m, v) => m.increment(v)).result()(MetricName.NumberOutDomain.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(domain))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(domain))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.NumberOutDomain.entryName)._1 shouldEqual 0
    }
  }

  "NumberValuesMetricCalculator" must {
    val compareValue = 3
    val results = Seq(2, 1, 1, 2)
    val failCounts = Seq(13, 14, 14, 13)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(compareValue))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.NumberValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(compareValue))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(compareValue))(
          (m, v) => m.increment(v)).result()(MetricName.NumberValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(compareValue))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(compareValue))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.NumberValues.entryName)._1 shouldEqual 0
    }
  }

  "NumberLessThanMetricCalculator" must {
    val compareValue = 3
    val includeBound = true
    val results = Seq(7, 4, 5, 4)
    val failCounts = Seq(8, 11, 10, 11)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(compareValue, includeBound))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.NumberLessThan.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(compareValue, includeBound))(
          (m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(compareValue, includeBound))(
          (m, v) => m.increment(v)).result()(MetricName.NumberLessThan.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(compareValue, includeBound))(
          (m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(compareValue, includeBound))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.NumberLessThan.entryName)._1 shouldEqual 0
    }
  }

  "NumberGreaterThanMetricCalculator" must {
    val compareValue = 3
    val includeBound = false
    val results = Seq(8, 11, 10, 5)
    val failCounts = Seq(7, 4, 5, 10)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(compareValue, includeBound))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.NumberGreaterThan.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(compareValue, includeBound))(
          (m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(compareValue, includeBound))(
          (m, v) => m.increment(v)).result()(MetricName.NumberGreaterThan.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail count for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(compareValue, includeBound))(
          (m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(compareValue, includeBound))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.NumberGreaterThan.entryName)._1 shouldEqual 0
    }
  }

  "NumberBetweenMetricCalculator" must {
    val lowerCompareValue = 3
    val upperCompareValue = 6
    val includeBound = true
    val results = Seq(7, 5, 3, 5)
    val failCounts = Seq(8, 10, 12, 10)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](
          new NumberBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
        )((m, v) => m.increment(Seq(v))).result()(MetricName.NumberBetween.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](
          new NumberBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
        )((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](
          new NumberBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
        )((m, v) => m.increment(v)).result()(MetricName.NumberBetween.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](
          new NumberBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
        )((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new NumberBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.NumberBetween.entryName)._1 shouldEqual 0
    }
  }

  "NumberNotBetweenMetricCalculator" must {
    val lowerCompareValue = 2
    val upperCompareValue = 8
    val includeBound = true
    val results = Seq(8, 4, 9, 4)
    val failCounts = Seq(7, 11, 6, 11)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](
          new NumberNotBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
        )((m, v) => m.increment(Seq(v))).result()(MetricName.NumberNotBetween.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](
          new NumberNotBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
        )((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](
          new NumberNotBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
        )((m, v) => m.increment(v)).result()(MetricName.NumberNotBetween.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](
          new NumberNotBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
        )((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new NumberNotBetweenMetricCalculator(lowerCompareValue, upperCompareValue, includeBound)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.NumberNotBetween.entryName)._1 shouldEqual 0
    }
  }

  "SequenceCompletenessMetricCalculator" must {
    val incrementList: Seq[Int] = Seq(1, 4, 1, 4)
    val intSeq: Seq[Seq[Int]] = Seq(
      Range.inclusive(1, 100),
      Range.inclusive(0, 96, 4),
      Range.inclusive(1, 1000000),
      Range.inclusive(0, 999996, 4)
    )
    val nullIndices = Set(3, 7, 9, 11)
    val emptyIndices = Set(4, 8, 12, 16)
    val nullIntSeq = intSeq.map(s => s.zipWithIndex.map {
      case (_, idx) if nullIndices.contains(idx) => null
      case (v, _) => v
    })
    val emptyIntSeq = nullIntSeq.map(s => s.zipWithIndex.map {
      case (_, idx) if emptyIndices.contains(idx) => ""
      case (v, _) => v
    })

    val results = Seq(Seq.fill(4)(1), Seq(0.96, 0.84, 0.999996, 0.999984), Seq(0.92, 0.68, 0.999992, 0.999968))
    val allSingleColSeq = Seq(intSeq, nullIntSeq, emptyIntSeq)

    "return correct metric value for single column sequence" in {
      (allSingleColSeq, results).zipped.toList.foreach { tt =>
        val metricResults = (tt._1, incrementList, tt._2).zipped.toList.map(t => (
          t._1.foldLeft[MetricCalculator](new SequenceCompletenessMetricCalculator(t._2))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.SequenceCompleteness.entryName)._1,
          t._3
        ))
        metricResults.foreach(v => v._1 shouldEqual v._2)
      }
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new SequenceCompletenessMetricCalculator(incrementList.head)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.SequenceCompleteness.entryName)._1 shouldEqual 0.0
    }

    "return error calculator status for multi column sequence" in {
      val values = Seq(
        Seq(Seq("foo", "bar"), Seq("bar", "baz")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s =>
        s.foldLeft[MetricCalculator](new SequenceCompletenessMetricCalculator(incrementList.head)) {
          (m, v) =>
            val mc = m.increment(v)
            mc.getStatus shouldEqual CalculatorStatus.Error
            mc
        }
      }
    }
  }
}
