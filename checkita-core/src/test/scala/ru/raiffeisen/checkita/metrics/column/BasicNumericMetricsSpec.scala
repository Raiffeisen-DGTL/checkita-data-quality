package ru.raiffeisen.checkita.metrics.column

import org.isarnproject.sketches.TDigest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.metrics.{MetricCalculator, StatusableCalculator}
import ru.raiffeisen.checkita.metrics.MetricProcessor.ParamMap
import ru.raiffeisen.checkita.metrics.column.BasicNumericMetrics._
import ru.raiffeisen.checkita.utils.{getParametrizedMetricTail, tryToDouble}


class BasicNumericMetricsSpec extends AnyWordSpec with Matchers {
  private val testSingleColSeq = Seq(
    Seq(0, 3, 8, 4, 0, 5, 5, 8, 9, 3, 2, 2, 6, 2, 6),
    Seq(7.28, 6.83, 3.0, 2.0, 6.66, 9.03, 3.69, 2.76, 4.64, 7.83, 9.19, 4.0, 7.5, 3.87, 1.0),
    Seq("7.24", "9.74", "8.32", "9.15", "5.0", "8.38", "2.0", "3.42", "3.0", "6.04", "1.0", "8.37", "0.9", "1.0", "6.54"),
    Seq(4, 3.14, "foo", 3.0, -25.321, "bar", "[12, 35]", true, 'd', '3', "34.12", "2.0", "3123dasd", 42, "4")
  )
  private val testMultiColSeq = testSingleColSeq.map(s => (0 to 4).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_))))

  "TDigestMetricCalculator" must {
    val params: ParamMap = Map(
      "targetSideNumber" -> 0.1
    )
    val results = testSingleColSeq.map(
      s => s.flatMap(tryToDouble).foldLeft(TDigest.empty(0.005))((t, v) => t + v)).map(
      t => (t.cdfInverse(0.5), t.cdfInverse(0.25), t.cdfInverse(0.75), t.cdfInverse(0.1), t.cdf(0.1))
    )

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new TDigestMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result(),
        t._2
      ))
      metricResults.foreach { t =>
        t._1("MEDIAN_VALUE" + getParametrizedMetricTail(params))._1 shouldEqual t._2._1
        t._1("FIRST_QUANTILE" + getParametrizedMetricTail(params))._1 shouldEqual t._2._2
        t._1("THIRD_QUANTILE" + getParametrizedMetricTail(params))._1 shouldEqual t._2._3
        t._1("GET_QUANTILE" + getParametrizedMetricTail(params))._1 shouldEqual t._2._4
        t._1("GET_PERCENTILE" + getParametrizedMetricTail(params))._1 shouldEqual t._2._5
      }
    }

    "throw assertion error for multi column sequence" in {
      testMultiColSeq.foreach { v =>
        an [AssertionError] should be thrownBy v.foldLeft[MetricCalculator](
          new TDigestMetricCalculator(params))((m, v) => m.increment(v))
      }
    }

    "return zero percentile and NaN for other metrics when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new TDigestMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("MEDIAN_VALUE" + getParametrizedMetricTail(params))._1.isNaN shouldEqual true
      metricResult.result()("FIRST_QUANTILE" + getParametrizedMetricTail(params))._1.isNaN shouldEqual true
      metricResult.result()("THIRD_QUANTILE" + getParametrizedMetricTail(params))._1.isNaN shouldEqual true
      metricResult.result()("GET_QUANTILE" + getParametrizedMetricTail(params))._1.isNaN shouldEqual true
      metricResult.result()("GET_PERCENTILE" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw NoSuchElementException for quantile result when targetSideNumber is greater than 1" in {
      val specialParams: ParamMap = Map("targetSideNumber" -> 15)
      an [NoSuchElementException] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
        new TDigestMetricCalculator(specialParams))((m, v) => m.increment(Seq(v)))
        .result()("GET_QUANTILE" + getParametrizedMetricTail(params))
    }
  }

  "MinNumericValueMetricCalculator" must {
    val params: ParamMap = Map.empty
    val results = Seq(0.0, 1.0, 0.9, -25.321)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MinNumericValueMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("MIN_NUMBER")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MinNumericValueMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("MIN_NUMBER")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return max double value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new MinNumericValueMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("MIN_NUMBER")._1 shouldEqual Double.MaxValue
    }
  }

  "MaxNumericValueMetricCalculator" must {
    val params: ParamMap = Map.empty
    val results = Seq(9.0, 9.19, 9.74, 42.0)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MaxNumericValueMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("MAX_NUMBER")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MaxNumericValueMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("MAX_NUMBER")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return min double value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new MaxNumericValueMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("MAX_NUMBER")._1 shouldEqual Double.MinValue
    }
  }

  "SumNumericValueMetricCalculator" must {
    val params: ParamMap = Map.empty

    "return correct metric value for single column sequence" in {
      val results = Seq(63.0, 79.28, 80.10000000000002, 69.939)
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new SumNumericValueMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("SUM_NUMBER")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return correct metric value for multi column sequence" in {
      val results = Seq(63.0, 79.28, 80.1, 69.939)
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new SumNumericValueMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("SUM_NUMBER")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new SumNumericValueMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("SUM_NUMBER")._1 shouldEqual 0
    }
  }

  "StdAvgNumericValueCalculator" must {
    val params: ParamMap = Map.empty

    "return correct metric value for single column sequence" in {
      val avg_results = testSingleColSeq.map(s => s.flatMap(tryToDouble)).map(s => s.sum / s.length)
      val std_results = testSingleColSeq.map(s => s.flatMap(tryToDouble).map(v => v * v))
        .map(s => s.sum / s.length).zip(avg_results).map(t => Math.sqrt(t._1 - t._2 * t._2))
      val results = avg_results zip std_results
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StdAvgNumericValueCalculator(params))(
          (m, v) => m.increment(Seq(v))).result(),
        t._2
      ))
      metricResults.foreach(v => v._1("AVG_NUMBER")._1 shouldEqual v._2._1)
      metricResults.foreach(v => v._1("STD_NUMBER")._1 shouldEqual v._2._2)
    }

    "throw assertion error for multi column sequence" in {
      testMultiColSeq.foreach { v =>
        an [AssertionError] should be thrownBy v.foldLeft[MetricCalculator](
          new StdAvgNumericValueCalculator(params))((m, v) => m.increment(v))
      }
    }

    "return NaN values when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new StdAvgNumericValueCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("STD_NUMBER")._1.isNaN shouldBe true
      metricResult.result()("AVG_NUMBER")._1.isNaN shouldBe true
    }
  }

  "NumberFormattedValuesMetricCalculator" must {
    val paramsList = Seq(
      (Map("precision" -> 5, "scale" -> 3), 4, 21),
      (Map("precision" -> 5, "scale" -> 3, "compareRule" -> "outbound"), 18, 7)
    ) // map expected result vs parameters and fail counts

    val values = Seq(
      43.113, 39.2763, 21.1248, 94.8884, 96.997, 8.7525, 2.1505, 79.6918, 25.5519, 11.8093, 97.7182, 6.7502, 95.5276,
      57.2292, 16.4476, 67.8032, 68.8456, 57.617, 26.8743, 57.2209, 24.14, 32.7863, 35.7226, 46.2913, 41.1243
    ) // inbound = 4, outbound = 18


    "return correct metric value and fail counts for single column sequence" in {
      val typedValues = Seq(
        values, values.map(BigDecimal.valueOf), values.map(_.toString)
      )
      val metricResults = for (
        (params, result, failCount) <- paramsList;
        values <- typedValues
      ) yield (
        values.foldLeft[MetricCalculator](new NumberFormattedValuesMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))),
        result,
        failCount,
        params
      )

      metricResults.foreach(v => v._1.result()("FORMATTED_NUMBER" + getParametrizedMetricTail(v._4))._1 shouldEqual v._2)
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._3)
    }

    "return correct metric value and fail counts for multi column sequence" in {
      val multiColValues = (0 to 4).map(c => (0 to 4).map(r => c*5 + r)).map(_.map(values(_)))
      val typedValues = Seq(
        multiColValues,
        multiColValues.map(s => s.map(BigDecimal.valueOf)),
        multiColValues.map(s => s.map(_.toString))
      )
      val metricResults = for (
        (params, result, failCount) <- paramsList;
        values <- typedValues
      ) yield (
        values.foldLeft[MetricCalculator](new NumberFormattedValuesMetricCalculator(params))((m, v) => m.increment(v)),
        result,
        failCount,
        params
      )

      metricResults.foreach(v => v._1.result()("FORMATTED_NUMBER" + getParametrizedMetricTail(v._4))._1 shouldEqual v._2)
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._3)
    }

    "return zero when applied to empty sequence" in {
      val emptyValues = Seq.empty
      val params = paramsList.head._1
      val metricResult = emptyValues.foldLeft[MetricCalculator](new NumberFormattedValuesMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("FORMATTED_NUMBER" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "return zero when applied to string sequence which values are not convertable to numbers" in {
      val strValues = Seq("foo", "bar", "baz")
      val params = paramsList.head._1
      val metricResult = strValues.foldLeft[MetricCalculator](new NumberFormattedValuesMetricCalculator(params))(
        (m, v) => m.increment(Seq(v))
      )
      metricResult.result()("FORMATTED_NUMBER" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty,
        Map("notPrecision" -> 5, "scale" -> 2),
        Map("precision" -> 5, "notScale" -> 2),
        Map("precision" -> 5),
        Map("scale" -> 2),
        Map("precision" -> "foo", "scale" -> 2),
        Map("precision" -> 5, "scale" -> "bar")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy values.foldLeft[MetricCalculator](
          new NumberFormattedValuesMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "NumberCastValuesMetricCalculator" must {
    val params: ParamMap = Map.empty

    "return correct metric value and fail counts for single column sequence" in {
      val values = testSingleColSeq(3)
      val metricResult = values.foldLeft[MetricCalculator](new NumberCastValuesMetricCalculator(params))(
        (m, v) => m.increment(Seq(v))
      )
      metricResult.result()("CASTED_NUMBER")._1 shouldEqual 9
      metricResult.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual 6
    }

    "return correct metric value and fail counts for multi column sequence" in {
      val values = testMultiColSeq(3)
      val metricResult = values.foldLeft[MetricCalculator](new NumberCastValuesMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("CASTED_NUMBER")._1 shouldEqual 9
      metricResult.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual 6
    }

    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberCastValuesMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("CASTED_NUMBER")._1 shouldEqual 0
    }
  }

  "NumberInDomainValuesMetricCalculator" must {
    val params: ParamMap = Map(
      "domain" -> Seq(1, 2, 3, 4, 5)
    )
    val results = Seq(8, 4, 5, 5)
    val failCounts = Seq(7, 11, 10, 10)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("NUMBER_IN_DOMAIN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(params))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("NUMBER_IN_DOMAIN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(params))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }

    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberInDomainValuesMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("NUMBER_IN_DOMAIN" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty, Map("notDomain" -> Seq("foo", "bar")), Map("domain" -> "not_a_sequence")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new NumberInDomainValuesMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "NumberOutDomainValuesMetricCalculator" must {
    val params: ParamMap = Map(
      "domain" -> Seq(1, 2, 3, 4, 5)
    )
    val results = Seq(7, 11, 10, 10)
    val failCounts = Seq(8, 4, 5, 5)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("NUMBER_OUT_DOMAIN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("NUMBER_OUT_DOMAIN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(params))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberOutDomainValuesMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("NUMBER_OUT_DOMAIN" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty, Map("notDomain" -> Seq("foo", "bar")), Map("domain" -> "not_a_sequence")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new NumberOutDomainValuesMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "NumberValuesMetricCalculator" must {
    val params: ParamMap = Map(
      "compareValue" -> 3
    )
    val results = Seq(2, 1, 1, 2)
    val failCounts = Seq(13, 14, 14, 13)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("NUMBER_VALUES" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(params))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("NUMBER_VALUES" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(params))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberValuesMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("NUMBER_VALUES" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty, Map("notCompareValue" -> 5), Map("compareValue" -> "not_a_value")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new NumberValuesMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "NumberLessThanMetricCalculator" must {
    val params: ParamMap = Map(
      "compareValue" -> 3,
      "includeBound" -> true
    )
    val results = Seq(7, 4, 5, 4)
    val failCounts = Seq(8, 11, 10, 11)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("NUMBER_LESS_THAN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("NUMBER_LESS_THAN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(params))(
          (m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberLessThanMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("NUMBER_LESS_THAN" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty,
        Map("notCompareValue" -> 5, "includeBound" -> true),
        Map("includeBound" -> true),
        Map("compareValue" -> "foo", "includeBound" -> true)
        // wrong includeBound value just results in using default value
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new NumberLessThanMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "NumberGreaterThanMetricCalculator" must {
    val params: ParamMap = Map(
      "compareValue" -> 3
    )
    val results = Seq(8, 11, 10, 5)
    val failCounts = Seq(7, 4, 5, 10)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("NUMBER_GREATER_THAN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("NUMBER_GREATER_THAN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail count for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(params))(
          (m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberGreaterThanMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("NUMBER_GREATER_THAN" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty,
        Map("notCompareValue" -> 5, "includeBound" -> true),
        Map("includeBound" -> true),
        Map("compareValue" -> "foo", "includeBound" -> true)
        // wrong includeBound value just results in using default value
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new NumberGreaterThanMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "NumberBetweenMetricCalculator" must {
    val params: ParamMap = Map(
      "lowerCompareValue" -> 3,
      "upperCompareValue" -> 6,
      "includeBound" -> true
    )
    val results = Seq(7, 5, 3, 5)
    val failCounts = Seq(8, 10, 12, 10)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberBetweenMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("NUMBER_BETWEEN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberBetweenMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberBetweenMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("NUMBER_BETWEEN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberBetweenMetricCalculator(params))(
          (m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberBetweenMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("NUMBER_BETWEEN" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty,
        Map("notlowerCompareValue" -> 3, "upperCompareValue" -> 6, "includeBound" -> true),
        Map("lowerCompareValue" -> 3, "notUpperCompareValue" -> 6, "includeBound" -> true),
        Map("lowerCompareValue" -> 3),
        Map("notUpperCompareValue" -> 6, "includeBound" -> true),
        Map("lowerCompareValue" -> "foo", "upperCompareValue" -> 6),
        Map("lowerCompareValue" -> 3, "upperCompareValue" -> "bar")
        // wrong includeBound value just results in using default value
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new NumberBetweenMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "NumberNotBetweenMetricCalculator" must {
    val params: ParamMap = Map(
      "lowerCompareValue" -> 2,
      "upperCompareValue" -> 8,
      "includeBound" -> true
    )
    val results = Seq(8, 4, 9, 4)
    val failCounts = Seq(7, 11, 6, 11)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberNotBetweenMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("NUMBER_NOT_BETWEEN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = testSingleColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberNotBetweenMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberNotBetweenMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("NUMBER_NOT_BETWEEN" + getParametrizedMetricTail(params))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = testMultiColSeq zip failCounts
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NumberNotBetweenMetricCalculator(params))(
          (m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NumberNotBetweenMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("NUMBER_NOT_BETWEEN" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty,
        Map("notlowerCompareValue" -> 2, "upperCompareValue" -> 8, "includeBound" -> true),
        Map("lowerCompareValue" -> 2, "notUpperCompareValue" -> 8, "includeBound" -> true),
        Map("lowerCompareValue" -> 2),
        Map("notUpperCompareValue" -> 8, "includeBound" -> true),
        Map("lowerCompareValue" -> "foo", "upperCompareValue" -> 8),
        Map("lowerCompareValue" -> 2, "upperCompareValue" -> "bar")
        // wrong includeBound value just results in using default value
      )
      wrongParams.foreach { params =>
        an[Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new NumberNotBetweenMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "SequenceCompletenessMetricCalculator" must {
    val paramList: Seq[ParamMap] = Seq(
      Map.empty,
      Map("increment" -> 4),
      Map.empty,
      Map("increment" -> 4)
    )
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
        val metricResults = (tt._1, paramList, tt._2).zipped.toList.map(t => (
          t._1.foldLeft[MetricCalculator](new SequenceCompletenessMetricCalculator(t._2))(
            (m, v) => m.increment(Seq(v))).result()("SEQUENCE_COMPLETENESS")._1,
          t._3
        ))
        metricResults.foreach(v => v._1 shouldEqual v._2)
      }
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new SequenceCompletenessMetricCalculator(paramList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("SEQUENCE_COMPLETENESS")._1 shouldEqual 0.0
    }

    "throw assertion error when applied to multi-column sequence" in {
      val values = Seq(
        Seq(Seq("foo", "bar"), Seq("bar", "baz")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s =>
        an [AssertionError] should be thrownBy s.foldLeft[MetricCalculator](
          new SequenceCompletenessMetricCalculator(paramList.head))((m, v) => m.increment(v))
      }
    }
  }
}
