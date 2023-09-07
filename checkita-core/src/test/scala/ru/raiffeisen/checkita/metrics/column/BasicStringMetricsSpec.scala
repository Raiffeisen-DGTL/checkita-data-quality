package ru.raiffeisen.checkita.metrics.column

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.metrics.{MetricCalculator, StatusableCalculator}
import ru.raiffeisen.checkita.metrics.MetricProcessor.ParamMap
import ru.raiffeisen.checkita.metrics.column.BasicStringMetrics._
import ru.raiffeisen.checkita.utils.getParametrizedMetricTail


class BasicStringMetricsSpec extends AnyWordSpec with Matchers {
  private val testSingleColSeq = Seq(
    Seq("Gpi2C7", "DgXDiA", "Gpi2C7", "Gpi2C7", "M66yO0", "M66yO0", "M66yO0", "xTOn6x", "xTOn6x", "3xGSz0", "3xGSz0", "Gpi2C7"),
    Seq("3.09", "3.09", "6.83", "3.09", "6.83", "3.09", "6.83", "7.28", "2.77", "6.66", "7.28", "2.77"),
    Seq(5.85, 5.85, 5.85, 8.32, 8.32, 7.24, 7.24, 7.24, 8.32, 9.15, 7.24, 5.85),
    Seq(4, 3.14, "foo", 3.0, -25.321, "bar", "[12, 35]", true, '4', '3', "-25.321", "3123dasd")
  )
  private val testMultiColSeq = testSingleColSeq.map(s => (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_))))

  private val nullIndices = Set(3, 7, 9)
  private val emptyIndices = Set(1, 5, 8)
  private val nullSingleColSeq = testSingleColSeq.map(s => s.zipWithIndex.map {
    case (_, idx) if nullIndices.contains(idx) => null
    case (v, _) => v
  })
  private val emptySingleColSeq = nullSingleColSeq.map(s => s.zipWithIndex.map {
    case (_, idx) if emptyIndices.contains(idx) => ""
    case (v, _) => v
  })
  private val nullMultiColSeq = nullSingleColSeq.map(s => (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_))))
  private val emptyMultiColSeq = emptySingleColSeq.map(s => (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_))))

  "UniqueValuesMetricCalculator" must {
    val params: ParamMap = Map.empty
    val results = Seq(5, 5, 4, 10)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new UniqueValuesMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("DISTINCT_VALUES")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new UniqueValuesMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("DISTINCT_VALUES")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new UniqueValuesMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("DISTINCT_VALUES")._1 shouldEqual 0
    }
  }

  "RegexMatchMetricCalculator" must {
    val paramList: Seq[ParamMap] = Seq(
      Map("regex" -> """^[a-zA-Z]{6}$"""),
      Map("regex" -> """^3\..+"""),
      Map("regex" -> """.*32$"""),
      Map("regex" -> """.*[a-zA-Z]$""")
    )
    val results = Seq(1, 4, 3, 4)
    val failCounts = Seq(11, 8, 9, 8)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()("REGEX_MATCH" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()("REGEX_MATCH" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(paramList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("REGEX_MATCH" + getParametrizedMetricTail(paramList.head))._1 shouldEqual 0
    }
    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty, Map("notRegex" -> """32$""")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new RegexMatchMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "RegexMismatchMetricCalculator" must {
    val paramList: Seq[ParamMap] = Seq(
      Map("regex" -> """^[a-zA-Z]{6}$"""),
      Map("regex" -> """^3\..+"""),
      Map("regex" -> """.*32$"""),
      Map("regex" -> """.*[a-zA-Z]$""")
    )
    val results = Seq(11, 8, 9, 8)
    val failCounts = Seq(1, 4, 3, 4)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()("REGEX_MISMATCH" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()("REGEX_MISMATCH" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(paramList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("REGEX_MISMATCH" + getParametrizedMetricTail(paramList.head))._1 shouldEqual 0
    }
    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty, Map("notRegex" -> """32$""")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new RegexMismatchMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "NullValuesMetricCalculator" must {
    val params: ParamMap = Map.empty
    val results = Seq.fill(4)(3)

    "return correct metric value for single column sequence" in {
      val values = nullSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NullValuesMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("NULL_VALUES")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = nullSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NullValuesMetricCalculator(params))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = nullMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NullValuesMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("NULL_VALUES")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = nullMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NullValuesMetricCalculator(params))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NullValuesMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("NULL_VALUES")._1 shouldEqual 0
    }
  }

  "EmptyStringValuesMetricCalculator" must {
    val params: ParamMap = Map.empty
    val results = Seq.fill(4)(3)

    "return correct metric value for single column sequence" in {
      val values = emptySingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("EMPTY_VALUES")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = emptySingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator(params))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = emptyMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("EMPTY_VALUES")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = emptyMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator(params))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("EMPTY_VALUES")._1 shouldEqual 0
    }
  }

  "CompletenessMetricCalculator" must {
    val paramList: Seq[ParamMap] = Seq(
      Map.empty, Map.empty, Map("includeEmptyStrings" -> true)
    )
    val results = Seq(Seq.fill(4)(1.0), Seq.fill(4)(0.75), Seq.fill(4)(0.5))
    val allSingleColSeq = Seq(testSingleColSeq, nullSingleColSeq, emptySingleColSeq)
    val allMultiColSeq = Seq(testMultiColSeq, nullMultiColSeq, emptyMultiColSeq)

    "return correct metric value for single column sequence" in {
      (allSingleColSeq, paramList, results).zipped.toList.foreach { tt =>
        val values = tt._1 zip tt._3
        val metricResults = values.map(t => (
          t._1.foldLeft[MetricCalculator](new CompletenessMetricCalculator(tt._2))(
            (m, v) => m.increment(Seq(v))).result()("COMPLETENESS")._1,
          t._2
        ))
        metricResults.foreach(v => v._1 shouldEqual v._2)
      }
    }
    "return correct metric value for multi column sequence" in {
      (allMultiColSeq, paramList, results).zipped.toList.foreach { tt =>
        val values = tt._1 zip tt._3
        val metricResults = values.map(t => (
          t._1.foldLeft[MetricCalculator](new CompletenessMetricCalculator(tt._2))(
            (m, v) => m.increment(v)).result()("COMPLETENESS")._1,
          t._2
        ))
        metricResults.foreach(v => v._1 shouldEqual v._2)
      }
    }
    "return NaN when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new CompletenessMetricCalculator(paramList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("COMPLETENESS")._1.isNaN shouldEqual true
    }
  }

  "MinStringValueMetricCalculator" must {
    val params: ParamMap = Map.empty
    val results = Seq(6, 4, 4, 1)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MinStringValueMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("MIN_STRING")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MinStringValueMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("MIN_STRING")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return max Int value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new MinStringValueMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("MIN_STRING")._1 shouldEqual Int.MaxValue
    }
  }

  "MaxStringValueMetricCalculator" must {
    val params: ParamMap = Map.empty
    val results = Seq(6, 4, 4, 8)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MaxStringValueMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("MAX_STRING")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MaxStringValueMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("MAX_STRING")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return min Int value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new MaxStringValueMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("MAX_STRING")._1 shouldEqual Int.MinValue
    }
  }

  "AvgStringValueMetricCalculator" must {
    val params: ParamMap = Map.empty
    val results = Seq(6, 4, 4, 4.166666666666667)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new AvgStringValueMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result()("AVG_STRING")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new AvgStringValueMetricCalculator(params))(
          (m, v) => m.increment(v)).result()("AVG_STRING")._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return NaN when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new AvgStringValueMetricCalculator(params))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("AVG_STRING")._1.isNaN shouldEqual true
    }
  }

  "DateFormattedValuesMetricCalculator" must {
    // (params, metric_result, failure_count)
    val paramsWithResults: Seq[(ParamMap, Int, Int)] = Seq(
      (Map("dateFormat" -> "yyyy-MM-dd"), 2, 10),
      (Map("dateFormat" -> "EEE, MMM dd, yyyy"), 1, 11),
      (Map("dateFormat" -> "yyyy-MM-dd'T'HH:mm:ss.SSSZ"), 1, 11),
      (Map("dateFormat" -> "h:mm a"), 2, 10)
    )
    val valuesSingleCol = Seq(
      "Gpi2C7", "DgXDiA", "2022-03-43", "2001-07-04T12:08:56.235-0700",
      "Wed, Feb 16, 2022", "2021-12-31", "12:08 PM", "xTOn6x", "xTOn6x", "08:44 AM", "3xGSz0", "1999-01-21")
    val valuesMultiCol = (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(valuesSingleCol(_)))

    "return correct metric value for single column sequence" in {
      val metricResults = paramsWithResults.map(t => (
        valuesSingleCol.foldLeft[MetricCalculator](new DateFormattedValuesMetricCalculator(t._1))(
          (m, v) => m.increment(Seq(v))).result()("FORMATTED_DATE" + getParametrizedMetricTail(t._1))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct failure counts for single column sequence" in {
      val metricResults = paramsWithResults.map(t => (
        valuesSingleCol.foldLeft[MetricCalculator](new DateFormattedValuesMetricCalculator(t._1))(
          (m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val metricResults = paramsWithResults.map(t => (
        valuesMultiCol.foldLeft[MetricCalculator](new DateFormattedValuesMetricCalculator(t._1))(
          (m, v) => m.increment(v)).result()("FORMATTED_DATE" + getParametrizedMetricTail(t._1))._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct failure counts for multi column sequence" in {
      val metricResults = paramsWithResults.map(t => (
        valuesMultiCol.foldLeft[MetricCalculator](new DateFormattedValuesMetricCalculator(t._1))(
          (m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new DateFormattedValuesMetricCalculator(paramsWithResults.head._1))((m, v) => m.increment(v))
      metricResult.result()(
        "FORMATTED_DATE" + getParametrizedMetricTail(paramsWithResults.head._1))._1 shouldEqual 0
    }
  }

  "StringLengthValuesMetricCalculator" must {
    val paramList: Seq[ParamMap] = Seq(
      Map("length" -> 6, "compareRule" -> "eq"),
      Map("length" -> 4, "compareRule" -> "lte"),
      Map("length" -> 4, "compareRule" -> "gt"),
      Map("length" -> 5, "compareRule" -> "gte")
    )
    val results = Seq(12, 12, 0, 4)
    val failCounts = Seq(0, 0, 12, 8)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringLengthValuesMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()("STRING_LENGTH" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringLengthValuesMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringLengthValuesMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()("STRING_LENGTH" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringLengthValuesMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new StringLengthValuesMetricCalculator(paramList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("STRING_LENGTH" + getParametrizedMetricTail(paramList.head))._1 shouldEqual 0
    }
    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty,
        Map("notLength" -> 6, "compareRule" -> "eq"),
        Map("length" -> 6, "notCompareRule" -> "eq"),
        Map("length" -> 6),
        Map("compareRule" -> "eq"),
        Map("length" -> "not-an-int-number", "compareRule" -> "eq"),
        Map("length" -> 6, "compareRule" -> "not-a-compare-rule")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new StringLengthValuesMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "StringInDomainValuesMetricCalculator" must {
    val paramList: Seq[ParamMap] = Seq(
      Map("domain" -> Seq("M66yO0", "DgXDiA")),
      Map("domain" -> Seq("7.28", "2.77")),
      Map("domain" -> Seq("8.32", "9.15")),
      Map("domain" -> Seq("[12, 35]", "true"))
    )
    val results = Seq(4, 4, 4, 2)
    val failCounts = Seq(8, 8, 8, 10)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringInDomainValuesMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()("STRING_IN_DOMAIN" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringInDomainValuesMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringInDomainValuesMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()("STRING_IN_DOMAIN" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringInDomainValuesMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new StringInDomainValuesMetricCalculator(paramList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("STRING_IN_DOMAIN" + getParametrizedMetricTail(paramList.head))._1 shouldEqual 0
    }
    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty, Map("notDomain" -> Seq("foo", "bar")), Map("domain" -> "not-a-sequence")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new StringInDomainValuesMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "StringOutDomainValuesMetricCalculator" must {
    val paramList: Seq[ParamMap] = Seq(
      Map("domain" -> Seq("M66yO0", "DgXDiA")),
      Map("domain" -> Seq("7.28", "2.77")),
      Map("domain" -> Seq("8.32", "9.15")),
      Map("domain" -> Seq("[12, 35]", "true"))
    )
    val results = Seq(8, 8, 8, 10)
    val failCounts = Seq(4, 4, 4, 2)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()("STRING_OUT_DOMAIN" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()("STRING_OUT_DOMAIN" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(paramList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("STRING_OUT_DOMAIN" + getParametrizedMetricTail(paramList.head))._1 shouldEqual 0
    }
    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty, Map("notDomain" -> Seq("foo", "bar")), Map("domain" -> "not-a-sequence")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new StringOutDomainValuesMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }

  "StringValuesMetricCalculator" must {
    val paramList: Seq[ParamMap] = Seq(
      Map("compareValue" -> "Gpi2C7"),
      Map("compareValue" -> "3.09"),
      Map("compareValue" -> "5.85"),
      Map("compareValue" -> "4")
    )
    val results = Seq(4, 4, 4, 2)
    val failCounts = Seq(8, 8, 8, 10)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringValuesMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()("STRING_VALUES" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringValuesMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringValuesMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()("STRING_VALUES" + getParametrizedMetricTail(t._2))._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringValuesMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.asInstanceOf[StatusableCalculator].getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new StringValuesMetricCalculator(paramList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("STRING_VALUES" + getParametrizedMetricTail(paramList.head))._1 shouldEqual 0
    }
    "throw exception when params are empty or wrong" in {
      val wrongParams: Seq[ParamMap] = Seq(
        Map.empty, Map("notCompareValue" -> "foo")
      )
      wrongParams.foreach { params =>
        an [Exception] should be thrownBy testSingleColSeq.head.foldLeft[MetricCalculator](
          new StringValuesMetricCalculator(params))((m, v) => m.increment(Seq(v)))
      }
    }
  }
}
