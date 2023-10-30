package ru.raiffeisen.checkita.core.metrics.regular

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.core.metrics.regular.BasicStringMetrics._
import ru.raiffeisen.checkita.core.metrics.{MetricCalculator, MetricName}

class BasicStringMetricsSpec extends AnyWordSpec with Matchers {
  private val testSingleColSeq = Seq(
    Seq("Gpi2C7", "DgXDiA", "Gpi2C7", "Gpi2C7", "M66yO0", "M66yO0", "M66yO0", "xTOn6x", "xTOn6x", "3xGSz0", "3xGSz0", "Gpi2C7"),
    Seq("3.09", "3.09", "6.83", "3.09", "6.83", "3.09", "6.83", "7.28", "2.77", "6.66", "7.28", "2.77"),
    Seq(5.85, 5.85, 5.85, 8.32, 8.32, 7.24, 7.24, 7.24, 8.32, 9.15, 7.24, 5.85),
    Seq(4, 3.14, "foo", 3.0, -25.321, "bar", "[12, 35]", true, '4', '3', "-25.321", "3123dasd")
  )
  private val testMultiColSeq = testSingleColSeq.map(
    s => (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_)))
  )

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

  "DistinctValuesMetricCalculator" must {
    val results = Seq(5, 5, 4, 10)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new DistinctValuesMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.DistinctValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new DistinctValuesMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.DistinctValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new DistinctValuesMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.DistinctValues.entryName)._1 shouldEqual 0
    }
  }
  
  "RegexMatchMetricCalculator" must {
    val regexList: Seq[String] = Seq(
      """^[a-zA-Z]{6}$""",
      """^3\..+""",
      """.*32$""",
      """.*[a-zA-Z]$"""
    )
    val results = Seq(1, 4, 3, 4)
    val failCounts = Seq(11, 8, 9, 8)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, regexList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.RegexMatch.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, regexList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, regexList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()(MetricName.RegexMatch.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, regexList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new RegexMatchMetricCalculator(regexList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.RegexMatch.entryName)._1 shouldEqual 0
    }
  }

  "RegexMismatchMetricCalculator" must {
    val regexList: Seq[String] = Seq(
      """^[a-zA-Z]{6}$""",
      """^3\..+""",
      """.*32$""",
      """.*[a-zA-Z]$"""
    )
    val results = Seq(11, 8, 9, 8)
    val failCounts = Seq(1, 4, 3, 4)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, regexList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.RegexMismatch.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, regexList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, regexList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()(MetricName.RegexMismatch.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, regexList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new RegexMismatchMetricCalculator(regexList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.RegexMismatch.entryName)._1 shouldEqual 0
    }
  }

  "NullValuesMetricCalculator" must {
    val results = Seq.fill(4)(3)

    "return correct metric value for single column sequence" in {
      val values = nullSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NullValuesMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.NullValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = nullSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NullValuesMetricCalculator())((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = nullMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NullValuesMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.NullValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = nullMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new NullValuesMetricCalculator())((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new NullValuesMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.NullValues.entryName)._1 shouldEqual 0
    }
  }

  "EmptyStringValuesMetricCalculator" must {
    val results = Seq.fill(4)(3)

    "return correct metric value for single column sequence" in {
      val values = emptySingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.EmptyValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = emptySingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator())((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = emptyMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.EmptyValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = emptyMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator())((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new EmptyStringValuesMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.EmptyValues.entryName)._1 shouldEqual 0
    }
  }

  "CompletenessMetricCalculator" must {
    val paramList: Seq[Boolean] = Seq(false, false, true)
    val results = Seq(Seq.fill(4)(1.0), Seq.fill(4)(0.75), Seq.fill(4)(0.5))
    val allSingleColSeq = Seq(testSingleColSeq, nullSingleColSeq, emptySingleColSeq)
    val allMultiColSeq = Seq(testMultiColSeq, nullMultiColSeq, emptyMultiColSeq)

    "return correct metric value for single column sequence" in {
      (allSingleColSeq, paramList, results).zipped.toList.foreach { tt =>
        val values = tt._1 zip tt._3
        val metricResults = values.map(t => (
          t._1.foldLeft[MetricCalculator](new CompletenessMetricCalculator(tt._2))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.Completeness.entryName)._1,
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
            (m, v) => m.increment(v)).result()(MetricName.Completeness.entryName)._1,
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
      metricResult.result()(MetricName.Completeness.entryName)._1.isNaN shouldEqual true
    }
  }

  "MinStringValueMetricCalculator" must {
    val results = Seq(6, 4, 4, 1)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MinStringValueMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.MinString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MinStringValueMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.MinString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return max Int value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new MinStringValueMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.MinString.entryName)._1 shouldEqual Int.MaxValue
    }
  }

  "MaxStringValueMetricCalculator" must {
    val results = Seq(6, 4, 4, 8)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MaxStringValueMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.MaxString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new MaxStringValueMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.MaxString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return min Int value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new MaxStringValueMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.MaxString.entryName)._1 shouldEqual Int.MinValue
    }
  }

  "AvgStringValueMetricCalculator" must {
    val results = Seq(6, 4, 4, 4.166666666666667)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new AvgStringValueMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.AvgString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new AvgStringValueMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.AvgString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return NaN when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new AvgStringValueMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.AvgString.entryName)._1.isNaN shouldEqual true
    }
  }

  "DateFormattedValuesMetricCalculator" must {
    // (params, metric_result, failure_count)
    val paramsWithResults: Seq[(String, Int, Int)] = Seq(
      ("yyyy-MM-dd", 2, 10),
      ("EEE, MMM dd, yyyy", 1, 11),
      ("yyyy-MM-dd'T'HH:mm:ss.SSSZ", 1, 11),
      ("h:mm a", 2, 10)
    )
    val valuesSingleCol = Seq(
      "Gpi2C7", "DgXDiA", "2022-03-43", "2001-07-04T12:08:56.235-0700",
      "Wed, Feb 16, 2022", "2021-12-31", "12:08 PM", "xTOn6x", "xTOn6x", "08:44 AM", "3xGSz0", "1999-01-21")
    val valuesMultiCol = (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(valuesSingleCol(_)))

    "return correct metric value for single column sequence" in {
      val metricResults = paramsWithResults.map(t => (
        valuesSingleCol.foldLeft[MetricCalculator](new DateFormattedValuesMetricCalculator(t._1))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.FormattedDate.entryName)._1,
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
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val metricResults = paramsWithResults.map(t => (
        valuesMultiCol.foldLeft[MetricCalculator](new DateFormattedValuesMetricCalculator(t._1))(
          (m, v) => m.increment(v)).result()(MetricName.FormattedDate.entryName)._1,
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
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new DateFormattedValuesMetricCalculator(paramsWithResults.head._1))((m, v) => m.increment(v))
      metricResult.result()(MetricName.FormattedDate.entryName)._1 shouldEqual 0
    }
  }

  "StringLengthValuesMetricCalculator" must {
    val paramList: Seq[(Int, String)] = Seq((6, "eq"), (4, "lte"), (4, "gt"), (5, "gte"))
    val results = Seq(12, 12, 0, 4)
    val failCounts = Seq(0, 0, 12, 8)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringLengthValuesMetricCalculator(t._2._1, t._2._2))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.StringLength.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringLengthValuesMetricCalculator(t._2._1, t._2._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, paramList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringLengthValuesMetricCalculator(t._2._1, t._2._2))(
          (m, v) => m.increment(v)).result()(MetricName.StringLength.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringLengthValuesMetricCalculator(t._2._1, t._2._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new StringLengthValuesMetricCalculator(paramList.head._1, paramList.head._2)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.StringLength.entryName)._1 shouldEqual 0
    }
  }

  "StringInDomainValuesMetricCalculator" must {
    val domainList: Seq[Set[String]] = Seq(
      Set("M66yO0", "DgXDiA"),
      Set("7.28", "2.77"),
      Set("8.32", "9.15"),
      Set("[12, 35]", "true")
    )
    val results = Seq(4, 4, 4, 2)
    val failCounts = Seq(8, 8, 8, 10)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, domainList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringInDomainValuesMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.StringInDomain.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, domainList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringInDomainValuesMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, domainList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringInDomainValuesMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()(MetricName.StringInDomain.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, domainList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringInDomainValuesMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](
        new StringInDomainValuesMetricCalculator(domainList.head)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.StringInDomain.entryName)._1 shouldEqual 0
    }
  }

  "StringOutDomainValuesMetricCalculator" must {
    val domainList: Seq[Set[String]] = Seq(
      Set("M66yO0", "DgXDiA"),
      Set("7.28", "2.77"),
      Set("8.32", "9.15"),
      Set("[12, 35]", "true")
    )
    val results = Seq(8, 8, 8, 10)
    val failCounts = Seq(4, 4, 4, 2)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, domainList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.StringOutDomain.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, domainList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, domainList, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()(MetricName.StringOutDomain.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, domainList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new StringOutDomainValuesMetricCalculator(domainList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.StringOutDomain.entryName)._1 shouldEqual 0
    }
  }

  "StringValuesMetricCalculator" must {
    val compareValues: Seq[String] = Seq("Gpi2C7", "3.09", "5.85", "4")
    val results = Seq(4, 4, 4, 2)
    val failCounts = Seq(8, 8, 8, 10)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, compareValues, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringValuesMetricCalculator(t._2))(
          (m, v) => m.increment(Seq(v))).result()(MetricName.StringValues.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence" in {
      val values = (testSingleColSeq, compareValues, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringValuesMetricCalculator(t._2))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, compareValues, results).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringValuesMetricCalculator(t._2))(
          (m, v) => m.increment(v)).result()(MetricName.StringValues.entryName)._1,
        t._3
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence" in {
      val values = (testMultiColSeq, compareValues, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new StringValuesMetricCalculator(t._2))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new StringValuesMetricCalculator(compareValues.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.StringValues.entryName)._1 shouldEqual 0
    }
  }
}
