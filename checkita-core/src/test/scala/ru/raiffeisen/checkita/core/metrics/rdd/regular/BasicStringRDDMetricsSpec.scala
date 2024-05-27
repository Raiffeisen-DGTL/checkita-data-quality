package ru.raiffeisen.checkita.core.metrics.rdd.regular

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common.checkSerDe
import ru.raiffeisen.checkita.core.metrics.MetricName
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricCalculator
import ru.raiffeisen.checkita.core.metrics.rdd.regular.BasicNumericRDDMetrics.TDigestRDDMetricCalculator
import ru.raiffeisen.checkita.core.metrics.rdd.regular.BasicStringRDDMetrics._
import ru.raiffeisen.checkita.core.serialization.Implicits._

class BasicStringRDDMetricsSpec extends AnyWordSpec with Matchers {
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

  "DistinctValuesRDDMetricCalculator" must {
    val results = Seq(5, 5, 4, 10)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new DistinctValuesRDDMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.DistinctValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new DistinctValuesRDDMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.DistinctValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new DistinctValuesRDDMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.DistinctValues.entryName)._1 shouldEqual 0
    }
    
    "be serializable for buffer checkpointing" in {
      testSingleColSeq.map(v => v.foldLeft[RDDMetricCalculator](
        new DistinctValuesRDDMetricCalculator()
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }
  
  "DuplicateValuesRDDMetricCalculator" must {
    "return correct metric value for single column sequence" in {
      val results = Seq(7, 7, 8, 2)
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new DuplicateValuesRDDMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.DuplicateValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }

    "return correct metric value for multi column sequence" in {
      val results = Seq(1, 2, 0, 0)
      val data = Seq(
        Seq(
          Seq("Gpi2C7", "DgXDiA", "Gpi2C7"),
          Seq("M66yO0", null, "xTOn6x"),
          Seq("M66yO0", "xTOn6x", null),
          Seq("Gpi2C7", "DgXDiA", "Gpi2C7")   
        ),
        Seq(
          Seq(3.09, 3.09, 6.83),
          Seq(6.83, 7, 2.77),
          Seq(6.83, 7.00, 2.77),
          Seq(3.09, 3.09, 6.83),        
        ),
        Seq(
          Seq(5.85, 5.85, 5.85),
          Seq(8.32, 8.32, 7.24),
          Seq(7.24, 7.24, 8.32),
          Seq(9.15, 7.24, 5.85)
        ),
        Seq(
          Seq('4', "3.14", "foo"),
          Seq(3.0, "true", "4"),
          Seq(3, true, 4),
          Seq(4.0, 3.14, "foo")          
        )
      )
      val values = data zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new DuplicateValuesRDDMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.DuplicateValues.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new DuplicateValuesRDDMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.DuplicateValues.entryName)._1 shouldEqual 0
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.map(v => v.foldLeft[RDDMetricCalculator](
        new DuplicateValuesRDDMetricCalculator()
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "RegexMatchRDDMetricCalculator" must {
    val regexList: Seq[String] = Seq(
      """^[a-zA-Z]{6}$""",
      """^3\..+""",
      """.*32$""",
      """.*[a-zA-Z]$"""
    )
    val results = Seq(1, 4, 3, 4)
    val failCounts = Seq(11, 8, 9, 8)
    val failCountsRev = Seq(1, 4, 3, 4)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, regexList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new RegexMatchRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.RegexMatch.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [direct error collection]" in {
      val values = (testSingleColSeq, regexList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new RegexMatchRDDMetricCalculator(t._2, false))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [reversed error collection]" in {
      val values = (testSingleColSeq, regexList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new RegexMatchRDDMetricCalculator(t._2, true))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, regexList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new RegexMatchRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(v)).result()(MetricName.RegexMatch.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [direct error collection]" in {
      val values = (testMultiColSeq, regexList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new RegexMatchRDDMetricCalculator(t._2, false))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [reversed error collection]" in {
      val values = (testMultiColSeq, regexList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new RegexMatchRDDMetricCalculator(t._2, true))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new RegexMatchRDDMetricCalculator(regexList.head, false))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.RegexMatch.entryName)._1 shouldEqual 0
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.zip(regexList).map(v => v._1.foldLeft[RDDMetricCalculator](
        new RegexMatchRDDMetricCalculator(v._2, false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "RegexMismatchRDDMetricCalculator" must {
    val regexList: Seq[String] = Seq(
      """^[a-zA-Z]{6}$""",
      """^3\..+""",
      """.*32$""",
      """.*[a-zA-Z]$"""
    )
    val results = Seq(11, 8, 9, 8)
    val failCounts = Seq(1, 4, 3, 4)
    val failCountsRev = Seq(11, 8, 9, 8)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, regexList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new RegexMismatchRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.RegexMismatch.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [direct error collection]" in {
      val values = (testSingleColSeq, regexList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new RegexMismatchRDDMetricCalculator(t._2, false))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [reversed error collection]" in {
      val values = (testSingleColSeq, regexList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new RegexMismatchRDDMetricCalculator(t._2, true))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, regexList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new RegexMismatchRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(v)).result()(MetricName.RegexMismatch.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [direct error collection]" in {
      val values = (testMultiColSeq, regexList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new RegexMismatchRDDMetricCalculator(t._2, false))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [reversed error collection]" in {
      val values = (testMultiColSeq, regexList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new RegexMismatchRDDMetricCalculator(t._2, true))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new RegexMismatchRDDMetricCalculator(regexList.head, false))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.RegexMismatch.entryName)._1 shouldEqual 0
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.zip(regexList).map(v => v._1.foldLeft[RDDMetricCalculator](
        new RegexMismatchRDDMetricCalculator(v._2, false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "NullValuesRDDMetricCalculator" must {
    val results = Seq.fill(4)(3)
    val failCount = Seq.fill(4)(9)
    val failCountRev = Seq.fill(4)(3)

    "return correct metric value for single column sequence" in {
      val values = nullSingleColSeq zip results
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new NullValuesRDDMetricCalculator(reversed))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.NullValues.entryName)._1,
          t._2
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [direct error collection]" in {
      val values = nullSingleColSeq zip failCount
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new NullValuesRDDMetricCalculator(false))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [reversed error collection]" in {
      val values = nullSingleColSeq zip failCountRev
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new NullValuesRDDMetricCalculator(true))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = nullMultiColSeq zip results
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new NullValuesRDDMetricCalculator(reversed))(
            (m, v) => m.increment(v)).result()(MetricName.NullValues.entryName)._1,
          t._2
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [direct error collection]" in {
      val values = nullMultiColSeq zip failCount
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new NullValuesRDDMetricCalculator(false))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [reversed error collection]" in {
      val values = nullMultiColSeq zip failCountRev
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new NullValuesRDDMetricCalculator(true))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new NullValuesRDDMetricCalculator(false))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.NullValues.entryName)._1 shouldEqual 0
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.map(v => v.foldLeft[RDDMetricCalculator](
        new NullValuesRDDMetricCalculator(false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "EmptyValuesRDDMetricCalculator" must {
    val results = Seq.fill(4)(3)
    val failCount = Seq.fill(4)(9)
    val failCountRev = Seq.fill(4)(3)

    "return correct metric value for single column sequence" in {
      val values = emptySingleColSeq zip results
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new EmptyValuesRDDMetricCalculator(reversed))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.EmptyValues.entryName)._1,
          t._2
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [direct error collection]" in {
      val values = emptySingleColSeq zip failCount
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new EmptyValuesRDDMetricCalculator(false))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [reversed error collection]" in {
      val values = emptySingleColSeq zip failCountRev
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new EmptyValuesRDDMetricCalculator(true))((m, v) => m.increment(Seq(v))),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = emptyMultiColSeq zip results
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new EmptyValuesRDDMetricCalculator(reversed))(
            (m, v) => m.increment(v)).result()(MetricName.EmptyValues.entryName)._1,
          t._2
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [direct error collection]" in {
      val values = emptyMultiColSeq zip failCount
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new EmptyValuesRDDMetricCalculator(false))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [reversed error collection]" in {
      val values = emptyMultiColSeq zip failCountRev
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new EmptyValuesRDDMetricCalculator(true))((m, v) => m.increment(v)),
        t._2
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new EmptyValuesRDDMetricCalculator(false))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.EmptyValues.entryName)._1 shouldEqual 0
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.map(v => v.foldLeft[RDDMetricCalculator](
        new EmptyValuesRDDMetricCalculator(false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "CompletenessRDDMetricCalculator" must {
    val paramList: Seq[Boolean] = Seq(false, false, true)
    val results = Seq(Seq.fill(4)(1.0), Seq.fill(4)(0.75), Seq.fill(4)(0.5))
    val failCount = Seq(Seq.fill(4)(12), Seq.fill(4)(9), Seq.fill(4)(6))
    val failCountRev = Seq(Seq.fill(4)(0), Seq.fill(4)(3), Seq.fill(4)(6))
    val allSingleColSeq = Seq(testSingleColSeq, nullSingleColSeq, emptySingleColSeq)
    val allMultiColSeq = Seq(testMultiColSeq, nullMultiColSeq, emptyMultiColSeq)

    "return correct metric value for single column sequence" in {
      (allSingleColSeq, paramList, results).zipped.toList.foreach { tt =>
        val values = tt._1 zip tt._3
        val metricResults = for {
          reversed <- Seq(false, true)
          metricResult <- values.map(t => (
            t._1.foldLeft[RDDMetricCalculator](new CompletenessRDDMetricCalculator(tt._2, reversed))(
              (m, v) => m.increment(Seq(v))).result()(MetricName.Completeness.entryName)._1,
            t._2
          ))
        } yield metricResult
        metricResults.foreach(v => v._1 shouldEqual v._2)
      }
    }
    "return correct fail counts for single column sequence [direct error collection]" in {
      (allSingleColSeq, paramList, failCount).zipped.toList.foreach { tt =>
        val values = tt._1 zip tt._3
        val metricResults = values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new CompletenessRDDMetricCalculator(tt._2, false))(
            (m, v) => m.increment(Seq(v))
          ),
          t._2
        ))
        metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
      }
    }
    "return correct fail counts for single column sequence [reversed error collection]" in {
      (allSingleColSeq, paramList, failCountRev).zipped.toList.foreach { tt =>
        val values = tt._1 zip tt._3
        val metricResults = values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new CompletenessRDDMetricCalculator(tt._2, true))(
            (m, v) => m.increment(Seq(v))
          ),
          t._2
        ))
        metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
      }
    }
    "return correct metric value for multi column sequence" in {
      (allMultiColSeq, paramList, results).zipped.toList.foreach { tt =>
        val values = tt._1 zip tt._3
        val metricResults = for {
          reversed <- Seq(false, true)
          metricResult <- values.map(t => (
            t._1.foldLeft[RDDMetricCalculator](new CompletenessRDDMetricCalculator(tt._2, reversed))(
              (m, v) => m.increment(v)).result()(MetricName.Completeness.entryName)._1,
            t._2
          ))
        } yield metricResult
        metricResults.foreach(v => v._1 shouldEqual v._2)
      }
    }
    "return correct fail counts for multi column sequence [direct error collection]" in {
      (allMultiColSeq, paramList, failCount).zipped.toList.foreach { tt =>
        val values = tt._1 zip tt._3
        val metricResults = values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new CompletenessRDDMetricCalculator(tt._2, false))((m, v) => m.increment(v)),
          t._2
        ))
        metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
      }
    }
    "return correct fail counts for multi column sequence [reversed error collection]" in {
      (allMultiColSeq, paramList, failCountRev).zipped.toList.foreach { tt =>
        val values = tt._1 zip tt._3
        val metricResults = values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new CompletenessRDDMetricCalculator(tt._2, true))((m, v) => m.increment(v)),
          t._2
        ))
        metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
      }
    }
    "return NaN when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new CompletenessRDDMetricCalculator(paramList.head, false))(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.Completeness.entryName)._1.isNaN shouldEqual true
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.map(v => v.foldLeft[RDDMetricCalculator](
        new CompletenessRDDMetricCalculator(paramList.head, false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "MinStringRDDMetricCalculator" must {
    val results = Seq(6, 4, 4, 1)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new MinStringRDDMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.MinString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new MinStringRDDMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.MinString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return max Int value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new MinStringRDDMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.MinString.entryName)._1 shouldEqual Int.MaxValue
    }
    
    "be serializable for buffer checkpointing" in {
      testSingleColSeq.map(v => v.foldLeft[RDDMetricCalculator](
        new MinStringRDDMetricCalculator()
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "MaxStringRDDMetricCalculator" must {
    val results = Seq(6, 4, 4, 8)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new MaxStringRDDMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.MaxString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new MaxStringRDDMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.MaxString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return min Int value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new MaxStringRDDMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.MaxString.entryName)._1 shouldEqual Int.MinValue
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.map(v => v.foldLeft[RDDMetricCalculator](
        new MaxStringRDDMetricCalculator()
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "AvgStringRDDMetricCalculator" must {
    val results = Seq(6, 4, 4, 4.166666666666667)

    "return correct metric value for single column sequence" in {
      val values = testSingleColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new AvgStringRDDMetricCalculator())(
          (m, v) => m.increment(Seq(v))).result()(MetricName.AvgString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = testMultiColSeq zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new AvgStringRDDMetricCalculator())(
          (m, v) => m.increment(v)).result()(MetricName.AvgString.entryName)._1,
        t._2
      ))
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return NaN when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](new AvgStringRDDMetricCalculator())(
        (m, v) => m.increment(v)
      )
      metricResult.result()(MetricName.AvgString.entryName)._1.isNaN shouldEqual true
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.map(v => v.foldLeft[RDDMetricCalculator](
        new AvgStringRDDMetricCalculator()
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "FormattedDateRDDMetricCalculator" must {
    // (params, metric_result, failure_count, reversed_failure_count)
    val paramsWithResults: Seq[(String, Int, Int, Int)] = Seq(
      ("yyyy-MM-dd", 2, 10, 2),
      ("EEE, MMM dd, yyyy", 1, 11, 1),
      ("yyyy-MM-dd'T'HH:mm:ss.SSSZ", 1, 11, 1),
      ("h:mm a", 2, 10, 2)
    )
    val valuesSingleCol = Seq(
      "Gpi2C7", "DgXDiA", "2022-03-43", "2001-07-04T12:08:56.235-0700",
      "Wed, Feb 16, 2022", "2021-12-31", "12:08 PM", "xTOn6x", "xTOn6x", "08:44 AM", "3xGSz0", "1999-01-21")
    val valuesMultiCol = (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(valuesSingleCol(_)))

    "return correct metric value for single column sequence" in {
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- paramsWithResults.map(t => (
          valuesSingleCol.foldLeft[RDDMetricCalculator](new FormattedDateRDDMetricCalculator(t._1, reversed))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.FormattedDate.entryName)._1,
          t._2
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct failure counts for single column sequence [direct error collection]" in {
      val metricResults = paramsWithResults.map(t => (
        valuesSingleCol.foldLeft[RDDMetricCalculator](new FormattedDateRDDMetricCalculator(t._1, false))(
          (m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct failure counts for single column sequence [reversed error collection]" in {
      val metricResults = paramsWithResults.map(t => (
        valuesSingleCol.foldLeft[RDDMetricCalculator](new FormattedDateRDDMetricCalculator(t._1, true))(
          (m, v) => m.increment(Seq(v))),
        t._4
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- paramsWithResults.map(t => (
          valuesMultiCol.foldLeft[RDDMetricCalculator](new FormattedDateRDDMetricCalculator(t._1, reversed))(
            (m, v) => m.increment(v)).result()(MetricName.FormattedDate.entryName)._1,
          t._2
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct failure counts for multi column sequence [direct error collection]" in {
      val metricResults = paramsWithResults.map(t => (
        valuesMultiCol.foldLeft[RDDMetricCalculator](new FormattedDateRDDMetricCalculator(t._1, false))(
          (m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct failure counts for multi column sequence [reversed error collection]" in {
      val metricResults = paramsWithResults.map(t => (
        valuesMultiCol.foldLeft[RDDMetricCalculator](new FormattedDateRDDMetricCalculator(t._1, true))(
          (m, v) => m.increment(v)),
        t._4
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](
        new FormattedDateRDDMetricCalculator(paramsWithResults.head._1, false))((m, v) => m.increment(v))
      metricResult.result()(MetricName.FormattedDate.entryName)._1 shouldEqual 0
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.zip(paramsWithResults).map(v => v._1.foldLeft[RDDMetricCalculator](
        new FormattedDateRDDMetricCalculator(v._2._1, false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "StringLengthRDDMetricCalculator" must {
    val paramList: Seq[(Int, String)] = Seq((6, "eq"), (4, "lte"), (4, "gt"), (5, "gte"))
    val results = Seq(12, 12, 0, 4)
    val failCounts = Seq(0, 0, 12, 8)
    val failCountsRev = Seq(12, 12, 0, 4)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, paramList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new StringLengthRDDMetricCalculator(t._2._1, t._2._2, reversed))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.StringLength.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [direct error collection]" in {
      val values = (testSingleColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringLengthRDDMetricCalculator(t._2._1, t._2._2, false))(
          (m, v) => m.increment(Seq(v))
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [reversed error collection]" in {
      val values = (testSingleColSeq, paramList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringLengthRDDMetricCalculator(t._2._1, t._2._2, true))(
          (m, v) => m.increment(Seq(v))
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, paramList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new StringLengthRDDMetricCalculator(t._2._1, t._2._2, reversed))(
            (m, v) => m.increment(v)).result()(MetricName.StringLength.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [direct error collection]" in {
      val values = (testMultiColSeq, paramList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringLengthRDDMetricCalculator(t._2._1, t._2._2, false))(
          (m, v) => m.increment(v)
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [reversed error collection]" in {
      val values = (testMultiColSeq, paramList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringLengthRDDMetricCalculator(t._2._1, t._2._2, true))(
          (m, v) => m.increment(v)
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](
        new StringLengthRDDMetricCalculator(paramList.head._1, paramList.head._2, false)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.StringLength.entryName)._1 shouldEqual 0
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.zip(paramList).map(v => v._1.foldLeft[RDDMetricCalculator](
        new StringLengthRDDMetricCalculator(v._2._1, v._2._2, false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "StringInDomainRDDMetricCalculator" must {
    val domainList: Seq[Set[String]] = Seq(
      Set("M66yO0", "DgXDiA"),
      Set("7.28", "2.77"),
      Set("8.32", "9.15"),
      Set("[12, 35]", "true")
    )
    val results = Seq(4, 4, 4, 2)
    val failCounts = Seq(8, 8, 8, 10)
    val failCountsRev = Seq(4, 4, 4, 2)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, domainList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new StringInDomainRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.StringInDomain.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [direct error collection]" in {
      val values = (testSingleColSeq, domainList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringInDomainRDDMetricCalculator(t._2, false))(
          (m, v) => m.increment(Seq(v))
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [reversed error collection]" in {
      val values = (testSingleColSeq, domainList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringInDomainRDDMetricCalculator(t._2, true))(
          (m, v) => m.increment(Seq(v))
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, domainList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new StringInDomainRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(v)).result()(MetricName.StringInDomain.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [direct error collection]" in {
      val values = (testMultiColSeq, domainList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringInDomainRDDMetricCalculator(t._2, false))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [reversed error collection]" in {
      val values = (testMultiColSeq, domainList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringInDomainRDDMetricCalculator(t._2, true))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](
        new StringInDomainRDDMetricCalculator(domainList.head, false)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.StringInDomain.entryName)._1 shouldEqual 0
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.zip(domainList).map(v => v._1.foldLeft[RDDMetricCalculator](
        new StringInDomainRDDMetricCalculator(v._2, false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "StringOutDomainRDDMetricCalculator" must {
    val domainList: Seq[Set[String]] = Seq(
      Set("M66yO0", "DgXDiA"),
      Set("7.28", "2.77"),
      Set("8.32", "9.15"),
      Set("[12, 35]", "true")
    )
    val results = Seq(8, 8, 8, 10)
    val failCounts = Seq(4, 4, 4, 2)
    val failCountsRev = Seq(8, 8, 8, 10)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, domainList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new StringOutDomainRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.StringOutDomain.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [direct error collection]" in {
      val values = (testSingleColSeq, domainList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringOutDomainRDDMetricCalculator(t._2, false))(
          (m, v) => m.increment(Seq(v))
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [reversed error collection]" in {
      val values = (testSingleColSeq, domainList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringOutDomainRDDMetricCalculator(t._2, true))(
          (m, v) => m.increment(Seq(v))
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, domainList, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new StringOutDomainRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(v)).result()(MetricName.StringOutDomain.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [direct error collection]" in {
      val values = (testMultiColSeq, domainList, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringOutDomainRDDMetricCalculator(t._2, false))(
          (m, v) => m.increment(v)
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [reversed error collection]" in {
      val values = (testMultiColSeq, domainList, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringOutDomainRDDMetricCalculator(t._2, true))(
          (m, v) => m.increment(v)
        ),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](
        new StringOutDomainRDDMetricCalculator(domainList.head, false)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.StringOutDomain.entryName)._1 shouldEqual 0
    }
    
    "be serializable for buffer checkpointing" in {
      testSingleColSeq.zip(domainList).map(v => v._1.foldLeft[RDDMetricCalculator](
        new StringOutDomainRDDMetricCalculator(v._2, false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "StringValuesRDDMetricCalculator" must {
    val compareValues: Seq[String] = Seq("Gpi2C7", "3.09", "5.85", "4")
    val results = Seq(4, 4, 4, 2)
    val failCounts = Seq(8, 8, 8, 10)
    val failCountsRev = Seq(4, 4, 4, 2)

    "return correct metric value for single column sequence" in {
      val values = (testSingleColSeq, compareValues, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new StringValuesRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.StringValues.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [direct error collection]" in {
      val values = (testSingleColSeq, compareValues, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringValuesRDDMetricCalculator(t._2, false))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for single column sequence [reversed error collection]" in {
      val values = (testSingleColSeq, compareValues, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringValuesRDDMetricCalculator(t._2, true))((m, v) => m.increment(Seq(v))),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct metric value for multi column sequence" in {
      val values = (testMultiColSeq, compareValues, results).zipped.toList
      val metricResults = for {
        reversed <- Seq(false, true)
        metricResult <- values.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new StringValuesRDDMetricCalculator(t._2, reversed))(
            (m, v) => m.increment(v)).result()(MetricName.StringValues.entryName)._1,
          t._3
        ))
      } yield metricResult
      metricResults.foreach(v => v._1 shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [direct error collection]" in {
      val values = (testMultiColSeq, compareValues, failCounts).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringValuesRDDMetricCalculator(t._2, false))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return correct fail counts for multi column sequence [reversed error collection]" in {
      val values = (testMultiColSeq, compareValues, failCountsRev).zipped.toList
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new StringValuesRDDMetricCalculator(t._2, true))((m, v) => m.increment(v)),
        t._3
      ))
      metricResults.foreach(v => v._1.getFailCounter shouldEqual v._2)
    }
    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](
        new StringValuesRDDMetricCalculator(compareValues.head, false)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.StringValues.entryName)._1 shouldEqual 0
    }

    "be serializable for buffer checkpointing" in {
      testSingleColSeq.zip(compareValues).map(v => v._1.foldLeft[RDDMetricCalculator](
        new StringValuesRDDMetricCalculator(v._2, false)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }
}
