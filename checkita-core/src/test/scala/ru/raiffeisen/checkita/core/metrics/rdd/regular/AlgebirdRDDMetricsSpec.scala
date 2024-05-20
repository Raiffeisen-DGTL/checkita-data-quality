package ru.raiffeisen.checkita.core.metrics.rdd.regular

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common.checkSerDe
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.metrics.MetricName
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricCalculator
import ru.raiffeisen.checkita.core.metrics.rdd.regular.AlgebirdRDDMetrics._
import ru.raiffeisen.checkita.core.metrics.serialization.Implicits._

import scala.util.Random

class AlgebirdRDDMetricsSpec extends AnyWordSpec with Matchers {
  private val testValues = Seq(
    Seq("Gpi2C7", "Gpi2C7", "xTOn6x", "3xGSz0", "Gpi2C7", "Gpi2C7", "Gpi2C7", "xTOn6x", "3xGSz0", "xTOn6x", "M66yO0", "M66yO0"),
    Seq(5.94, 1.72, 5.94, 5.87, 5.94, 5.94, 5.94, 1.72, 5.87, 1.72, 8.26, 8.26),
    Seq("2.54", "7.71", "2.54", "2.16", "2.54", "2.54", "2.54", "7.71", "2.16", "7.71", "6.85", "6.85"),
    Seq('4', 3.14, "foo", 3.0, -25.321, "-25.321", "[12, 35]", true, "-25.321", 4, "bar", "3123dasd")
  )

  "HyperLogLogMetricCalculator" must {
    val accuracy = 0.01
    
    "return correct metric for single-column sequence" in {
      val results = Seq(4, 4, 4, 9)
      val values = testValues zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new HyperLogLogRDDMetricCalculator(accuracy))(
          (m, v) => m.increment(Seq(v))).result(),
        t._2
      ))
      metricResults.foreach { t =>
        t._1(MetricName.ApproximateDistinctValues.entryName)._1 shouldEqual t._2
      }
    }

    "return zero values when applied to empty sequence" in {
      val values = Seq.empty
      values.foldLeft[RDDMetricCalculator](new HyperLogLogRDDMetricCalculator(accuracy))((m, v) => m.increment(Seq(v)))
        .result()(MetricName.ApproximateDistinctValues.entryName)._1 shouldEqual 0
    }
    
    "return error calculator status for multi column sequence" in {
      val values = Seq(
        Seq(Seq("foo", "bar"), Seq("bar", "baz")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )

      values.foreach { s =>
        s.foldLeft[RDDMetricCalculator](new HyperLogLogRDDMetricCalculator(accuracy)) {
          (m, v) =>
            val mc = m.increment(v)
            mc.getStatus shouldEqual CalculatorStatus.Error
            mc
        }
      }
    }
    
    "be serializable for buffer checkpointing" in {
      testValues.map(v => v.foldLeft[RDDMetricCalculator](new HyperLogLogRDDMetricCalculator(accuracy))(
          (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "TopKMetricCalculator" must {
    val maxCapacity = 100
    val targetNumber = 2

    "return correct metric for single-column sequence" in {
      val results = Seq(
        Seq((0.4166666666666667, "Gpi2C7"), (0.25, "xTOn6x")),
        Seq((0.4166666666666667, "5.94"), (0.25, "1.72")),
        Seq((0.4166666666666667, "2.54"), (0.25, "7.71")),
        Seq((0.25, "-25.321"), (0.16666666666666666, "4"))
      )
      val values = testValues zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[RDDMetricCalculator](new TopKRDDMetricCalculator(maxCapacity, targetNumber))(
          (m, v) => m.increment(Seq(v))).result(),
        t._2
      ))
      metricResults.foreach{ t =>
        t._2.zipWithIndex.foreach { x =>
          t._1(MetricName.TopN.entryName + "_" + (x._2 + 1))._1 shouldEqual x._1._1
          t._1(MetricName.TopN.entryName + "_" + (x._2 + 1))._2.get shouldEqual x._1._2
        }
      }
    }

    "return NaN for frequency and empty string as top value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](
        new TopKRDDMetricCalculator(maxCapacity, targetNumber)
      )((m, v) => m.increment(Seq(v))).result()
      metricResult(MetricName.TopN.entryName + "_" + 1)._1.isNaN shouldEqual true
      metricResult(MetricName.TopN.entryName + "_" + 1)._2.get shouldEqual ""
    }
    
    "return error calculator status for multi column sequence" in {
      val values = Seq(
        Seq(Seq("foo", "bar"), Seq("bar", "baz")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )

      values.foreach { s =>
        s.foldLeft[RDDMetricCalculator](new TopKRDDMetricCalculator(maxCapacity, targetNumber)) {
          (m, v) =>
            val mc = m.increment(v)
            mc.getStatus shouldEqual CalculatorStatus.Error
            mc
        }
      }
    }
    
    "be serializable for buffer checkpointing" in {
      testValues.map(v => v.foldLeft[RDDMetricCalculator](new TopKRDDMetricCalculator(maxCapacity, targetNumber))(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }

  "HLLSequenceCompletenessMetricCalculator" must {
    val paramList: Seq[(Double, Long)] = Seq((0.01, 1), (0.01, 4), (0.001, 1), (0.001, 4))
    val intSeq: Seq[Seq[Int]] = Seq(
      Range.inclusive(1, 10000),
      Range.inclusive(0, 9996, 4),
      Range.inclusive(1, 100000),
      Range.inclusive(0, 99996, 4)
    )

    val rand = Random
    rand.setSeed(42)

    val nullIndices = Seq.fill(100)(rand.nextInt(10000)).toSet
    val emptyIndices = Seq.fill(100)(rand.nextInt(10000)).toSet
    val nullIntSeq = intSeq.map(s => s.zipWithIndex.map {
      case (_, idx) if nullIndices.contains(idx) => null
      case (v, _) => v
    })
    val emptyIntSeq = nullIntSeq.map(s => s.zipWithIndex.map {
      case (_, idx) if emptyIndices.contains(idx) => ""
      case (v, _) => v
    })

    val results = Seq(
      Seq(0.9974, 1.0004, 0.99963, 1.00028),
      Seq(0.9886, 0.9896, 0.99866, 0.99632),
      Seq(0.9788, 0.9804, 0.99773, 0.9924)
    )
    val allSingleColSeq = Seq(intSeq, nullIntSeq, emptyIntSeq)

    "return correct metric value for single column sequence" in {
      (allSingleColSeq, results).zipped.toList.foreach { tt =>
        val metricResults = (tt._1, paramList, tt._2).zipped.toList.map(t => (
          t._1.foldLeft[RDDMetricCalculator](new HLLSequenceCompletenessRDDMetricCalculator(t._2._1, t._2._2))(
            (m, v) => m.increment(Seq(v))).result()(MetricName.ApproximateSequenceCompleteness.entryName)._1,
          t._3
        ))
        metricResults.foreach(v => v._1 shouldEqual v._2)
      }
    }

    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[RDDMetricCalculator](
        new HLLSequenceCompletenessRDDMetricCalculator(paramList.head._1, paramList.head._2)
      )((m, v) => m.increment(v))
      metricResult.result()(MetricName.ApproximateSequenceCompleteness.entryName)._1 shouldEqual 0.0
    }

    "return error calculator status for multi column sequence" in {
      val values = Seq(
        Seq(Seq("foo", "bar"), Seq("bar", "baz")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )

      values.foreach { s =>
        s.foldLeft[RDDMetricCalculator](
          new HLLSequenceCompletenessRDDMetricCalculator(paramList.head._1, paramList.head._2)
        ){
          (m, v) =>
            val mc = m.increment(v)
            mc.getStatus shouldEqual CalculatorStatus.Error
            mc
        }
      }
    }
    "be serializable for buffer checkpointing" in {
      intSeq.zip(paramList).map(t => t._1.foldLeft[RDDMetricCalculator](
        new HLLSequenceCompletenessRDDMetricCalculator(t._2._1, t._2._2)
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }
}
