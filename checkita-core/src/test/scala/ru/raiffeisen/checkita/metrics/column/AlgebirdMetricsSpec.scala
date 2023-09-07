package ru.raiffeisen.checkita.metrics.column

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.metrics.MetricCalculator
import ru.raiffeisen.checkita.metrics.MetricProcessor.ParamMap
import ru.raiffeisen.checkita.metrics.column.AlgebirdMetrics._
import ru.raiffeisen.checkita.utils.getParametrizedMetricTail

import scala.util.Random

class AlgebirdMetricsSpec extends AnyWordSpec with Matchers {
  private val testValues = Seq(
    Seq("Gpi2C7", "Gpi2C7", "xTOn6x", "3xGSz0", "Gpi2C7", "Gpi2C7", "Gpi2C7", "xTOn6x", "3xGSz0", "xTOn6x", "M66yO0", "M66yO0"),
    Seq(5.94, 1.72, 5.94, 5.87, 5.94, 5.94, 5.94, 1.72, 5.87, 1.72, 8.26, 8.26),
    Seq("2.54", "7.71", "2.54", "2.16", "2.54", "2.54", "2.54", "7.71", "2.16", "7.71", "6.85", "6.85"),
    Seq('4', 3.14, "foo", 3.0, -25.321, "-25.321", "[12, 35]", true, "-25.321", 4, "bar", "3123dasd")
  )

  "HyperLogLogMetricCalculator" must {
    val params: ParamMap = Map.empty

    "return correct metric for single-column sequence" in {
      val results = Seq(4, 4, 4, 9)
      val values = testValues zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new HyperLogLogMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result(),
        t._2
      ))
      metricResults.foreach { t =>
        t._1("APPROXIMATE_DISTINCT_VALUES" + getParametrizedMetricTail(params))._1 shouldEqual t._2
      }
    }

    "return zero values when applied to empty sequence" in {
      val values = Seq.empty
      values.foldLeft[MetricCalculator](new HyperLogLogMetricCalculator(params))((m, v) => m.increment(Seq(v)))
        .result()("APPROXIMATE_DISTINCT_VALUES" + getParametrizedMetricTail(params))._1 shouldEqual 0
    }

    "throw assertion error when applied to multi-column sequence" in {
      val values = Seq(
        Seq(Seq("foo", "bar"), Seq("bar", "baz")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s =>
        an [AssertionError] should be thrownBy s.foldLeft[MetricCalculator](
          new HyperLogLogMetricCalculator(params))((m, v) => m.increment(v))
      }
    }
  }

  "TopKMetricCalculator" must {
    val params: ParamMap = Map("targetNumber" -> 2)

    "return correct metric for single-column sequence" in {
      val results = Seq(
        Seq((0.4166666666666667, "Gpi2C7"), (0.25, "xTOn6x")),
        Seq((0.4166666666666667, "5.94"), (0.25, "1.72")),
        Seq((0.4166666666666667, "2.54"), (0.25, "7.71")),
        Seq((0.25, "-25.321"), (0.16666666666666666, "4"))
      )
      val values = testValues zip results
      val metricResults = values.map(t => (
        t._1.foldLeft[MetricCalculator](new TopKMetricCalculator(params))(
          (m, v) => m.increment(Seq(v))).result(),
        t._2
      ))
      metricResults.foreach{ t =>
        t._2.zipWithIndex.foreach { x =>
          t._1("TOP_N_" + (x._2 + 1) + getParametrizedMetricTail(params))._1 shouldEqual x._1._1
          t._1("TOP_N_" + (x._2 + 1) + getParametrizedMetricTail(params))._2.get shouldEqual x._1._2
        }
      }
    }

    "return NaN for frequency and empty string as top value when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new TopKMetricCalculator(params))((m, v) => m.increment(Seq(v)))
        .result()
      metricResult("TOP_N_" + 1 + getParametrizedMetricTail(params))._1.isNaN shouldEqual true
      metricResult("TOP_N_" + 1 + getParametrizedMetricTail(params))._2.get shouldEqual ""
    }

    "throw assertion error when applied to multi-column sequence" in {
      val values = Seq(
        Seq(Seq("foo", "bar"), Seq("bar", "baz")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s =>
        an [AssertionError] should be thrownBy s.foldLeft[MetricCalculator](
          new TopKMetricCalculator(params))((m, v) => m.increment(v))
      }
    }
  }

  "HLLSequenceCompletenessMetricCalculator" must {
    val paramList: Seq[ParamMap] = Seq(
      Map.empty,
      Map("increment" -> 4),
      Map("accuracyError" -> 0.001d),
      Map("accuracyError" -> 0.001d, "increment" -> 4)
    )
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
          t._1.foldLeft[MetricCalculator](new HLLSequenceCompletenessMetricCalculator(t._2))(
            (m, v) => m.increment(Seq(v))).result()("APPROXIMATE_SEQUENCE_COMPLETENESS" + getParametrizedMetricTail(t._2))._1,
          t._3
        ))
        metricResults.foreach(v => v._1 shouldEqual v._2)
      }
    }

    "return zero when applied to empty sequence" in {
      val values = Seq.empty
      val metricResult = values.foldLeft[MetricCalculator](new HLLSequenceCompletenessMetricCalculator(paramList.head))(
        (m, v) => m.increment(v)
      )
      metricResult.result()("APPROXIMATE_SEQUENCE_COMPLETENESS" + getParametrizedMetricTail(paramList.head))._1 shouldEqual 0.0
    }

    "throw assertion error when applied to multi-column sequence" in {
      val values = Seq(
        Seq(Seq("foo", "bar"), Seq("bar", "baz")),
        Seq(Seq("foo", "bar", "baz"), Seq("qux", "lux", "fux"))
      )
      values.foreach { s =>
        an [AssertionError] should be thrownBy s.foldLeft[MetricCalculator](
          new HLLSequenceCompletenessMetricCalculator(paramList.head))((m, v) => m.increment(v))
      }
    }
  }
}
