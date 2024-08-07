package org.checkita.dqf.core.metrics.rdd.regular

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common.checkSerDe
import org.checkita.dqf.core.metrics.MetricName
import org.checkita.dqf.core.metrics.rdd.RDDMetricCalculator
import org.checkita.dqf.core.metrics.rdd.regular.FileRDDMetrics.RowCountRDDMetricCalculator
import org.checkita.dqf.core.serialization.Implicits._

import scala.util.Random

class FileRDDMetricsSpec extends AnyWordSpec with Matchers {
  val rand: Random.type = Random
  val testValues: Seq[Seq[Seq[Any]]] = Seq(
    Seq.fill(42)(Seq.fill(1)(rand.nextInt(100))),
    Seq.fill(42)(Seq.fill(4)(rand.alphanumeric.take(5).mkString)),
    Seq.fill(42)(Seq.fill(42)(rand.nextDouble()))
  )
  "RowCountRDDMetricCalculator" must {
    "return correct metric value for input sequence" in {
      testValues.foreach(s => s.foldLeft[RDDMetricCalculator](new RowCountRDDMetricCalculator())(
        (m, v) => m.increment(v)).result()(MetricName.RowCount.entryName)._1 shouldEqual 42
      )
    }

    "be serializable for buffer checkpointing" in {
      testValues.map(t => t.foldLeft[RDDMetricCalculator](
        new RowCountRDDMetricCalculator()
      )(
        (m, v) => m.increment(Seq(v))
      )).foreach(c => checkSerDe[RDDMetricCalculator](c))
    }
  }
}