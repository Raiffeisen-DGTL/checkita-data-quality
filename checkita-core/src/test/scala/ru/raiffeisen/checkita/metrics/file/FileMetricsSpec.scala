package ru.raiffeisen.checkita.metrics.file

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.metrics.MetricCalculator
import ru.raiffeisen.checkita.metrics.file.FileMetrics.RowCountMetricCalculator

import scala.util.Random

class FileMetricsSpec extends AnyWordSpec with Matchers {
  val rand: Random.type = Random
  val testValues = Seq(
    Seq.fill(42)(Seq.fill(1)(rand.nextInt(100))),
    Seq.fill(42)(Seq.fill(4)(rand.alphanumeric.take(5).mkString)),
    Seq.fill(42)(Seq.fill(42)(rand.nextDouble))
  )
  "RowCountMetricCalculator" must {
    "return correct metric value for input sequence" in {
      testValues.foreach(s => s.foldLeft[MetricCalculator](RowCountMetricCalculator(0))(
        (m, v) => m.increment(v)).result()("ROW_COUNT")._1 shouldEqual 42
      )
    }
  }
}
