package org.checkita.core.checks.snapshot

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.Common._
import org.checkita.core.CalculatorStatus
import org.checkita.core.checks.CommonChecksVals._

class DifferByLTCheckCalculatorSpec extends AnyWordSpec with Matchers {

  "DifferByLTCheckCalculator" must {
    // topN metric can only be checked via specific trend check since they produce multiple results.
    "return correct result for metrics results except TopN metric results" in {
      // all combinations: (baseMetric, compareMetric, compareThreshold, status)
      val allCombinations: Seq[(String, Option[String], Double, CalculatorStatus)] = Seq(
        ("metric1", None, 314, CalculatorStatus.Error),
        ("metric1", Some("metric6"), 1e-9, CalculatorStatus.Success),
        ("metric1", Some("metric3"), 0.01, CalculatorStatus.Failure),
        ("metric1", Some("metric4"), 0.0232, CalculatorStatus.Success),
        ("metric1", Some("metric4"), 0.0231, CalculatorStatus.Failure),
        ("metric2", Some("metric5"), 314, CalculatorStatus.Error),
        ("metric5", None, 314, CalculatorStatus.Error),
      )

      allCombinations.foreach(t =>
        DifferByLTCheckCalculator("less_than_check", t._1, t._2, t._3)
          .run(metricResults).status shouldEqual t._4
      )
    }
  }
}
