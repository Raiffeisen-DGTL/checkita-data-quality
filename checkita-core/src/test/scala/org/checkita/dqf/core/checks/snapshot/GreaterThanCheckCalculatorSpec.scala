package org.checkita.dqf.core.checks.snapshot

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.checks.CommonChecksVals._

class GreaterThanCheckCalculatorSpec extends AnyWordSpec with Matchers {

  "GreaterThanCheckCalculator" must {
    // topN metric can only be checked via specific trend check since they produce multiple results.
    "return correct result for metrics results except TopN metric results" in {
      // all combinations: (baseMetric, compareMetric, compareThreshold, status)
      val allCombinations: Seq[(String, Option[String], Option[Double], CalculatorStatus)] = Seq(
        ("metric1", None, Some(313), CalculatorStatus.Success),
        ("metric1", None, Some(314), CalculatorStatus.Failure),
        ("metric1", None, Some(315), CalculatorStatus.Failure),
        ("metric1", Some("metric6"), None, CalculatorStatus.Failure),
        ("metric1", Some("metric3"), None, CalculatorStatus.Success),
        ("metric1", Some("metric6"), Some(314), CalculatorStatus.Error),
        ("metric1", Some("metric5"), None, CalculatorStatus.Error),
        ("metric2", None, Some(0.92), CalculatorStatus.Success),
        ("metric2", None, Some(0.921), CalculatorStatus.Failure),
        ("metric2", None, Some(0.9211), CalculatorStatus.Failure),
        ("metric4", Some("metric6"), None, CalculatorStatus.Success),
        ("metric2", Some("metric6"), Some(314), CalculatorStatus.Error),
        ("metric2", Some("metric5"), None, CalculatorStatus.Error),
        ("metric5", None, Some(314), CalculatorStatus.Error),
      )

      allCombinations.foreach(t =>
        GreaterThanCheckCalculator("greater_than_check", t._1, t._2, t._3, isCritical = false)
          .run(metricResults).status shouldEqual t._4
      )
    }
  }
}
