package org.checkita.dqf.core.checks.trend

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.config.Enums.TrendCheckRule
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.checks.CommonChecksVals._

class AverageBoundFullCheckCalculatorSpec extends AnyWordSpec with Matchers {

  "AverageBoundFullCheckCalculator" must {

    "return correct result for metrics results except TopN metric results" in {
      // all combinations: (baseMetric, compareThreshold, rule, windowSize, windowOffset)
      val allCombinations: Seq[(String, Double, TrendCheckRule, String, Option[String], CalculatorStatus)] = Seq(
        ("99th_quantile", 0.016, TrendCheckRule.Record, "3", None, CalculatorStatus.Success),
        ("99th_quantile", 0.015, TrendCheckRule.Record, "3", None, CalculatorStatus.Failure),
        ("99th_quantile", 0.059, TrendCheckRule.Record, "5", None, CalculatorStatus.Success),
        ("99th_quantile", 0.058, TrendCheckRule.Record, "5", None, CalculatorStatus.Failure),
        ("99th_quantile", 0.054, TrendCheckRule.Record, "7", None, CalculatorStatus.Success),
        ("99th_quantile", 0.053, TrendCheckRule.Record, "7", None, CalculatorStatus.Failure),

        ("99th_quantile", 0.016, TrendCheckRule.Datetime, "3d", None, CalculatorStatus.Success),
        ("99th_quantile", 0.015, TrendCheckRule.Datetime, "3d", None, CalculatorStatus.Failure),
        ("99th_quantile", 0.059, TrendCheckRule.Datetime, "5d", None, CalculatorStatus.Success),
        ("99th_quantile", 0.058, TrendCheckRule.Datetime, "5d", None, CalculatorStatus.Failure),
        ("99th_quantile", 0.054, TrendCheckRule.Datetime, "7d", None, CalculatorStatus.Success),
        ("99th_quantile", 0.053, TrendCheckRule.Datetime, "7d", None, CalculatorStatus.Failure),

        ("99th_quantile", 0.090, TrendCheckRule.Record, "3", Some("3"), CalculatorStatus.Success),
        ("99th_quantile", 0.089, TrendCheckRule.Record, "3", Some("3"), CalculatorStatus.Failure),
        ("99th_quantile", 0.077, TrendCheckRule.Record, "5", Some("3"), CalculatorStatus.Success),
        ("99th_quantile", 0.076, TrendCheckRule.Record, "5", Some("3"), CalculatorStatus.Failure),
        ("99th_quantile", 0.069, TrendCheckRule.Record, "7", Some("3"), CalculatorStatus.Success),
        ("99th_quantile", 0.068, TrendCheckRule.Record, "7", Some("3"), CalculatorStatus.Failure),

        ("99th_quantile", 0.090, TrendCheckRule.Datetime, "3d", Some("3d"), CalculatorStatus.Success),
        ("99th_quantile", 0.089, TrendCheckRule.Datetime, "3d", Some("3d"), CalculatorStatus.Failure),
        ("99th_quantile", 0.077, TrendCheckRule.Datetime, "5d", Some("3d"), CalculatorStatus.Success),
        ("99th_quantile", 0.076, TrendCheckRule.Datetime, "5d", Some("3d"), CalculatorStatus.Failure),
        ("99th_quantile", 0.069, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Success),
        ("99th_quantile", 0.068, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Failure),

        ("99th_quantile", 0.068, TrendCheckRule.Record, "7d", None, CalculatorStatus.Error),
        ("99th_quantile", 0.068, TrendCheckRule.Record, "7", Some("3d"), CalculatorStatus.Error),
        ("99th_quantile", 0.068, TrendCheckRule.Datetime, "7", None, CalculatorStatus.Error),
        ("99th_quantile", 0.068, TrendCheckRule.Datetime, "7d", Some("3"), CalculatorStatus.Error),
        ("metric1", 0.068, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Error), // not found in db
        ("metric7", 0.068, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Error), // not found in results
        ("metric5", 0.068, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Error) // topN metric
      )

      allCombinations.foreach(t =>
        AverageBoundFullCheckCalculator("avg_bound_full_check", t._1, t._2, t._3, t._4, t._5)
          .run(metricResults).status shouldEqual t._6
      )
      // error if storage menger is not provided:
      AverageBoundFullCheckCalculator(
        "avg_bound_full_check", "99th_quantile", 0.068, TrendCheckRule.Datetime, "7d", Some("3d")
      ).run(metricResults)(jobId, None, settings, spark, fs).status shouldEqual CalculatorStatus.Error
    }
  }
}
