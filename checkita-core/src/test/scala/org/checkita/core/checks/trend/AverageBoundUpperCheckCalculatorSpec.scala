package org.checkita.core.checks.trend

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.Common._
import org.checkita.config.Enums.TrendCheckRule
import org.checkita.core.CalculatorStatus
import org.checkita.core.checks.CommonChecksVals._

class AverageBoundUpperCheckCalculatorSpec extends AnyWordSpec with Matchers {

  "AverageBoundUpperCheckCalculator" must {

    "return correct result for metrics results except TopN metric results" in {
      // all combinations: (baseMetric, compareThreshold, rule, windowSize, windowOffset)
      val allCombinations: Seq[(String, Double, TrendCheckRule, String, Option[String], CalculatorStatus)] = Seq(
        ("avg_mag_error", 0.0055, TrendCheckRule.Record, "3", None, CalculatorStatus.Success),
        ("avg_mag_error", 0.0054, TrendCheckRule.Record, "3", None, CalculatorStatus.Failure),
        ("avg_mag_error", 0.025, TrendCheckRule.Record, "5", None, CalculatorStatus.Success),
        ("avg_mag_error", 0.024, TrendCheckRule.Record, "5", None, CalculatorStatus.Failure),
        ("avg_mag_error", 0.047, TrendCheckRule.Record, "7", None, CalculatorStatus.Success),
        ("avg_mag_error", 0.046, TrendCheckRule.Record, "7", None, CalculatorStatus.Failure),

        ("avg_mag_error", 0.0055, TrendCheckRule.Datetime, "3d", None, CalculatorStatus.Success),
        ("avg_mag_error", 0.0054, TrendCheckRule.Datetime, "3d", None, CalculatorStatus.Failure),
        ("avg_mag_error", 0.025, TrendCheckRule.Datetime, "5d", None, CalculatorStatus.Success),
        ("avg_mag_error", 0.024, TrendCheckRule.Datetime, "5d", None, CalculatorStatus.Failure),
        ("avg_mag_error", 0.047, TrendCheckRule.Datetime, "7d", None, CalculatorStatus.Success),
        ("avg_mag_error", 0.046, TrendCheckRule.Datetime, "7d", None, CalculatorStatus.Failure),

        ("avg_mag_error", 0.100, TrendCheckRule.Record, "3", Some("3"), CalculatorStatus.Success),
        ("avg_mag_error", 0.099, TrendCheckRule.Record, "3", Some("3"), CalculatorStatus.Failure),
        ("avg_mag_error", 0.092, TrendCheckRule.Record, "5", Some("3"), CalculatorStatus.Success),
        ("avg_mag_error", 0.091, TrendCheckRule.Record, "5", Some("3"), CalculatorStatus.Failure),
        ("avg_mag_error", 0.054, TrendCheckRule.Record, "7", Some("3"), CalculatorStatus.Success),
        ("avg_mag_error", 0.053, TrendCheckRule.Record, "7", Some("3"), CalculatorStatus.Failure),

        ("avg_mag_error", 0.100, TrendCheckRule.Datetime, "3d", Some("3d"), CalculatorStatus.Success),
        ("avg_mag_error", 0.099, TrendCheckRule.Datetime, "3d", Some("3d"), CalculatorStatus.Failure),
        ("avg_mag_error", 0.092, TrendCheckRule.Datetime, "5d", Some("3d"), CalculatorStatus.Success),
        ("avg_mag_error", 0.091, TrendCheckRule.Datetime, "5d", Some("3d"), CalculatorStatus.Failure),
        ("avg_mag_error", 0.054, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Success),
        ("avg_mag_error", 0.053, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Failure),

        ("avg_mag_error", 0.068, TrendCheckRule.Record, "7d", None, CalculatorStatus.Error),
        ("avg_mag_error", 0.068, TrendCheckRule.Record, "7", Some("3d"), CalculatorStatus.Error),
        ("avg_mag_error", 0.068, TrendCheckRule.Datetime, "7", None, CalculatorStatus.Error),
        ("avg_mag_error", 0.068, TrendCheckRule.Datetime, "7d", Some("3"), CalculatorStatus.Error),
        ("metric1", 0.068, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Error), // not found in db
        ("metric7", 0.068, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Error), // not found in results
        ("metric5", 0.068, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Error) // topN metric
      )

      allCombinations.foreach(t =>
        AverageBoundUpperCheckCalculator("avg_bound_upper_check", t._1, t._2, t._3, t._4, t._5)
          .run(metricResults).status shouldEqual t._6
      )
      // error if storage menger is not provided:
      AverageBoundUpperCheckCalculator(
        "avg_bound_full_check", "99th_quantile", 0.068, TrendCheckRule.Datetime, "7d", Some("3d")
      ).run(metricResults)(jobId, None, settings, spark, fs).status shouldEqual CalculatorStatus.Error
    }
  }
}