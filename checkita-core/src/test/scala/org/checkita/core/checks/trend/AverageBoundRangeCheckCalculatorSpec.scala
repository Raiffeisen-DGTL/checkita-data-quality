package org.checkita.core.checks.trend

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.Common._
import org.checkita.config.Enums.TrendCheckRule
import org.checkita.core.CalculatorStatus
import org.checkita.core.checks.CommonChecksVals._

class AverageBoundRangeCheckCalculatorSpec extends AnyWordSpec with Matchers {

  "AverageBoundRangeCheckCalculator" must {

    "return correct result for metrics results except TopN metric results" in {
      // all combinations: (baseMetric, compareThreshold, rule, windowSize, windowOffset)
      val allCombinations: Seq[(String, Double, Double, TrendCheckRule, String, Option[String], CalculatorStatus)] = 
        Seq(
          ("99th_quantile", 0.016, 0.0, TrendCheckRule.Record, "3", None, CalculatorStatus.Success),
          ("99th_quantile", 0.015, 0.0, TrendCheckRule.Record, "3", None, CalculatorStatus.Failure),
          ("99th_quantile", 0.059, 0.0, TrendCheckRule.Record, "5", None, CalculatorStatus.Success),
          ("99th_quantile", 0.058, 0.0, TrendCheckRule.Record, "5", None, CalculatorStatus.Failure),
          ("99th_quantile", 0.054, 0.0, TrendCheckRule.Record, "7", None, CalculatorStatus.Success),
          ("99th_quantile", 0.053, 0.0, TrendCheckRule.Record, "7", None, CalculatorStatus.Failure),
  
          ("99th_quantile", 0.016, 0.0, TrendCheckRule.Datetime, "3d", None, CalculatorStatus.Success),
          ("99th_quantile", 0.015, 0.0, TrendCheckRule.Datetime, "3d", None, CalculatorStatus.Failure),
          ("99th_quantile", 0.059, 0.0, TrendCheckRule.Datetime, "5d", None, CalculatorStatus.Success),
          ("99th_quantile", 0.058, 0.0, TrendCheckRule.Datetime, "5d", None, CalculatorStatus.Failure),
          ("99th_quantile", 0.054, 0.0, TrendCheckRule.Datetime, "7d", None, CalculatorStatus.Success),
          ("99th_quantile", 0.053, 0.0, TrendCheckRule.Datetime, "7d", None, CalculatorStatus.Failure),
  
          ("99th_quantile", 0.090, 0.0, TrendCheckRule.Record, "3", Some("3"), CalculatorStatus.Success),
          ("99th_quantile", 0.089, 0.0, TrendCheckRule.Record, "3", Some("3"), CalculatorStatus.Failure),
          ("99th_quantile", 0.077, 0.0, TrendCheckRule.Record, "5", Some("3"), CalculatorStatus.Success),
          ("99th_quantile", 0.076, 0.0, TrendCheckRule.Record, "5", Some("3"), CalculatorStatus.Failure),
          ("99th_quantile", 0.069, 0.0, TrendCheckRule.Record, "7", Some("3"), CalculatorStatus.Success),
          ("99th_quantile", 0.068, 0.0, TrendCheckRule.Record, "7", Some("3"), CalculatorStatus.Failure),
  
          ("99th_quantile", 0.090, 0.0, TrendCheckRule.Datetime, "3d", Some("3d"), CalculatorStatus.Success),
          ("99th_quantile", 0.089, 0.0, TrendCheckRule.Datetime, "3d", Some("3d"), CalculatorStatus.Failure),
          ("99th_quantile", 0.077, 0.0, TrendCheckRule.Datetime, "5d", Some("3d"), CalculatorStatus.Success),
          ("99th_quantile", 0.076, 0.0, TrendCheckRule.Datetime, "5d", Some("3d"), CalculatorStatus.Failure),
          ("99th_quantile", 0.069, 0.0, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Success),
          ("99th_quantile", 0.068, 0.0, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Failure),

          ("avg_mag_error", 0.0, 0.0055, TrendCheckRule.Record, "3", None, CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.0054, TrendCheckRule.Record, "3", None, CalculatorStatus.Failure),
          ("avg_mag_error", 0.0, 0.025, TrendCheckRule.Record, "5", None, CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.024, TrendCheckRule.Record, "5", None, CalculatorStatus.Failure),
          ("avg_mag_error", 0.0, 0.047, TrendCheckRule.Record, "7", None, CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.046, TrendCheckRule.Record, "7", None, CalculatorStatus.Failure),

          ("avg_mag_error", 0.0, 0.0055, TrendCheckRule.Datetime, "3d", None, CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.0054, TrendCheckRule.Datetime, "3d", None, CalculatorStatus.Failure),
          ("avg_mag_error", 0.0, 0.025, TrendCheckRule.Datetime, "5d", None, CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.024, TrendCheckRule.Datetime, "5d", None, CalculatorStatus.Failure),
          ("avg_mag_error", 0.0, 0.047, TrendCheckRule.Datetime, "7d", None, CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.046, TrendCheckRule.Datetime, "7d", None, CalculatorStatus.Failure),

          ("avg_mag_error", 0.0, 0.100, TrendCheckRule.Record, "3", Some("3"), CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.099, TrendCheckRule.Record, "3", Some("3"), CalculatorStatus.Failure),
          ("avg_mag_error", 0.0, 0.092, TrendCheckRule.Record, "5", Some("3"), CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.091, TrendCheckRule.Record, "5", Some("3"), CalculatorStatus.Failure),
          ("avg_mag_error", 0.0, 0.054, TrendCheckRule.Record, "7", Some("3"), CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.053, TrendCheckRule.Record, "7", Some("3"), CalculatorStatus.Failure),

          ("avg_mag_error", 0.0, 0.100, TrendCheckRule.Datetime, "3d", Some("3d"), CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.099, TrendCheckRule.Datetime, "3d", Some("3d"), CalculatorStatus.Failure),
          ("avg_mag_error", 0.0, 0.092, TrendCheckRule.Datetime, "5d", Some("3d"), CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.091, TrendCheckRule.Datetime, "5d", Some("3d"), CalculatorStatus.Failure),
          ("avg_mag_error", 0.0, 0.054, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Success),
          ("avg_mag_error", 0.0, 0.053, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Failure),
          
          ("99th_quantile", 0.068, 0.0, TrendCheckRule.Record, "7d", None, CalculatorStatus.Error),
          ("99th_quantile", 0.068, 0.0, TrendCheckRule.Record, "7", Some("3d"), CalculatorStatus.Error),
          ("99th_quantile", 0.068, 0.0, TrendCheckRule.Datetime, "7", None, CalculatorStatus.Error),
          ("99th_quantile", 0.068, 0.0, TrendCheckRule.Datetime, "7d", Some("3"), CalculatorStatus.Error),
          ("metric1", 0.068, 0.0, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Error), // not found in db
          ("metric7", 0.068, 0.0, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Error), // not found in results
          ("metric5", 0.068, 0.0, TrendCheckRule.Datetime, "7d", Some("3d"), CalculatorStatus.Error) // topN metric
        )

      allCombinations.foreach(t =>
        AverageBoundRangeCheckCalculator("avg_bound_full_check", t._1, t._2, t._3, t._4, t._5, t._6)
          .run(metricResults).status shouldEqual t._7
      )
      // error if storage menger is not provided:
      AverageBoundRangeCheckCalculator(
        "avg_bound_full_check", "99th_quantile", 0.068, 0, TrendCheckRule.Datetime, "7d", Some("3d")
      ).run(metricResults)(jobId, None, settings, spark, fs).status shouldEqual CalculatorStatus.Error
    }
  }
}