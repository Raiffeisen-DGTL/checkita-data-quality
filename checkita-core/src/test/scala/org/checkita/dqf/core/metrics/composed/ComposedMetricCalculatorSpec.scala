package org.checkita.dqf.core.metrics.composed

import eu.timepit.refined.auto._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.config.RefinedTypes.ID
import org.checkita.dqf.config.jobconf.Metrics.ComposedMetricConfig
import org.checkita.dqf.core.Results.{MetricCalculatorResult, ResultType}
import org.checkita.dqf.core.metrics.MetricName

class ComposedMetricCalculatorSpec extends AnyWordSpec with Matchers {
  val metricResults: Seq[MetricCalculatorResult] = Seq(
    MetricCalculatorResult(
      "hive_table1_nulls",
      MetricName.NullValues.entryName,
      5.0,
      None,
      Seq("hive_table1"),
      Seq.empty,
      Seq("id", "name"),
      None,
      ResultType.RegularMetric
    ),
    MetricCalculatorResult(
      "hive_table1_row_cnt",
      MetricName.RowCount.entryName,
      10.0,
      None,
      Seq("hive_table1"),
      Seq.empty,
      Seq.empty,
      None,
      ResultType.RegularMetric
    )
  )
  
  "ComposedMetricCalculator" must {
    "return correct value in addition and subtraction test with two metrics" in {
      val composedMetric = ComposedMetricConfig(
        ID("test"), None, "{{ hive_table1_nulls }} + {{ hive_table1_row_cnt }} - 3"
        )
      val result = 12.0
      ComposedMetricCalculator(metricResults).run(composedMetric).result shouldEqual result
    }

    "return correct value in multiplication and division test with two metrics" in {
      val composedMetric = ComposedMetricConfig(
        ID("test"), None, "100 * {{ hive_table1_nulls }} / {{ hive_table1_row_cnt }}"
      )
      val result = 50.0
      ComposedMetricCalculator(metricResults).run(composedMetric).result shouldEqual result
    }

    "return correct value in multiplication, division and power test with one metric" in {
      val composedMetric = ComposedMetricConfig(
        ID("test"), None, "5 * {{ hive_table1_row_cnt }} ^ 2 * 2"
      )
      val result = 1000.0
      ComposedMetricCalculator(metricResults).run(composedMetric).result shouldEqual result
    }

    "return correct value in all operations test with two metrics" in {
      val composedMetric = ComposedMetricConfig(
        ID("test"), None,
        "({{ hive_table1_row_cnt }} ^ 2 / 10 + ({{ hive_table1_nulls }} * 2 - 9) ^ 10 + 4) ^ 2 - {{ hive_table1_row_cnt }} / 5 - 23"
      )
      val result = 200.0
      ComposedMetricCalculator(metricResults).run(composedMetric).result shouldEqual result
    }

    "return some errors if wrong metric name was provided" in {
      val composedMetric = ComposedMetricConfig(
        ID("test"), None, "100 * {{ hive_table1_values }} / {{ hive_table1_row_cnt }}"
      )
      ComposedMetricCalculator(metricResults).run(composedMetric).errors.nonEmpty shouldEqual true
    }
  }
}
