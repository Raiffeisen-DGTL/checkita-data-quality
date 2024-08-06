package org.checkita.dqf.core.checks

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.checkita.dqf.Common.spark
import org.checkita.dqf.core.Results.{MetricCalculatorResult, ResultType}
import org.checkita.dqf.core.Source

object CommonChecksVals {
  
  private val flatSchema = StructType(Seq(
    StructField("id", StringType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("someValue1", StringType, nullable = true),
    StructField("otherValue2", StringType, nullable = true),
    StructField("dateTime", StringType, nullable = true)
  ))

  private val nestedSchema = StructType(Seq(
    StructField("id", StringType, nullable = true),
    StructField("data", StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("dateTime", StringType, nullable = true),
      StructField("values", StructType(Seq(
        StructField("someValue1", StringType, nullable = true),
        StructField("otherValue2", StringType, nullable = true)
      )))
    )))
  ))

  val flatSrc: Source = Source("flat", spark.createDataFrame(spark.sparkContext.emptyRDD[Row], flatSchema))
  val nestedSrc: Source = Source("nested", spark.createDataFrame(spark.sparkContext.emptyRDD[Row], nestedSchema))
  
  val metricResults = Map(
    "metric1" -> Seq(MetricCalculatorResult(
      "metric1", "ROW_COUNT", 314, None, Seq("source1"), Seq.empty, Seq.empty, None, ResultType.RegularMetric
    )),
    "metric2" -> Seq(MetricCalculatorResult(
      "metric2", "COMPLETENESS", 0.921, None, Seq("source1"), Seq.empty, Seq("col1"), None, ResultType.RegularMetric
    )),
    "metric3" -> Seq(MetricCalculatorResult(
      "metric3", "REGEX_MATCH", 256, None, Seq("source1"), Seq.empty, Seq("col2"), None, ResultType.RegularMetric
    )),
    "metric4" -> Seq(MetricCalculatorResult(
      "metric4", "SUM_NUMBER", 321.456, None, Seq("source1"), Seq.empty, Seq("col3"), None, ResultType.RegularMetric
    )),
    "metric5" -> Seq(
      MetricCalculatorResult(
        "metric5", "TOP_N_1", 123, Some("VALUE1"), Seq("source1"), Seq.empty, Seq("col4"), None, ResultType.RegularMetric
      ),
      MetricCalculatorResult(
        "metric5", "TOP_N_2", 77, Some("VALUE2"), Seq("source1"), Seq.empty, Seq("col4"), None, ResultType.RegularMetric
      ),
      MetricCalculatorResult(
        "metric5", "TOP_N_3", 25, Some("VALUE3"), Seq("source1"), Seq.empty, Seq("col4"), None, ResultType.RegularMetric
      ),
    ),
    "metric6" -> Seq(MetricCalculatorResult(
      "metric6", "DISTINCT_VALUES", 314, None, Seq("source1"), Seq.empty, Seq("col2"), None, ResultType.RegularMetric
    )),
    "99th_quantile" -> Seq(MetricCalculatorResult(
      "99th_quantile", "GET_QUANTILE", 4.75, None, Seq("usgs"), Seq.empty, Seq("mag"), None, ResultType.RegularMetric
    )),
    "avg_mag_error" -> Seq(MetricCalculatorResult(
      "avg_mag_error", "AVG_NUMBER", 0.225, None, Seq("usgs"), Seq.empty, Seq("magerror"), None, ResultType.RegularMetric
    )),
    "mag_med_to_avg_dvg" -> Seq(MetricCalculatorResult(
      "mag_med_to_avg_dvg", "COMPOSED", 0.835, None, Seq("usgs"), Seq.empty, Seq.empty, None, ResultType.ComposedMetric
    )),
  )
}
