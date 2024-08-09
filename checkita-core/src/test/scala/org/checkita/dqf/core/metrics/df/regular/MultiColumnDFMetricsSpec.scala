package org.checkita.dqf.core.metrics.df.regular

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common.{sc, spark}
import org.checkita.dqf.core.metrics.df.DFMetricCalculator
import org.checkita.dqf.core.metrics.df.regular.MultiColumnDFMetrics._

class MultiColumnDFMetricsSpec extends AnyWordSpec with Matchers with DFMetricsTestUtils {

  private val doubleCols = Seq("c1", "c2")

  private val testValues = Seq(
    Seq(
      Seq("Gpi2C7", "xTOn6x"), Seq("xTOn6x", "3xGSz0"), Seq("Gpi2C7", "Gpi2C7"),
      Seq("Gpi2C7", "xTOn6x"), Seq("3xGSz0", "xTOn6x"), Seq("M66yO0", "M66yO0")
    ),
    Seq(
      Seq(5.94, 1.72), Seq(1.72, 5.87), Seq(5.94, 5.94),
      Seq(5.94, 1.72), Seq(5.87, 1.72), Seq(8.26, 8.26)
    ),
    Seq(
      Seq("2.54", "7.71"), Seq("7.71", "2.16"), Seq("2.54", "2.54"),
      Seq("2.54", "7.71"), Seq("2.16", "7.71"), Seq("6.85", "6.85")
    ),
    Seq(
      Seq("4", "3.14"), Seq("foo", "3.0"), Seq("-25.321", "-25.321"),
      Seq("[12, 35]", "true"), Seq("3", "3"), Seq("bar", "3123dasd")
    )
  )

  private val failTestValues: Seq[Seq[Any]] = Seq(
    Seq("foo", "bar", "buz"),
    Seq(
      Seq("one", "two", "three"),
      Seq("four", "five", "six"),
      Seq("seven", "eight", "nine")
    )
  )

  private val dateValues = Seq(
    Seq(Seq("2022-01-01", "2022-01-01"), Seq("1999-12-31", "2000-01-01"), Seq("2005-03-03", "2005-03-01"), Seq("2010-10-21", "2010-10-18")),
    Seq(Seq("2022-01-01", "2022-01-01"), Seq("foo", "bar"), Seq("123", "123"), Seq("2022-01-01 12:31:48", "2022-01-01 07:12:34"))
  )

  private val testSchemas = Seq(StringType, DoubleType, StringType, StringType).map(dt => StructType(Seq(
    StructField("c1", dt, nullable = true),
    StructField("c2", dt, nullable = true)
  )))

  private val failSchemas = Seq(
    StructType(Seq(StructField("c1", StringType, nullable = true))),
    StructType(Seq(
      StructField("c1", StringType, nullable = true),
      StructField("c2", StringType, nullable = true),
      StructField("c1", StringType, nullable = true)
    ))
  )

  private val dateSchemas = Seq(StringType, StringType).map(dt => StructType(Seq(
    StructField("c1", dt, nullable = true),
    StructField("c2", dt, nullable = true)
  )))

  private val testDFs: Seq[DataFrame] = getTestDataFrames(testValues, testSchemas)
  private val failDFs: Seq[DataFrame] = getTestDataFrames(failTestValues, failSchemas)
  private val dateDFs: Seq[DataFrame] = getTestDataFrames(dateValues, dateSchemas)
  private val emptyDF = spark.createDataFrame(sc.emptyRDD[Row], testSchemas.head)

//  Seq(
//    testDFs,
//    failDFs
//  ).foreach { dfSeq => dfSeq.foreach { df =>
//    df.printSchema()
//    df.show(truncate = false)
//  }}

  "CovarianceDFMetricCalculator" must {
    val mId = "covariance"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)
    val results = Seq(Double.NaN, 0.4258749999999993, -2.4728499999999998, 181.8582246666667)
    val failCounts = Seq(6, 0, 0, 3)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator =
      (mId, cols, _) => CovarianceDFMetricCalculator(mId, cols)

    "return correct metric value and fail counts for double-column sequence" in {
      testMetric(testDFs, mId, doubleCols, results, failCounts, params, getCalc)
    }
    "throw an assertion error for single-column and multi-column (more than two columns) sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.head), mId, singleCols, Seq(0.0), Seq(0), params, getCalc
      )
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.tail.head), mId, multiCols, Seq(0.0), Seq(0), params, getCalc
      )
    }
    "return NaN value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, doubleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "CovarianceBesselDFMetricCalculator" must {
    val mId = "covarianceBessel"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)
    val results = Seq(Double.NaN, 0.5110499999999991, -2.9674199999999997, 272.78733700000004)
    val failCounts = Seq(6, 0, 0, 3)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator =
      (mId, cols, _) => CovarianceBesselDFMetricCalculator(mId, cols)

    "return correct metric value and fail counts for double-column sequence" in {
      testMetric(testDFs, mId, doubleCols, results, failCounts, params, getCalc)
    }
    "throw an assertion error for single-column and multi-column (more than two columns) sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.head), mId, singleCols, Seq(0.0), Seq(0), params, getCalc
      )
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.tail.head), mId, multiCols, Seq(0.0), Seq(0), params, getCalc
      )
    }
    "return NaN value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, doubleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "CoMomentDFMetricCalculator" must {
    val mId = "coMoment"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)
    val results = Seq(Double.NaN, 2.5552499999999956, -14.8371, 545.5746740000001)
    val failCounts = Seq(6, 0, 0, 3)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator =
      (mId, cols, _) => CoMomentDFMetricCalculator(mId, cols)

    "return correct metric value and fail counts for double-column sequence" in {
      testMetric(testDFs, mId, doubleCols, results, failCounts, params, getCalc)
    }
    "throw an assertion error for single-column and multi-column (more than two columns) sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.head), mId, singleCols, Seq(0.0), Seq(0), params, getCalc
      )
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.tail.head), mId, multiCols, Seq(0.0), Seq(0), params, getCalc
      )
    }
    "return NaN value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, doubleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "ColumnEqDFMetricCalculator" must {
    val mId = "columnEq"
    val toParams: Boolean => Seq[Map[String, Any]] =
      reversed => Seq.fill(4)(Map("reversed" -> reversed))

    val directParams: Seq[Map[String, Any]] = toParams(false)
    val reversedParams: Seq[Map[String, Any]] = toParams(true)
    val results = Seq.fill(4)(2).map(_.toDouble)
    val failCounts = Seq.fill(4)(4)
    val failCountsRev = Seq.fill(4)(2)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      ColumnEqDFMetricCalculator(mId, cols, reversed)
    }

    "return correct metric value and fail counts for double-column sequence" in {
      testMetric(testDFs, mId, doubleCols, results, failCounts, directParams, getCalc)
      testMetric(testDFs, mId, doubleCols, results, failCountsRev, reversedParams, getCalc)
    }
    "throw an assertion error for single-column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.head), mId, singleCols, Seq(0.0), Seq(0), Seq(directParams.head), getCalc
      )
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, doubleCols, Seq(0.0), Seq(0), Seq(directParams.head), getCalc)
    }
  }

  "DayDistanceDFMetricCalculator" must {
    val mId = "columnEq"
    val toParams: Boolean => Seq[Map[String, Any]] = reversed => Seq.fill(2)(Map(
      "threshold" -> 3,
      "dateFormat" -> "yyyy-MM-dd",
      "reversed" -> reversed
    ))
    val directParams: Seq[Map[String, Any]] = toParams(false)
    val reversedParams: Seq[Map[String, Any]] = toParams(true)
    val results = Seq(3, 1).map(_.toDouble)
    val failCounts = Seq(1, 3)
    val failCountsRev = Seq(3, 1)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val threshold = params.getOrElse("threshold", false).asInstanceOf[Int]
      val dateFormat = params.getOrElse("dateFormat", false).asInstanceOf[String]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      DayDistanceDFMetricCalculator(mId, cols, dateFormat, threshold, reversed)
    }

    "return correct metric value and fail counts for double-column sequence" in {
      testMetric(dateDFs, mId, doubleCols, results, failCounts, directParams, getCalc)
      testMetric(dateDFs, mId, doubleCols, results, failCountsRev, reversedParams, getCalc)
    }
    "throw an assertion error for single-column and multi-column (more than two columns) sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.head), mId, singleCols, Seq(0.0), Seq(0), Seq(directParams.head), getCalc
      )
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.tail.head), mId, multiCols, Seq(0.0), Seq(0), Seq(directParams.head), getCalc
      )
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, doubleCols, Seq(0.0), Seq(0), Seq(directParams.head), getCalc)
    }
  }

  "LevenshteinDistanceDFMetricCalculator" must {
    val mId = "columnEq"
    val toParams: Boolean => Seq[Map[String, Any]] = reversed => Seq(
      (3.0, false), (0.501, true), (0.751, true), (2.0, false)
    ).map{
      case (threshold, normalized) => Map(
        "threshold" -> threshold,
        "normalized" -> normalized,
        "reversed" -> reversed
      )
    }
    val directParams: Seq[Map[String, Any]] = toParams(false)
    val reversedParams: Seq[Map[String, Any]] = toParams(true)
    val results = Seq(2, 2, 6, 2).map(_.toDouble)
    val failCounts = Seq(4, 4, 0, 4)
    val failCountsRev = Seq(2, 2, 6, 2)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val threshold = params.getOrElse("threshold", false).asInstanceOf[Double]
      val normalized = params.getOrElse("normalized", false).asInstanceOf[Boolean]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      LevenshteinDistanceDFMetricCalculator(mId, cols, threshold, normalized, reversed)
    }

    "return correct metric value and fail counts for double-column sequence" in {
      testMetric(testDFs, mId, doubleCols, results, failCounts, directParams, getCalc)
      testMetric(testDFs, mId, doubleCols, results, failCountsRev, reversedParams, getCalc)
    }
    "throw an assertion error for single-column and multi-column (more than two columns) sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.head), mId, singleCols, Seq(0.0), Seq(0), Seq(directParams.head), getCalc
      )
      an [AssertionError] should be thrownBy  testMetric(
        Seq(failDFs.tail.head), mId, multiCols, Seq(0.0), Seq(0), Seq(directParams.head), getCalc
      )
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, doubleCols, Seq(0.0), Seq(0), Seq(directParams.head), getCalc)
    }
  }

}
