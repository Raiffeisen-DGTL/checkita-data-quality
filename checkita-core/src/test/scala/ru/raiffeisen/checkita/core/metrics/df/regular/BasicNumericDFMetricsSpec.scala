package ru.raiffeisen.checkita.core.metrics.df.regular

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.isarnproject.sketches.java.TDigest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common._
import ru.raiffeisen.checkita.core.metrics.df.DFMetricCalculator
import ru.raiffeisen.checkita.core.metrics.df.regular.BasicNumericDFMetrics._
import ru.raiffeisen.checkita.core.metrics.rdd.Casting.tryToDouble


class BasicNumericDFMetricsSpec extends AnyWordSpec with Matchers with DFMetricsTestUtils {

  private val seqTypes: Seq[DataType] = Seq(IntegerType, DoubleType, StringType, StringType)
  private val testSingleColSchemas = getSingleColSchema(seqTypes)
  private val testMultiColSchemas = getMultiColSchema(seqTypes)

  private val testSingleColSeq = Seq(
    Seq(0, 3, 8, 4, 0, 5, 5, 8, 9, 3, 2, 2, 6, 2, 6),
    Seq(7.28, 6.83, 3.0, 2.0, 6.66, 9.03, 3.69, 2.76, 4.64, 7.83, 9.19, 4.0, 7.5, 3.87, 1.0),
    Seq("7.24", "9.74", "8.32", "9.15", "5.0", "8.38", "2.0", "3.42", "3.0", "6.04", "1.0", "8.37", "0.9", "1.0", "6.54"),
    Seq("4", "3.14", "foo", "3.0", "-25.321", "bar", "[12, 35]", "true", "d", "3", "34.12", "2.0", "3123dasd", "42", "4")
  )
  private val testMultiColSeq = testSingleColSeq.map(s => (0 to 4).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_))))

  private val emptyDF = spark.createDataFrame(sc.emptyRDD[Row], testSingleColSchemas.head)
  private val testSingleColDFs: Seq[DataFrame] = getTestDataFrames(testSingleColSeq, testSingleColSchemas)
  private val testMultiColDFs: Seq[DataFrame] = getTestDataFrames(testMultiColSeq, testMultiColSchemas)

//  Seq(
//    testSingleColDFs,
//    testMultiColDFs,
//  ).foreach { dfSeq => dfSeq.foreach { df =>
//    df.printSchema()
//    df.show(truncate = false)
//  }}

  "MedianValueDFMetricCalculator" must {
    val mId = "medianValue"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map("accuracyError" -> 0.005))
    val results = testSingleColSeq.map(
      s => s.flatMap(tryToDouble).foldLeft(TDigest.empty(0.005)){(t, v) => t.update(v); t}
    ).map(t => t.cdfInverse(0.5))
    val failCountsSingleSeq = Seq(0, 0, 0, 6)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val accuracyError = params.getOrElse("accuracyError", 0.005).asInstanceOf[Double]
      MedianValueDFMetricCalculator(mId, cols, accuracyError)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        testMultiColDFs, mId, multiCols, results, failCountsSingleSeq, params, getCalc
      )
    }
    "return NaN value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "FirstQuantileDFMetricCalculator" must {
    val mId = "firstQuantile"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map("accuracyError" -> 0.005))
    val results = testSingleColSeq.map(
      s => s.flatMap(tryToDouble).foldLeft(TDigest.empty(0.005)){(t, v) => t.update(v); t}
    ).map(t => t.cdfInverse(0.25))
    val failCountsSingleSeq = Seq(0, 0, 0, 6)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val accuracyError = params.getOrElse("accuracyError", 0.005).asInstanceOf[Double]
      FirstQuantileDFMetricCalculator(mId, cols, accuracyError)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        testMultiColDFs, mId, multiCols, results, failCountsSingleSeq, params, getCalc
      )
    }
    "return NaN value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "ThirdQuantileDFMetricCalculator" must {
    val mId = "thirdQuantile"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map("accuracyError" -> 0.005))
    val results = testSingleColSeq.map(
      s => s.flatMap(tryToDouble).foldLeft(TDigest.empty(0.005)){(t, v) => t.update(v); t}
    ).map(t => t.cdfInverse(0.75))
    val failCountsSingleSeq = Seq(0, 0, 0, 6)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val accuracyError = params.getOrElse("accuracyError", 0.005).asInstanceOf[Double]
      ThirdQuantileDFMetricCalculator(mId, cols, accuracyError)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        testMultiColDFs, mId, multiCols, results, failCountsSingleSeq, params, getCalc
      )
    }
    "return NaN value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "GetQuantileDFMetricCalculator" must {
    val mId = "getQuantile"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map("accuracyError" -> 0.005, "target" -> 0.1))
    val results = testSingleColSeq.map(
      s => s.flatMap(tryToDouble).foldLeft(TDigest.empty(0.005)){(t, v) => t.update(v); t}
    ).map(t => t.cdfInverse(0.1))
    val failCountsSingleSeq = Seq(0, 0, 0, 6)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val accuracyError = params.getOrElse("accuracyError", 0.005).asInstanceOf[Double]
      val target = params.getOrElse("target", 0.005).asInstanceOf[Double]
      GetQuantileDFMetricCalculator(mId, cols, accuracyError, target)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        testMultiColDFs, mId, multiCols, results, failCountsSingleSeq, params, getCalc
      )
    }
    "return NaN value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "GetPercentileDFMetricCalculator" must {
    val mId = "getPercentile"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map("accuracyError" -> 0.005, "target" -> 0.1))
    val results = testSingleColSeq.map(
      s => s.flatMap(tryToDouble).foldLeft(TDigest.empty(0.005)){(t, v) => t.update(v); t}
    ).map(t => t.cdf(0.1))
    val failCountsSingleSeq = Seq(0, 0, 0, 6)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val accuracyError = params.getOrElse("accuracyError", 0.005).asInstanceOf[Double]
      val target = params.getOrElse("target", 0.005).asInstanceOf[Double]
      GetPercentileDFMetricCalculator(mId, cols, accuracyError, target)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        testMultiColDFs, mId, multiCols, results, failCountsSingleSeq, params, getCalc
      )
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), params, getCalc)
    }
  }

  "MinNumberDFMetricCalculator" must {
    val mId = "minNumber"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)
    val results = Seq(0.0, 1.0, 0.9, -25.321)
    val failCountsSingleSeq = Seq(0, 0, 0, 6)
    val failCountsMultiSeq = Seq(0, 0, 0, 1)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, _) => {
      MinNumberDFMetricCalculator(mId, cols)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, params, getCalc)
    }
    "return max double value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.MaxValue), Seq(0), params, getCalc)
    }
  }

  "MaxNumberDFMetricCalculator" must {
    val mId = "maxNumber"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)
    val results = Seq(9.0, 9.19, 9.74, 42.0)
    val failCountsSingleSeq = Seq(0, 0, 0, 6)
    val failCountsMultiSeq = Seq(0, 0, 0, 1)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, _) => {
      MaxNumberDFMetricCalculator(mId, cols)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, params, getCalc)
    }
    "return min double value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.MinValue), Seq(0), params, getCalc)
    }
  }

  "SumNumberDFMetricCalculator" must {
    val mId = "sumNumber"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)
    val results1 = Seq(63.0, 79.28, 80.10000000000002, 69.939)
    val results2 = Seq(63.0, 79.28, 80.1, 69.939)
    val failCountsSingleSeq = Seq(0, 0, 0, 6)
    val failCountsMultiSeq = Seq(0, 0, 0, 4)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, _) => {
      SumNumberDFMetricCalculator(mId, cols)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results1, failCountsSingleSeq, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results2, failCountsMultiSeq, params, getCalc)
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), params, getCalc)
    }
  }

  "AvgNumberDFMetricCalculator" must {
    val mId = "avgNumber"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)
    val results = testSingleColSeq.map(s => s.flatMap(tryToDouble)).map(s => s.sum / s.length)
    val failCountsSingleSeq = Seq(0, 0, 0, 6)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, _) => {
      AvgNumberDFMetricCalculator(mId, cols)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        testMultiColDFs, mId, multiCols, results, failCountsSingleSeq, params, getCalc
      )
    }
    "return NaN value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "StdNumberDFMetricCalculator" must {
    val mId = "stdNumber"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)
    // There is some double accuracy error between RDD and DF calculators:
    // RDD Calc Res => List(2.7373953556863744, 2.5334794694692557, 3.104158501107825,  18.52832987616531)
    //  DF Calc Res => List(2.737395355686375,  2.5334794694692553, 3.1041585011078285, 18.52832987616531)
    val results = Seq(2.737395355686375,  2.5334794694692553, 3.1041585011078285, 18.52832987616531)
    val failCountsSingleSeq = Seq(0, 0, 0, 6)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, _) => {
      StdNumberDFMetricCalculator(mId, cols)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        testMultiColDFs, mId, multiCols, results, failCountsSingleSeq, params, getCalc
      )
    }
    "return NaN value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "FormattedNumberDFMetricCalculator" must {
    val mId = "fmtNumber"
    // (params, result, failCntSingle, failCntMulti)
    val inputs = Seq(
      (Map("precision" -> 5, "scale" -> 3, "compareRule" -> "inbound", "reversed" -> false), 4, 21, 5),
      (Map("precision" -> 5, "scale" -> 3, "compareRule" -> "inbound", "reversed" -> true), 4, 4, 3),
      (Map("precision" -> 5, "scale" -> 3, "compareRule" -> "outbound", "reversed" -> false), 18, 7, 5),
      (Map("precision" -> 5, "scale" -> 3, "compareRule" -> "outbound", "reversed" -> true), 18, 18, 5)
    )

    val values = Seq(
      43.113, 39.2763, 21.1248, 94.8884, 96.997, 8.7525, 2.1505, 79.6918, 25.5519, 11.8093, 97.7182, 6.7502, 95.5276,
      57.2292, 16.4476, 67.8032, 68.8456, 57.617, 26.8743, 57.2209, 24.14, 32.7863, 35.7226, 46.2913, 41.1243
    )
    val multiColValues = (0 to 4).map(c => (0 to 4).map(r => c*5 + r)).map(_.map(values(_)))
    val multiColsSpec = Seq("c1", "c2", "c3", "c4", "c5")
    val multiColSchema = StructType(
      multiColsSpec.map(cn => StructField(cn, DoubleType, nullable = true))
    )

    val strValues = Seq("foo", "bar", "baz")

    val testDF = getTestDataFrames(Seq(values), Seq(testSingleColSchemas.tail.head))
    val testMultiDF = getTestDataFrames(Seq(multiColValues), Seq(multiColSchema))
    val testStrDF = getTestDataFrames(Seq(strValues), Seq(testSingleColSchemas.tail.tail.head))


    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val precision = params.getOrElse("precision", 0).asInstanceOf[Int]
      val scale = params.getOrElse("scale", 0).asInstanceOf[Int]
      val compareRule = params.getOrElse("compareRule", "").asInstanceOf[String]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      FormattedNumberDFMetricCalculator(mId, cols, precision, scale, compareRule, reversed)
    }

    "return correct metric value and fail counts for single column sequence" in {
      inputs.foreach{
        case (params, result, failCnt, _) =>
          testMetric(testDF, mId, singleCols, Seq(result), Seq(failCnt), Seq(params), getCalc)
      }
    }
    "return correct metric value and fail counts for multi column sequence" in {
      inputs.foreach{
        case (params, result, _, failCnt) =>
          testMetric(testMultiDF, mId, multiColsSpec, Seq(result), Seq(failCnt), Seq(params), getCalc)
      }
    }

    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), Seq(inputs.head._1), getCalc)
    }
    "return zero value when applied to string sequence which values are not convertible to numbers" in {
      testMetric(testStrDF, mId, singleCols, Seq(0.0), Seq(3), Seq(inputs.head._1), getCalc)
    }
  }

  "CastedNumberDFMetricCalculator" must {
    val mId = "castedNumber"
    val directParams = Seq.fill(4)(0).map(_ => Map("reversed" -> false))
    val reversedParams = Seq.fill(4)(0).map(_ => Map("reversed" -> true))
    val results = Seq(15, 15, 15, 9).map(_.toDouble)
    val failCountsSingleSeq = Seq(0, 0, 0, 6)
    val failCountsSingleSeqRev = Seq(15, 15, 15, 9)
    val failCountsMultiSeq = Seq(0, 0, 0, 4)
    val failCountsMultiSeqRev = Seq(5, 5, 5, 4)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      CastedNumberDFMetricCalculator(mId, cols, reversed)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
    }
  }

  "NumberInDomainDFMetricCalculator" must {
    val mId = "numberInDomain"
    val domain = Seq.fill(4)(Seq(1, 2, 3, 4, 5).map(_.toDouble).toSet)
    val directParams = domain.map(d => Map("domain" -> d, "reversed" -> false))
    val reversedParams = domain.map(d => Map("domain" -> d, "reversed" -> true))
    val results = Seq(8, 4, 5, 5).map(_.toDouble)
    val failCountsSingleSeq = Seq(7, 11, 10, 10)
    val failCountsSingleSeqRev = Seq(8, 4, 5, 5)
    val failCountsMultiSeq = Seq(4, 5, 5, 5)
    val failCountsMultiSeqRev = Seq(5, 4, 4, 4)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val domain = params.getOrElse("domain", Set.empty).asInstanceOf[Set[Double]]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      NumberInDomainDFMetricCalculator(mId, cols, domain, reversed)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
    }
  }

  "NumberOutDomainDFMetricCalculator" must {
    val mId = "numberOutDomain"
    val domain = Seq.fill(4)(Seq(1, 2, 3, 4, 5).map(_.toDouble).toSet)
    val directParams = domain.map(d => Map("domain" -> d, "reversed" -> false))
    val reversedParams = domain.map(d => Map("domain" -> d, "reversed" -> true))
    val results = Seq(7, 11, 10, 4).map(_.toDouble)
    val failCountsSingleSeq = Seq(8, 4, 5, 11)
    val failCountsSingleSeqRev = Seq(7, 11, 10, 4)
    val failCountsMultiSeq = Seq(5, 4, 4, 5)
    val failCountsMultiSeqRev = Seq(4, 5, 5, 4)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val domain = params.getOrElse("domain", Set.empty).asInstanceOf[Set[Double]]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      NumberOutDomainDFMetricCalculator(mId, cols, domain, reversed)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
    }
  }

  "NumberValuesDFMetricCalculator" must {
    val mId = "numberValues"
    val compareValue = 3.0
    val directParams = Seq.fill(4)(Map("compareValue" -> compareValue, "reversed" -> false))
    val reversedParams = Seq.fill(4)(Map("compareValue" -> compareValue, "reversed" -> true))
    val results = Seq(2, 1, 1, 2).map(_.toDouble)
    val failCountsSingleSeq = Seq(13, 14, 14, 13)
    val failCountsSingleSeqRev = Seq(2, 1, 1, 2)
    val failCountsMultiSeq = Seq(5, 5, 5, 5)
    val failCountsMultiSeqRev = Seq(2, 1, 1, 2)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val compareValue = params.getOrElse("compareValue", 0.0).asInstanceOf[Double]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      NumberValuesDFMetricCalculator(mId, cols, compareValue, reversed)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
    }
  }

  "NumberLessThanDFMetricCalculator" must {
    val mId = "numberLessThan"
    val params = (3.0, true)
    val directParams = Seq.fill(4)(Map("compareValue" -> params._1, "includeBound" -> params._2, "reversed" -> false))
    val reversedParams = Seq.fill(4)(Map("compareValue" -> params._1, "includeBound" -> params._2, "reversed" -> true))
    val results = Seq(7, 4, 5, 4).map(_.toDouble)
    val failCountsSingleSeq = Seq(8, 11, 10, 11)
    val failCountsSingleSeqRev = Seq(7, 4, 5, 4)
    val failCountsMultiSeq = Seq(4, 5, 5, 5)
    val failCountsMultiSeqRev = Seq(4, 4, 3, 2)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val compareValue = params.getOrElse("compareValue", 0.0).asInstanceOf[Double]
      val includeBound = params.getOrElse("includeBound", false).asInstanceOf[Boolean]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      NumberLessThanDFMetricCalculator(mId, cols, compareValue, includeBound, reversed)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
    }
  }

  "NumberGreaterThanDFMetricCalculator" must {
    val mId = "numberGreaterThan"
    val params = (3.0, false)
    val directParams = Seq.fill(4)(Map("compareValue" -> params._1, "includeBound" -> params._2, "reversed" -> false))
    val reversedParams = Seq.fill(4)(Map("compareValue" -> params._1, "includeBound" -> params._2, "reversed" -> true))
    val results = Seq(8, 11, 10, 5).map(_.toDouble)
    val failCountsSingleSeq = Seq(7, 4, 5, 10)
    val failCountsSingleSeqRev = Seq(8, 11, 10, 5)
    val failCountsMultiSeq = Seq(4, 4, 3, 5)
    val failCountsMultiSeqRev = Seq(4, 5, 5, 3)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val compareValue = params.getOrElse("compareValue", 0.0).asInstanceOf[Double]
      val includeBound = params.getOrElse("includeBound", false).asInstanceOf[Boolean]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      NumberGreaterThanDFMetricCalculator(mId, cols, compareValue, includeBound, reversed)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
    }
  }

  "NumberBetweenDFMetricCalculator" must {
    val mId = "numberBetween"
    val toParams: Boolean => Seq[Map[String, Any]] = rev => Seq.fill(4)(Map(
      "lowerCompareValue" -> 3.0,
      "upperCompareValue" -> 6.0,
      "includeBound" -> true,
      "reversed" -> rev
    ))
    val directParams = toParams(false)
    val reversedParams = toParams(true)

    val results = Seq(7, 5, 3, 5).map(_.toDouble)
    val failCountsSingleSeq = Seq(8, 10, 12, 10)
    val failCountsSingleSeqRev = Seq(7, 5, 3, 5)
    val failCountsMultiSeq = Seq(5, 5, 5, 5)
    val failCountsMultiSeqRev = Seq(5, 4, 2, 4)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val lowerCompareValue = params.getOrElse("lowerCompareValue", 0.0).asInstanceOf[Double]
      val upperCompareValue = params.getOrElse("upperCompareValue", 0.0).asInstanceOf[Double]
      val includeBound = params.getOrElse("includeBound", false).asInstanceOf[Boolean]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      NumberBetweenDFMetricCalculator(mId, cols, lowerCompareValue, upperCompareValue, includeBound, reversed)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
    }
  }

  "NumberNotBetweenDFMetricCalculator" must {
    val mId = "numberNotBetween"
    val toParams: Boolean => Seq[Map[String, Any]] = rev => Seq.fill(4)(Map(
      "lowerCompareValue" -> 2.0,
      "upperCompareValue" -> 8.0,
      "includeBound" -> true,
      "reversed" -> rev
    ))
    val directParams = toParams(false)
    val reversedParams = toParams(true)

    val results = Seq(8, 4, 9, 4).map(_.toDouble)
    val failCountsSingleSeq = Seq(7, 11, 6, 11)
    val failCountsSingleSeqRev = Seq(8, 4, 9, 4)
    val failCountsMultiSeq = Seq(5, 5, 5, 5)
    val failCountsMultiSeqRev = Seq(5, 3, 5, 3)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val lowerCompareValue = params.getOrElse("lowerCompareValue", 0.0).asInstanceOf[Double]
      val upperCompareValue = params.getOrElse("upperCompareValue", 0.0).asInstanceOf[Double]
      val includeBound = params.getOrElse("includeBound", false).asInstanceOf[Boolean]
      val reversed = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      NumberNotBetweenDFMetricCalculator(mId, cols, lowerCompareValue, upperCompareValue, includeBound, reversed)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
    }
  }


}
