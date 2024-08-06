package org.checkita.dqf.core.metrics.df.regular

import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.core.metrics.df.DFMetricCalculator
import org.checkita.dqf.core.metrics.df.regular.ApproxCardinalityDFMetrics._

import scala.collection.mutable
import scala.util.Random

class ApproxCardinalityDFMetricsSpec extends AnyWordSpec with Matchers with DFMetricsTestUtils {

  private val rand = Random
  rand.setSeed(42)

  private val seqTypes: Seq[DataType] = Seq(StringType, DoubleType, StringType, StringType)
  private val testSchemas = getSingleColSchema(seqTypes)
  private val intSchemas = getSingleColSchema(Seq.fill(4)(IntegerType))
  private val strSchemas = getSingleColSchema(Seq.fill(4)(StringType))

  private val testValues = Seq(
    Seq("Gpi2C7", "Gpi2C7", "xTOn6x", "3xGSz0", "Gpi2C7", "Gpi2C7", "Gpi2C7", "xTOn6x", "3xGSz0", "xTOn6x", "M66yO0", "M66yO0"),
    Seq(5.94, 1.72, 5.94, 5.87, 5.94, 5.94, 5.94, 1.72, 5.87, 1.72, 8.26, 8.26),
    Seq("2.54", "7.71", "2.54", "2.16", "2.54", "2.54", "2.54", "7.71", "2.16", "7.71", "6.85", "6.85"),
    Seq("4", "3.14", "foo", "3.0", "-25.321", "-25.321", "[12, 35]", "true", "-25.321", "4", "bar", "3123dasd")
  )

  private val intSeq: Seq[Seq[Int]] = Seq(
    Range.inclusive(1, 10000),
    Range.inclusive(0, 9996, 4),
    Range.inclusive(1, 100000),
    Range.inclusive(0, 99996, 4)
  )
  private val nullIndices = Seq.fill(100)(rand.nextInt(10000)).toSet
  private val emptyIndices = Seq.fill(100)(rand.nextInt(10000)).toSet
  private val nullIntSeq = intSeq.map(s => s.zipWithIndex.map {
    case (_, idx) if nullIndices.contains(idx) => null
    case (v, _) => v
  })
  private val emptyStrSeq = nullIntSeq.map(s => s.zipWithIndex.map {
    case (_, idx) if emptyIndices.contains(idx) => ""
    case (v, _) if v == null => v
    case (v, _) => v.toString
  })
  private val nullFailCnt = nullIntSeq.map(seq => seq.count(_ == null))
  private val emptyFailCnt = emptyStrSeq.map(seq => seq.count(v => v == null || v.asInstanceOf[String] == ""))

  private val testDFs: Seq[DataFrame] = getTestDataFrames(testValues, testSchemas)
  private val multiColDFs: Seq[DataFrame] = getTestDataFrames(
    Seq(Seq(1, 2, 3), Seq(4, 5, 6)),
    getMultiColSchema(Seq(IntegerType))
  )
  private val intDFs: Seq[DataFrame] = getTestDataFrames(intSeq, intSchemas)
  private val nullDFs: Seq[DataFrame] = getTestDataFrames(nullIntSeq, intSchemas)
  private val emptyStrDFs: Seq[DataFrame] = getTestDataFrames(emptyStrSeq, strSchemas)
  private val emptyDF = spark.createDataFrame(sc.emptyRDD[Row], testSchemas.head)

//  Seq(
//    intDFs,
//    nullDFs,
//    emptyStrDFs
//  ).foreach { dfSeq => dfSeq.foreach { df =>
//    df.printSchema()
//    df.show(truncate = false)
//  }}

  "ApproximateDistinctValuesDFMetricCalculator" must {
    val mId = "approxDistValues"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map("accuracyError" -> 0.01))
    val results = Seq(4, 4, 4, 9).map(_.toDouble)
    val failCountsSingleSeq = Seq.fill(4)(0)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val accuracyError = params.getOrElse("accuracyError", 0.005).asInstanceOf[Double]
      ApproximateDistinctValuesDFMetricCalculator(mId, cols, accuracyError)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        multiColDFs, mId, multiCols, results, failCountsSingleSeq, params, getCalc
      )
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), params, getCalc)
    }
  }

  "ApproximateSequenceCompletenessDFMetricCalculator" must {
    val mId = "approxSeqCompleteness"

    val params: Seq[Map[String, Any]] = Seq((0.01, 1), (0.01, 4), (0.001, 1), (0.001, 4)).map {
      case (accuracy, increment) => Map("accuracyError" -> accuracy, "increment" -> increment)
    }
    val results = Seq(
      Seq(0.9974, 1.0004, 0.99963, 1.00028),
      Seq(0.9886, 0.9896, 0.99866, 0.99632),
      Seq(0.9788, 0.9804, 0.99773, 0.9924)
    )
    val failCountsSingleSeq = Seq(Seq.fill(4)(0), nullFailCnt, emptyFailCnt)
    val allDFs: Seq[Seq[DataFrame]] = Seq(intDFs, nullDFs, emptyStrDFs)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val accuracyError = params.getOrElse("accuracyError", 0.005).asInstanceOf[Double]
      val increment = params.getOrElse("increment", 1).asInstanceOf[Integer]
      ApproximateSequenceCompletenessDFMetricCalculator(mId, cols, accuracyError, increment.toLong)
    }

    "return correct metric value and fail counts for single column sequence" in {
      zipT(allDFs, results, failCountsSingleSeq).foreach {
        case (df, res, fc) => testMetric(df, mId, singleCols, res, fc, params, getCalc)
      }
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        multiColDFs, mId, multiCols, results.head, failCountsSingleSeq.head, params, getCalc
      )
    }
    "return zero value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), params, getCalc)
    }
  }

  "TopNDFMetricCalculator" must {
    val mId = "topN"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map("targetNumber" -> 2, "maxCapacity" -> 100))
    val results = Seq(
      Seq(("Gpi2C7", 0.4166666666666667), ("xTOn6x", 0.25)),
      Seq(("5.94", 0.4166666666666667), ("1.72", 0.25)),
      Seq(("2.54", 0.4166666666666667), ("7.71", 0.25)),
      Seq(("-25.321", 0.25), ("4", 0.16666666666666666))
    )
    val failCounts = Seq.fill(4)(0)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val targetNumber = params.getOrElse("targetNumber", 1).asInstanceOf[Int]
      val maxCapacity = params.getOrElse("maxCapacity", 100).asInstanceOf[Int]
      TopNDFMetricCalculator(mId, cols, maxCapacity, targetNumber)
    }

    def runTopNMetricCalc(df: DataFrame,
                          calculator: DFMetricCalculator): (Seq[(String, Double)], Int) = {
      val metDf = df.select(calculator.result, calculator.errors)
      val processed = metDf.collect().head
      val result = processed.getAs[mutable.WrappedArray[Row]](0).map { row =>
        row.getString(0) -> row.getDouble(1)
      }
      val errors = processed.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]](1)
      (result.toSeq, errors.size)
    }

    def testTopNMetric(dataFrames: Seq[DataFrame],
                       mId: String,
                       metCols: Seq[String],
                       results: Seq[Seq[(String, Double)]],
                       failCounts: Seq[Int],
                       paramSeq: Seq[Map[String, Any]],
                       fCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator): Unit = {

      val zipped: Seq[(DataFrame, Map[String, Any], Seq[(String, Double)], Int)] =
        zipT(dataFrames, paramSeq, results, failCounts).toSeq

      zipped.foreach {
        case (df, params, res, fc) =>
          val calculator = fCalc(mId, metCols, params)
          val (result, errorsNum) = runTopNMetricCalc(df, calculator)

          result.zip(res).foreach {
            case ((resVal, resFreq), (expVal, expFreq)) =>
              resVal shouldEqual expVal
              if (expFreq.isNaN) resFreq.isNaN shouldEqual true else resFreq shouldEqual expFreq
          }

          errorsNum shouldEqual fc
      }
    }

    "return correct metric value and fail counts for single column sequence" in {
      testTopNMetric(testDFs, mId, singleCols, results, failCounts, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testTopNMetric(
        multiColDFs, mId, multiCols, results.take(1), failCounts.take(1), params.take(1), getCalc
      )
    }
    "return empty string for value and NaN for frequency when applied to empty sequence" in {
      testTopNMetric(Seq(emptyDF), mId, singleCols, Seq(Seq(("", Double.NaN))), Seq(0), params.take(1), getCalc)
    }
  }
}
