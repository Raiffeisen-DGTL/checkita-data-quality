package org.checkita.dqf.core.metrics.df.regular

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.core.metrics.df.GroupingDFMetricCalculator
import org.checkita.dqf.core.metrics.df.regular.GroupingDFMetrics._

import scala.collection.mutable

class GroupingDFMetricsSpec extends AnyWordSpec with Matchers with DFMetricsTestUtils {

  private val seqTypes: Seq[DataType] = Seq(StringType, StringType, DoubleType, StringType)
  private val testSingleColSchemas = getSingleColSchema(seqTypes)
  private val testMultiColSchemas = getMultiColSchema(seqTypes)
  private val testRangeSchema = getSingleColSchema(Seq.fill(4)(StringType))

  private val testSingleColSeq = Seq(
    Seq("Gpi2C7", "DgXDiA", "Gpi2C7", "Gpi2C7", "M66yO0", "M66yO0", "M66yO0", "xTOn6x", "xTOn6x", "3xGSz0", "3xGSz0", "Gpi2C7"),
    Seq("3.09", "3.09", "6.83", "3.09", "6.83", "3.09", "6.83", "7.28", "2.77", "6.83", "7.28", "2.77"),
    Seq(5.85, 5.85, 5.85, 8.32, 8.32, 7.24, 7.24, 7.24, 8.32, 9.15, 7.24, 5.85),
    Seq("4", "3.14", "foo", "3.0", "-25.321", "bar", "[12, 35]", "true", "4", "3", "-25.321", "3123dasd")
  )

  private val testRangesSeq = Seq(
    Range.inclusive(1, 100).map(_.toString),
    Range.inclusive(0, 96, 4).map(_.toString),
    Range.inclusive(1, 1000000).map(_.toString),
    Range.inclusive(0, 999996, 4).map(_.toString)
  )

  private val testMultiColSeq = testSingleColSeq.map(
    s => (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_)))
  )

  private val nullIndices = Set(3, 7, 9)
  private val nullSingleColSeq = testSingleColSeq.map(s => s.zipWithIndex.map {
    case (_, idx) if nullIndices.contains(idx) => null
    case (v, _) => v
  })
  private val nullMultiColSeq = nullSingleColSeq.map(s => (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_))))

  private val rangeNullIndices = Set(3, 7, 9, 11)
  private val rangeEmptyIndices = Set(4, 8, 12, 16)

  private val nullRangesSeq = testRangesSeq.map(s => s.zipWithIndex.map {
    case (_, idx) if rangeNullIndices.contains(idx) => null
    case (v, _) => v
  })
  private val emptyRangesSeq = nullRangesSeq.map(s => s.zipWithIndex.map {
    case (_, idx) if rangeEmptyIndices.contains(idx) => ""
    case (v, _) => v
  })


  private val emptyDF = spark.createDataFrame(sc.emptyRDD[Row], testSingleColSchemas.head)
  private val testSingleColDFs: Seq[DataFrame] = getTestDataFrames(testSingleColSeq, testSingleColSchemas)
  private val testMultiColDFs: Seq[DataFrame] = getTestDataFrames(testMultiColSeq, testMultiColSchemas)
  private val nullSingleColDFs: Seq[DataFrame] = getTestDataFrames(nullSingleColSeq, testSingleColSchemas)
  private val nullMultiColDFs: Seq[DataFrame] = getTestDataFrames(nullMultiColSeq, testMultiColSchemas)

  private val testRangeDFs: Seq[DataFrame] = getTestDataFrames(testRangesSeq, testRangeSchema)
  private val nullRangeDFs: Seq[DataFrame] = getTestDataFrames(nullRangesSeq, testRangeSchema)
  private val emptyRangeDFs: Seq[DataFrame] = getTestDataFrames(emptyRangesSeq, testRangeSchema)

//  Seq(
//    testSingleColDFs,
//    testMultiColDFs,
//    nullSingleColDFs,
//    nullMultiColDFs
//  ).foreach { dfSeq => dfSeq.foreach { df =>
//    df.printSchema()
//    df.show(truncate = false)
//  }}

  protected def runGroupingDFMetricCalc(df: DataFrame,
                                        calculator: GroupingDFMetricCalculator): (Double, Int) = {
    implicit val colTypes: Map[String, DataType] =
      df.schema.map(sf => sf.name -> sf.dataType).toMap
    val metDf = df.groupBy(calculator.columns.map(col) : _*)
      .agg(calculator.groupResult, calculator.groupErrors)
      .select(calculator.result, calculator.errors)

//    metDf.explain(true)
//    metDf.show(truncate = false)
    val processed = metDf.collect().head
    val result = processed.getDouble(0)
    val errors = processed.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]](1)
    (result, errors.size)
  }

  protected def testGroupingMetric(dataFrames: Seq[DataFrame],
                                   mId: String,
                                   metCols: Seq[String],
                                   results: Seq[Double],
                                   failCounts: Seq[Int],
                                   paramSeq: Seq[Map[String, Any]],
                                   fCalc: (String, Seq[String], Map[String, Any]) => GroupingDFMetricCalculator): Unit = {

    val zipped: Seq[(DataFrame, Map[String, Any], Double, Int)] = zipT(dataFrames, paramSeq, results, failCounts)

    zipped.foreach {
      case (df, params, res, fc) =>
        //         println(s"Testing '$mId' metric. isMultiSeq = ${metCols.size > 1}. Params = $params, Expected: result = $res; failCount = $fc")
        val calculator = fCalc(mId, metCols, params)
        val (result, errorsNum) = runGroupingDFMetricCalc(df, calculator)

        if (res.isNaN) result.isNaN shouldEqual true
        else result shouldEqual res

        errorsNum shouldEqual fc
    }
  }

  "DistinctValuesDFMetricCalculator" must {
    val mId = "distinctValues"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)

    val resultsSingle = Seq(5, 4, 4, 10).map(_.toDouble)
    val resultsMulti = Seq(4, 3, 4, 4).map(_.toDouble)
    val nullResultsSingle = Seq(5, 4, 3, 7).map(_.toDouble)
    val nullResultsMulti = Seq.fill(4)(4).map(_.toDouble)

    val failCounts = Seq.fill(4)(0)
    val nullFailCountsSingle = Seq.fill(4)(3)
    val nullFailCountsMulti = Seq.fill(4)(0)

    val getCalc: (String, Seq[String], Map[String, Any]) => GroupingDFMetricCalculator =
      (mId, cols, _) => DistinctValuesDFMetricCalculator(mId, cols)

    "return correct metric value and fail counts for single column sequence" in {
      testGroupingMetric(testSingleColDFs, mId, singleCols, resultsSingle, failCounts, params, getCalc)
    }
    "return correct metric value and fail counts for single column sequence with null values" in {
      testGroupingMetric(nullSingleColDFs, mId, singleCols, nullResultsSingle, nullFailCountsSingle, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testGroupingMetric(testMultiColDFs, mId, multiCols, resultsMulti, failCounts, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence with null values" in {
      testGroupingMetric(nullMultiColDFs, mId, multiCols, nullResultsMulti, nullFailCountsMulti, params, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testGroupingMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), params, getCalc)
    }
  }

  "DuplicateValuesDFMetricCalculator" must {
    val mId = "distinctValues"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)

    val resultsSingle = Seq(7, 8, 8, 2).map(_.toDouble)
    val resultsMulti = Seq(0, 1, 0, 0).map(_.toDouble)
    val nullResultsSingle = Seq(4, 5, 6, 2).map(_.toDouble)
    val nullResultsMulti = Seq.fill(4)(0).map(_.toDouble)

    val failCountsSingle = Seq(7, 8, 8, 2)
    val failCountsMulti = Seq(0, 1, 0, 0)
    val nullFailCountsSingle = Seq(4, 5, 6, 2)
    val nullFailCountsMulti = Seq.fill(4)(0)

    val getCalc: (String, Seq[String], Map[String, Any]) => GroupingDFMetricCalculator =
      (mId, cols, _) => DuplicateValuesDFMetricCalculator(mId, cols)

    "return correct metric value and fail counts for single column sequence" in {
      testGroupingMetric(testSingleColDFs, mId, singleCols, resultsSingle, failCountsSingle, params, getCalc)
    }
    "return correct metric value and fail counts for single column sequence with null values" in {
      testGroupingMetric(nullSingleColDFs, mId, singleCols, nullResultsSingle, nullFailCountsSingle, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testGroupingMetric(testMultiColDFs, mId, multiCols, resultsMulti, failCountsMulti, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence with null values" in {
      testGroupingMetric(nullMultiColDFs, mId, multiCols, nullResultsMulti, nullFailCountsMulti, params, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testGroupingMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), params, getCalc)
    }
  }

  "SequenceCompletenessDFMetricCalculator" must {
    val mId = "sequenceCompleteness"
    val params: Seq[Map[String, Any]] = Seq(1, 4, 1, 4).map(i => Map("increment" -> i))

    val results = Seq.fill(4)(1).map(_.toDouble)
    val nullResults = Seq(0.96, 0.84, 0.999996, 0.999984)
    val emptyResults = Seq(0.92, 0.68, 0.999992, 0.999968)

    val failCounts = Seq.fill(4)(0)
    val nullFailCounts = Seq.fill(4)(4)
    val emptyFailCounts = Seq.fill(4)(8)

    val getCalc: (String, Seq[String], Map[String, Any]) => GroupingDFMetricCalculator = (mId, cols, params) => {
      val increment = params.getOrElse("increment", 1).asInstanceOf[Int]
      SequenceCompletenessDFMetricCalculator(mId, cols, increment)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testGroupingMetric(testRangeDFs, mId, singleCols, results, failCounts, params, getCalc)
    }
    "return correct metric value and fail counts for single column sequence with null values" in {
      testGroupingMetric(nullRangeDFs, mId, singleCols, nullResults, nullFailCounts, params, getCalc)
    }
    "return correct metric value and fail counts for single column sequence with null and empty values" in {
      testGroupingMetric(emptyRangeDFs, mId, singleCols, emptyResults, emptyFailCounts, params, getCalc)
    }
    "throw an assertion error for multi column sequence" in {
      an [AssertionError] should be thrownBy  testMetric(
        testMultiColDFs, mId, multiCols, results, failCounts, params, getCalc
      )
    }
    "return zero when applied to empty sequence" in {
      testGroupingMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), params, getCalc)
    }
  }
}
