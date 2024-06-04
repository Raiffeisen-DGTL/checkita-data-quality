package ru.raiffeisen.checkita.core.metrics.df.regular

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common._
import ru.raiffeisen.checkita.core.metrics.df.DFMetricCalculator
import ru.raiffeisen.checkita.core.metrics.df.regular.BasicStringDFMetrics._


class BasicStringDFMetricsSpec extends AnyWordSpec with Matchers with DFMetricsTestUtils {

  private val seqTypes: Seq[DataType] = Seq(StringType, StringType, DoubleType, StringType)
  private val testSingleColSchemas = getSingleColSchema(seqTypes)
  private val testMultiColSchemas = getMultiColSchema(seqTypes)

  private val testSingleColSeq = Seq(
    Seq("Gpi2C7", "DgXDiA", "Gpi2C7", "Gpi2C7", "M66yO0", "M66yO0", "M66yO0", "xTOn6x", "xTOn6x", "3xGSz0", "3xGSz0", "Gpi2C7"),
    Seq("3.09", "3.09", "6.83", "3.09", "6.83", "3.09", "6.83", "7.28", "2.77", "6.66", "7.28", "2.77"),
    Seq(5.85, 5.85, 5.85, 8.32, 8.32, 7.24, 7.24, 7.24, 8.32, 9.15, 7.24, 5.85),
    Seq("4", "3.14", "foo", "3.0", "-25.321", "bar", "[12, 35]", "true", "4", "3", "-25.321", "3123dasd")
  )

  private val testMultiColSeq = testSingleColSeq.map(
    s => (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_)))
  )

  private val nullIndices = Set(3, 7, 9)
  private val emptyIndices = Set(1, 5, 8)
  private val nullSingleColSeq = testSingleColSeq.map(s => s.zipWithIndex.map {
    case (_, idx) if nullIndices.contains(idx) => null
    case (v, _) => v
  })
  private val emptySingleColSeq = nullSingleColSeq.map(s => s.zipWithIndex.map {
    case (v, idx) if v.isInstanceOf[String] && emptyIndices.contains(idx) => ""
    case (v, _) => v
  })
  private val nullMultiColSeq = nullSingleColSeq.map(s => (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_))))
  private val emptyMultiColSeq = emptySingleColSeq.map(s => (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(s(_))))

  private val emptyDF = spark.createDataFrame(sc.emptyRDD[Row], testSingleColSchemas.head)
  private val testSingleColDFs: Seq[DataFrame] = getTestDataFrames(testSingleColSeq, testSingleColSchemas)
  private val testMultiColDFs: Seq[DataFrame] = getTestDataFrames(testMultiColSeq, testMultiColSchemas)
  private val nullSingleColDFs: Seq[DataFrame] = getTestDataFrames(nullSingleColSeq, testSingleColSchemas)
  private val nullMultiColDFs: Seq[DataFrame] = getTestDataFrames(nullMultiColSeq, testMultiColSchemas)
  private val emptySingleColDFs: Seq[DataFrame] = getTestDataFrames(emptySingleColSeq, testSingleColSchemas)
  private val emptyMultiColDFs: Seq[DataFrame] = getTestDataFrames(emptyMultiColSeq, testMultiColSchemas)

//  emptyDF.printSchema()
//  emptyDF.show(truncate = false)

//  Seq(
////    testSingleColDFs,
////    testMultiColDFs,
////    nullSingleColDFs,
////    nullMultiColDFs,
////    emptySingleColDFs,
////    emptyMultiColDFs
//  ).foreach { dfSeq => dfSeq.foreach { df =>
//    df.printSchema()
//    df.show(truncate = false)
//  }}

  "RegexMatchDFMetricCalculator" must {
    val mId = "regexMatch"
    val regexList: Seq[String] = Seq(
      """^[a-zA-Z]{6}$""",
      """^3\..+""",
      """.*32$""",
      """.*[a-zA-Z]$"""
    )
    val directParams = regexList.map(rgx => Map(
      "regex" -> rgx,
      "reversed" -> false
    ))
    val reversedParams = regexList.map(rgx => Map(
      "regex" -> rgx,
      "reversed" -> true
    ))
    val results = Seq(1, 4, 3, 4).map(_.toDouble)
    val failCountsSingleSeq = Seq(11, 8, 9, 8)
    val failCountsSingleSeqRev = Seq(1, 4, 3, 4)
    val failCountsMultiSeq = Seq(4, 4, 4, 4)
    val failCountsMultiSeqRev = Seq(1, 2, 2, 4)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val rgx = params.getOrElse("regex", "").asInstanceOf[String]
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      RegexMatchDFMetricCalculator(mId, cols, rgx, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams, getCalc)
    }
  }

  "RegexMismatchDFMetricCalculator" must {
    val mId = "regexMismatch"
    val regexList: Seq[String] = Seq(
      """^[a-zA-Z]{6}$""",
      """^3\..+""",
      """.*32$""",
      """.*[a-zA-Z]$"""
    )
    val directParams = regexList.map(rgx => Map(
      "regex" -> rgx,
      "reversed" -> false
    ))
    val reversedParams = regexList.map(rgx => Map(
      "regex" -> rgx,
      "reversed" -> true
    ))
    val results = Seq(11, 8, 9, 8).map(_.toDouble)
    val failCountsSingleSeq = Seq(1, 4, 3, 4)
    val failCountsSingleSeqRev = Seq(11, 8, 9, 8)
    val failCountsMultiSeq = Seq(1, 2, 2, 4)
    val failCountsMultiSeqRev = Seq(4, 4, 4, 4)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val rgx = params.getOrElse("regex", "").asInstanceOf[String]
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      RegexMismatchDFMetricCalculator(mId, cols, rgx, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams, getCalc)
    }
  }

  "NullValuesDFMetricCalculator" must {
    val mId = "nullValues"
    val directParams = Seq.fill(4)(0).map(_ => Map("reversed" -> false))
    val reversedParams = Seq.fill(4)(0).map(_ => Map("reversed" -> true))
    val results = Seq.fill(4)(3).map(_.toDouble)
    val failCountsSingleSeq = Seq.fill(4)(9)
    val failCountsSingleSeqRev = Seq.fill(4)(3)
    val failCountsMultiSeq = Seq(4, 4, 4, 4)
    val failCountsMultiSeqRev = Seq(3, 3, 3, 3)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      NullValuesDFMetricCalculator(mId, cols, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(nullSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(nullSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(nullMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(nullMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams, getCalc)
    }
  }

  "EmptyValuesDFMetricCalculator" must {
    val mId = "emptyValues"
    val directParams = Seq.fill(4)(0).map(_ => Map("reversed" -> false))
    val reversedParams = Seq.fill(4)(0).map(_ => Map("reversed" -> true))
    val results = Seq(3, 3, 0, 3).map(_.toDouble)
    val failCountsSingleSeq = Seq(9, 9, 12, 9)
    val failCountsSingleSeqRev = Seq(3, 3, 0, 3)
    val failCountsMultiSeq = Seq(4, 4, 4, 4)
    val failCountsMultiSeqRev = Seq(3, 3, 0, 3)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      EmptyValuesDFMetricCalculator(mId, cols, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(emptySingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(emptySingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(emptyMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(emptyMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams, getCalc)
    }
  }

  "CompletenessDFMetricCalculator" must {
    val mId = "completeness"
    val directParams = Seq(false, false, true).map { includeEmptyStrings =>
      Seq.fill(4)(0).map(_ => Map("reversed" -> false, "includeEmptyStrings" -> includeEmptyStrings))
    }
    val reversedParams = Seq(false, false, true).map { includeEmptyStrings =>
      Seq.fill(4)(0).map(_ => Map("reversed" -> true, "includeEmptyStrings" -> includeEmptyStrings))
    }
    val results = Seq(Seq.fill(4)(1.0), Seq.fill(4)(0.75), Seq(0.5, 0.5, 0.75, 0.5))
    val failCountsSingleSeq = Seq(Seq.fill(4)(12), Seq.fill(4)(9), Seq(6, 6, 9, 6))
    val failCountsSingleSeqRev = Seq(Seq.fill(4)(0), Seq.fill(4)(3), Seq(6, 6, 3, 6))
    val failCountsMultiSeq = Seq(Seq(4, 4, 4, 4), Seq(4, 4, 4, 4), Seq(4, 4, 4, 4))
    val failCountsMultiSeqRev = Seq(Seq(0, 0, 0, 0), Seq(3, 3, 3, 3), Seq(4, 4, 3, 4))
    val allSingleColDFs = Seq(testSingleColDFs, nullSingleColDFs, emptySingleColDFs)
    val allMultiColDFs = Seq(testMultiColDFs, nullMultiColDFs, emptyMultiColDFs)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val ies = params.getOrElse("includeEmptyStrings", false).asInstanceOf[Boolean]
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      CompletenessDFMetricCalculator(mId, cols, ies, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      (allSingleColDFs, directParams, results, failCountsSingleSeq).zipped.foreach {
        case (df, params, res, fc) => testMetric(df, mId, singleCols, res, fc, params, getCalc)
      }
      (allSingleColDFs, reversedParams, results, failCountsSingleSeqRev).zipped.foreach {
        case (df, params, res, fc) => testMetric(df, mId, singleCols, res, fc, params, getCalc)
      }
    }
    "return correct metric value and fail counts for multi column sequence" in {
      (allMultiColDFs, directParams, results, failCountsMultiSeq).zipped.foreach {
        case (df, params, res, fc) => testMetric(df, mId, multiCols, res, fc, params, getCalc)
      }
      (allMultiColDFs, reversedParams, results, failCountsMultiSeqRev).zipped.foreach {
        case (df, params, res, fc) => testMetric(df, mId, multiCols, res, fc, params, getCalc)
      }
    }
    "return NaN when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.NaN), Seq(0), directParams.head, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.NaN), Seq(0), reversedParams.head, getCalc)
    }
  }

  "EmptinessDFMetricCalculator" must {
    val mId = "emptiness"
    val directParams = Seq(false, false, true).map { includeEmptyStrings =>
      Seq.fill(4)(0).map(_ => Map("reversed" -> false, "includeEmptyStrings" -> includeEmptyStrings))
    }
    val reversedParams = Seq(false, false, true).map { includeEmptyStrings =>
      Seq.fill(4)(0).map(_ => Map("reversed" -> true, "includeEmptyStrings" -> includeEmptyStrings))
    }
    val results = Seq(Seq.fill(4)(0.0), Seq.fill(4)(0.25), Seq(0.5, 0.5, 0.25, 0.5))
    val failCountsSingleSeq = Seq(Seq.fill(4)(12), Seq.fill(4)(9), Seq(6, 6, 9, 6))
    val failCountsSingleSeqRev = Seq(Seq.fill(4)(0), Seq.fill(4)(3), Seq(6, 6, 3, 6))
    val failCountsMultiSeq = Seq(Seq(4, 4, 4, 4), Seq(4, 4, 4, 4), Seq(4, 4, 4, 4))
    val failCountsMultiSeqRev = Seq(Seq(0, 0, 0, 0), Seq(3, 3, 3, 3), Seq(4, 4, 3, 4))
    val allSingleColDFs = Seq(testSingleColDFs, nullSingleColDFs, emptySingleColDFs)
    val allMultiColDFs = Seq(testMultiColDFs, nullMultiColDFs, emptyMultiColDFs)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val ies = params.getOrElse("includeEmptyStrings", false).asInstanceOf[Boolean]
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      EmptinessDFMetricCalculator(mId, cols, ies, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      (allSingleColDFs, directParams, results, failCountsSingleSeq).zipped.foreach {
        case (df, params, res, fc) => testMetric(df, mId, singleCols, res, fc, params, getCalc)
      }
      (allSingleColDFs, reversedParams, results, failCountsSingleSeqRev).zipped.foreach {
        case (df, params, res, fc) => testMetric(df, mId, singleCols, res, fc, params, getCalc)
      }
    }
    "return correct metric value and fail counts for multi column sequence" in {
      (allMultiColDFs, directParams, results, failCountsMultiSeq).zipped.foreach {
        case (df, params, res, fc) => testMetric(df, mId, multiCols, res, fc, params, getCalc)
      }
      (allMultiColDFs, reversedParams, results, failCountsMultiSeqRev).zipped.foreach {
        case (df, params, res, fc) => testMetric(df, mId, multiCols, res, fc, params, getCalc)
      }
    }
    "return NaN when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams.head, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams.head, getCalc)
    }
  }

  "MinStringDFMetricCalculator" must {
    val mId = "minString"
    val params = Seq.fill(4)(Map.empty[String, Any])
    val results = Seq(6, 4, 4, 1).map(_.toDouble)
    val failCounts = Seq.fill(4)(0)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator =
      (mId, cols, _) => MinStringDFMetricCalculator(mId, cols)

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCounts, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCounts, params, getCalc)
    }
    "return max Int value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Int.MaxValue.toDouble), Seq(0), params, getCalc)
    }
  }

  "MaxStringDFMetricCalculator" must {
    val mId = "maxString"
    val params = Seq.fill(4)(Map.empty[String, Any])
    val results = Seq(6, 4, 4, 8).map(_.toDouble)
    val failCounts = Seq.fill(4)(0)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator =
      (mId, cols, _) => MaxStringDFMetricCalculator(mId, cols)

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCounts, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCounts, params, getCalc)
    }
    "return min Int value when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Int.MinValue.toDouble), Seq(0), params, getCalc)
    }
  }

  "AvgStringDFMetricCalculator" must {
    val mId = "avgString"
    val params = Seq.fill(4)(Map.empty[String, Any])
    val results = Seq(6, 4, 4, 4.166666666666667)
    val failCounts = Seq.fill(4)(0)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator =
      (mId, cols, _) => AvgStringDFMetricCalculator(mId, cols)

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCounts, params, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCounts, params, getCalc)
    }
    "return NaN when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(Double.NaN), Seq(0), params, getCalc)
    }
  }

  "FormattedDateDFMetricCalculator" must {
    val mId = "dateFmt"
    val formats: Seq[String] = Seq("yyyy-MM-dd", "MMM dd, yyyy", "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "h:mm a")
    val toParams: (Seq[String], Boolean) => Seq[Map[String, Any]] = (params, rev) => params.map { fmt =>
      Map("dateFormat" -> fmt, "reversed" -> rev)
    }
    val directParams = toParams(formats, false)
    val reversedParams = toParams(formats, true)
    val results = Seq(2, 1, 1, 2).map(_.toDouble)
    val failCountsSingleSeq = Seq(10, 11, 11, 10)
    val failCountsSingleSeqRev = Seq(2, 1, 1, 2)
    val failCountsMultiSeq = Seq(4, 4, 4, 4)
    val failCountsMultiSeqRev = Seq(2, 1, 1, 2)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val fmt = params.getOrElse("dateFormat", "").asInstanceOf[String]
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      FormattedDateDFMetricCalculator(mId, cols, fmt, rev)
    }

    val valuesSingleCol = Seq(
      "Gpi2C7",
      "DgXDiA",
      "2022-03-43",
      "2001-07-04T12:08:56.235-0700",
      "Feb 16, 2022",
      "2021-12-31",
      "12:08 PM",
      "xTOn6x",
      "xTOn6x",
      "08:44 AM",
      "3xGSz0",
      "1999-01-21"
    )
    val valuesMultiCol = (0 to 3).map(c => (0 to 2).map(r => c*3 + r)).map(_.map(valuesSingleCol(_)))

    val singleColDFs: Seq[DataFrame] = getTestDataFrames(
      Seq.fill(4)(valuesSingleCol), Seq.fill(4)(testSingleColSchemas.head)
    )
    val multiColDFs: Seq[DataFrame] = getTestDataFrames(
      Seq.fill(4)(valuesMultiCol), Seq.fill(4)(testMultiColSchemas.head)
    )

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(singleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(singleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(multiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(multiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams, getCalc)
    }
  }

  "StringLengthDFMetricCalculator" must {
    val mId = "stringLength"
    val paramList: Seq[(Int, String)] = Seq((6, "eq"), (4, "lte"), (4, "gt"), (5, "gte"))
    val toParams: (Seq[(Int, String)], Boolean) => Seq[Map[String, Any]] = (params, rev) => params.map {
      case (len, rule) => Map(
        "length" -> len,
        "compareRule" -> rule,
        "reversed" -> rev
      )
    }
    val directParams = toParams(paramList, false)
    val reversedParams = toParams(paramList, true)

    val results = Seq(12, 12, 0, 4).map(_.toDouble)
    val failCountsSingleSeq = Seq(0, 0, 12, 8)
    val failCountsSingleSeqRev = Seq(12, 12, 0, 4)
    val failCountsMultiSeq = Seq(0, 0, 4, 4)
    val failCountsMultiSeqRev = Seq(4, 4, 0, 3)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val len = params.getOrElse("length", 0).asInstanceOf[Int]
      val rule = params.getOrElse("compareRule", "").asInstanceOf[String]
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      StringLengthDFMetricCalculator(mId, cols, len, rule, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams, getCalc)
    }
  }

  "StringInDomainDFMetricCalculator" must {
    val mId = "stringInDomain"
    val paramList: Seq[Set[String]] = Seq(
      Set("M66yO0", "DgXDiA"),
      Set("7.28", "2.77"),
      Set("8.32", "9.15"),
      Set("[12, 35]", "true")
    )
    val toParams: (Seq[Set[String]], Boolean) => Seq[Map[String, Any]] =
      (params, rev) => params.map { domain => Map(
        "domain" -> domain,
        "reversed" -> rev
      )
    }
    val directParams = toParams(paramList, false)
    val reversedParams = toParams(paramList, true)

    val results = Seq(4, 4, 4, 2).map(_.toDouble)
    val failCountsSingleSeq = Seq(8, 8, 8, 10)
    val failCountsSingleSeqRev = Seq(4, 4, 4, 2)
    val failCountsMultiSeq = Seq(4, 4, 4, 4)
    val failCountsMultiSeqRev = Seq(3, 2, 3, 1)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val domain = params.getOrElse("domain", 0).asInstanceOf[Set[String]]
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      StringInDomainDFMetricCalculator(mId, cols, domain, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams, getCalc)
    }
  }

  "StringOutDomainDFMetricCalculator" must {
    val mId = "stringInDomain"
    val paramList: Seq[Set[String]] = Seq(
      Set("M66yO0", "DgXDiA"),
      Set("7.28", "2.77"),
      Set("8.32", "9.15"),
      Set("[12, 35]", "true")
    )
    val toParams: (Seq[Set[String]], Boolean) => Seq[Map[String, Any]] =
      (params, rev) => params.map { domain => Map(
        "domain" -> domain,
        "reversed" -> rev
      )}
    val directParams = toParams(paramList, false)
    val reversedParams = toParams(paramList, true)

    val results = Seq(8, 8, 8, 10).map(_.toDouble)
    val failCountsSingleSeq = Seq(4, 4, 4, 2)
    val failCountsSingleSeqRev = Seq(8, 8, 8, 10)
    val failCountsMultiSeq = Seq(3, 2, 3, 1)
    val failCountsMultiSeqRev = Seq(4, 4, 4, 4)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val domain = params.getOrElse("domain", 0).asInstanceOf[Set[String]]
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      StringOutDomainDFMetricCalculator(mId, cols, domain, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams, getCalc)
    }
  }

  "StringValuesDFMetricCalculator" must {
    val mId = "stringInDomain"
    val paramList: Seq[String] = Seq("Gpi2C7", "3.09", "5.85", "4")
    val toParams: (Seq[String], Boolean) => Seq[Map[String, Any]] =
      (params, rev) => params.map { value => Map(
        "compareValue" -> value,
        "reversed" -> rev
      )}
    val directParams = toParams(paramList, false)
    val reversedParams = toParams(paramList, true)

    val results = Seq(4, 4, 4, 2).map(_.toDouble)
    val failCountsSingleSeq = Seq(8, 8, 8, 10)
    val failCountsSingleSeqRev = Seq(4, 4, 4, 2)
    val failCountsMultiSeq = Seq(4, 4, 3, 4)
    val failCountsMultiSeqRev = Seq(3, 2, 2, 2)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator = (mId, cols, params) => {
      val cv = params.getOrElse("compareValue", 0).asInstanceOf[String]
      val rev = params.getOrElse("reversed", false).asInstanceOf[Boolean]
      StringValuesDFMetricCalculator(mId, cols, cv, rev)
    }

    "return correct metric value and fail counts for single column sequence" in {
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeq, directParams, getCalc)
      testMetric(testSingleColDFs, mId, singleCols, results, failCountsSingleSeqRev, reversedParams, getCalc)
    }
    "return correct metric value and fail counts for multi column sequence" in {
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeq, directParams, getCalc)
      testMetric(testMultiColDFs, mId, multiCols, results, failCountsMultiSeqRev, reversedParams, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), directParams, getCalc)
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), reversedParams, getCalc)
    }
  }
}
