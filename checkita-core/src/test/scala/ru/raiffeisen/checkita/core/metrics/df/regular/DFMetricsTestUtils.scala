package ru.raiffeisen.checkita.core.metrics.df.regular

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common._
import ru.raiffeisen.checkita.core.metrics.df.DFMetricCalculator

import scala.collection.mutable

trait DFMetricsTestUtils { this: AnyWordSpec with Matchers =>
  implicit val keyFields: Seq[String] = Seq.empty

  protected val singleCols: Seq[String] = Seq("c1")
  protected val multiCols: Seq[String] = Seq("c1", "c2", "c3")

  protected def getSingleColSchema(types: Seq[DataType]): Seq[StructType] = types.map(
    dt => StructType(Seq(StructField("c1", dt, nullable = true)))
  )

  protected def getMultiColSchema(types: Seq[DataType]): Seq[StructType] = types.map(dt => StructType(Seq(
    StructField("c1", dt, nullable = true),
    StructField("c2", dt, nullable = true),
    StructField("c3", dt, nullable = true)
  )))

  protected def getTestDataFrames(testData: Seq[Seq[Any]], schemas: Seq[StructType]): Seq[DataFrame] =
    testData.zip(schemas).map {
      case (data, schema) =>
        val rowData = data.map {
          case s: Seq[_] => Row(s: _*)
          case v => Row(v)
        }
        spark.createDataFrame(sc.parallelize(rowData), schema)
    }

  protected def runDFMetricCalc(df: DataFrame,
                                calculator: DFMetricCalculator): (Double, Int) = {
    val metDf = df.select(calculator.result, calculator.errors)
//    metDf.explain(true)
//    metDf.show(truncate = false)
    val processed = metDf.collect().head
    val result = processed.getDouble(0)
    val errors = processed.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]](1)
    (result, errors.size)
  }

  protected def testMetric(dataFrames: Seq[DataFrame],
                 mId: String,
                 metCols: Seq[String],
                 results: Seq[Double],
                 failCounts: Seq[Int],
                 paramSeq: Seq[Map[String, Any]],
                 fCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator): Unit = {

    val zipped: Seq[(DataFrame, Map[String, Any], Double, Int)] = 
      zipT(dataFrames, paramSeq, results, failCounts)

    zipped.foreach {
      case (df, params, res, fc) =>
//         println(s"Testing '$mId' metric. isMultiSeq = ${metCols.size > 1}. Params = $params, Expected: result = $res; failCount = $fc")
        val calculator = fCalc(mId, metCols, params)
        val (result, errorsNum) = runDFMetricCalc(df, calculator)

        if (res.isNaN) result.isNaN shouldEqual true
        else result shouldEqual res

        errorsNum shouldEqual fc
    }
  }
}
