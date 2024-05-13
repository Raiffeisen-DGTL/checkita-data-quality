package ru.raiffeisen.checkita.core.metrics.df.regular

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common.{sc, spark}
import ru.raiffeisen.checkita.core.metrics.MetricName
import ru.raiffeisen.checkita.core.metrics.df.DFMetricCalculator
import ru.raiffeisen.checkita.core.metrics.df.regular.FileDFMetrics.RowCountDFMetricCalculator
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricCalculator
import ru.raiffeisen.checkita.core.metrics.rdd.regular.FileRDDMetrics.RowCountRDDMetricCalculator

import scala.util.Random

class FileDFMetricsSpec extends AnyWordSpec with Matchers with DFMetricsTestUtils {
  val rand: Random.type = Random

  private val seqTypes: Seq[DataType] = Seq(IntegerType, StringType, DoubleType)
  private val testSchemas = getSingleColSchema(seqTypes)
  private val testValues = Seq(
    Seq.fill(42)(Seq.fill(1)(rand.nextInt(100))),
    Seq.fill(42)(Seq.fill(4)(rand.alphanumeric.take(5).mkString)),
    Seq.fill(42)(Seq.fill(42)(rand.nextDouble))
  )
  private val testDFs: Seq[DataFrame] = getTestDataFrames(testValues, testSchemas)
  private val emptyDF = spark.createDataFrame(sc.emptyRDD[Row], testSchemas.head)

  "RowCountDFMetricCalculator" must {
    val mId = "rowCount"
    val params: Seq[Map[String, Any]] = Seq.fill(4)(Map.empty)
    val results = Seq.fill(4)(42).map(_.toDouble)
    val failCountsSingleSeq = Seq.fill(4)(0)

    val getCalc: (String, Seq[String], Map[String, Any]) => DFMetricCalculator =
      (mId, _, _) => RowCountDFMetricCalculator(mId)

    "return correct metric value for input sequence" in {
      testMetric(testDFs, mId, singleCols, results, failCountsSingleSeq, params, getCalc)
    }
    "return zero when applied to empty sequence" in {
      testMetric(Seq(emptyDF), mId, singleCols, Seq(0.0), Seq(0), params, getCalc)
    }
   }

}
