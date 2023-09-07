package ru.raiffeisen.checkita.utils.io.dbmanager

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import ru.raiffeisen.checkita.checks.{CheckResultFormatted, LoadCheckResultFormatted}
import ru.raiffeisen.checkita.metrics._
import ru.raiffeisen.checkita.utils.camelToUnderscores

import java.sql.Timestamp

trait SparkDBManager extends DBManager {
  type R = Row

  protected val uniqueFields: Map[String, Seq[String]] = Map(
    "results_metric_columnar" -> Seq("job_id", "metric_id", "reference_date"),
    "results_metric_file" -> Seq("job_id", "metric_id", "reference_date"),
    "results_metric_composed" -> Seq("job_id", "metric_id", "reference_date"),
    "results_check" -> Seq("job_id", "check_id", "reference_date"),
    "results_check_load" -> Seq("job_id", "check_id", "reference_date"),
  )

  protected def buildSchema(result: AnyRef): StructType = StructType(
    result.getClass.getDeclaredFields.map{ f =>
      val fieldName = camelToUnderscores(f.getName)
      f.setAccessible(true)
      val fieldValue: Any = f.get(result)
      val fieldType = fieldValue match {
        case _: String => StringType
        case _: Double => DoubleType
        case _: Timestamp => TimestampType
        case _ => throw new IllegalArgumentException(s"Result field '$fieldName' has unsupported type.'")
      }
      StructField(fieldName, fieldType, nullable = true)
    }.toSeq
  )

  protected def updateData(spark: SparkSession,
                           currentData: DataFrame,
                           metrics: Seq[AnyRef],
                           resultSchema: StructType,
                           uniqueCols: Seq[String]): DataFrame = {

    val currentDataFmt = currentData
      .select(resultSchema.map(c => col(c.name).cast(c.dataType)) : _*)
      .withColumn("rank", lit(2))


    val newData = metrics.head match {
      case _: ColumnMetricResultFormatted =>
        spark.createDataFrame(metrics.asInstanceOf[Seq[ColumnMetricResultFormatted]])
      case _: FileMetricResultFormatted =>
        spark.createDataFrame(metrics.asInstanceOf[Seq[FileMetricResultFormatted]])
      case _: ComposedMetricResultFormatted =>
        spark.createDataFrame(metrics.asInstanceOf[Seq[ComposedMetricResultFormatted]])
      case _: CheckResultFormatted =>
        spark.createDataFrame(metrics.asInstanceOf[Seq[CheckResultFormatted]])
      case _: LoadCheckResultFormatted =>
        spark.createDataFrame(metrics.asInstanceOf[Seq[LoadCheckResultFormatted]])
      case _ => throw new IllegalArgumentException("Unsupported results type")
    }

    val newDataFmt = spark.createDataFrame(newData.rdd, resultSchema)
      .withColumn("rank", lit(1))

    assert(
      currentDataFmt.schema.zip(newDataFmt.schema).forall(t => t._1 == t._2),
      "History storage schema does not match the schema of result to written"
    )

    val w = Window.partitionBy(uniqueCols.map(col) : _*).orderBy(col("rank"))

    currentDataFmt.union(newDataFmt)
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1)
      .select(resultSchema.map(c => col(c.name)) : _*)
  }

  def colMetConverter(r: R): Seq[ColumnMetricResult] = {
    val columIds = r.get(5).asInstanceOf[String].filterNot("{}".toSet).split(", ").toSeq
    Seq(ColumnMetricResult(
      r.get(0).asInstanceOf[String],
      r.get(1).asInstanceOf[String],
      r.get(2).asInstanceOf[String],
      r.get(3).asInstanceOf[String],
      r.get(4).asInstanceOf[String],
      columIds,
      r.get(6).asInstanceOf[String],
      r.get(7).asInstanceOf[Double],
      r.get(8).asInstanceOf[String]
    ))
  }

  def colMetFmtConverter(r: R): Seq[ColumnMetricResultFormatted] = Seq(ColumnMetricResultFormatted(
    r.get(0).asInstanceOf[String],
    r.get(1).asInstanceOf[String],
    r.get(2).asInstanceOf[String],
    r.get(3).asInstanceOf[String],
    r.get(4).asInstanceOf[String],
    r.get(5).asInstanceOf[String],
    r.get(6).asInstanceOf[String],
    r.get(7).asInstanceOf[Double],
    r.get(8).asInstanceOf[String],
    r.get(9).asInstanceOf[Timestamp],
    r.get(10).asInstanceOf[Timestamp]
  ))

  def fileMetConverter(r: R): Seq[FileMetricResult] = Seq(FileMetricResult(
    r.get(0).asInstanceOf[String],
    r.get(1).asInstanceOf[String],
    r.get(2).asInstanceOf[String],
    r.get(3).asInstanceOf[String],
    r.get(4).asInstanceOf[String],
    r.get(5).asInstanceOf[Double],
    r.get(6).asInstanceOf[String],
  ))

  def compMetConverter(r: R): Seq[ComposedMetricResult] = Seq(ComposedMetricResult(
    r.get(0).asInstanceOf[String],
    r.get(1).asInstanceOf[String],
    r.get(2).asInstanceOf[String],
    r.get(3).asInstanceOf[String],
    r.get(4).asInstanceOf[String],
    r.get(5).asInstanceOf[String],
    r.get(6).asInstanceOf[Double],
    r.get(7).asInstanceOf[String]
  ))
}
