package ru.raiffeisen.checkita.postprocessors

import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import ru.raiffeisen.checkita.checks.CheckResult
import ru.raiffeisen.checkita.metrics.MetricResult
import ru.raiffeisen.checkita.sources.HdfsFile
import ru.raiffeisen.checkita.targets.HdfsTargetConfig
import ru.raiffeisen.checkita.utils.io.{HdfsReader, HdfsWriter}
import ru.raiffeisen.checkita.utils.{DQSettings, parseTargetConfig}

import scala.util.Try

final class TransposeByColumnPostprocessor(config: Config, settings: DQSettings)
  extends BasicPostprocessor(config, settings) {

  import scala.collection.JavaConverters._

  private val vs = config.getString("source")
  private val target: HdfsTargetConfig = {
    val conf = config.getConfig("saveTo")
    parseTargetConfig(conf)(settings).get
  }
  private val numOfColumns
  : Option[Int] = Try(config.getInt("numberOfColumns")).toOption

  private val keys: Option[Array[String]] = Try {
    config.getStringList("keyColumns").asScala.toArray[String]
  }.toOption
  private val offset: Int = 1

  override def process(vsRef: Set[HdfsFile], metRes: Seq[MetricResult], chkRes: Seq[CheckResult])
                      (implicit fs: FileSystem, sparkSes: SparkSession, settings: DQSettings): HdfsFile = {

    val reqVS: HdfsFile = vsRef.filter(vr => vr.id == vs).head
    val df: DataFrame = HdfsReader.load(reqVS).head

    val (colsToProcess: Array[String], colsToRemain: Array[String]) =
      keys match {
        case Some(ks) => (ks, df.columns.filterNot(ks.contains))
        case None     => (df.columns, Array.empty[String])
      }

    val headless: DataFrame = numOfColumns match {
      case Some(x) if x > colsToProcess.length =>
        val cols: Array[String] = colsToProcess
        val hlCols: Seq[String] =
          (0 until x).foldLeft(Seq.empty[String])((arr, i) =>
            arr ++ Seq(settings.backComp.keyFormatter(i + offset),
              settings.backComp.valueFormatter(i + offset)))

        val columnDF = cols.zipWithIndex.foldLeft(df) {
          case (curr, (col, i)) =>
            curr
              .withColumn(settings.backComp.keyFormatter(i + offset), lit(col))
              .withColumnRenamed(col, settings.backComp.valueFormatter(i + offset))
        }
        (cols.length until x)
          .foldLeft(columnDF) {
            case (curr, i) =>
              curr
                .withColumn(settings.backComp.keyFormatter(i + offset), lit(""))
                .withColumn(settings.backComp.valueFormatter(i + offset), lit(""))
          }
          .select((hlCols ++ colsToRemain).map(col): _*)
      case Some(x) if x <= colsToProcess.length =>
        val cols = colsToProcess.slice(0, x)
        val hlCols: Seq[String] =
          cols.indices.foldLeft(Seq.empty[String])((arr, i) =>
            arr ++ Seq(settings.backComp.keyFormatter(i + offset), settings.backComp.valueFormatter(i + offset)))
        cols.zipWithIndex
          .foldLeft(df) {
            case (curr, (col, i)) =>
              curr
                .withColumn(settings.backComp.keyFormatter(i + offset), lit(col))
                .withColumnRenamed(col, settings.backComp.valueFormatter(i + offset))
          }
          .select((hlCols ++ colsToRemain).map(col): _*)
      case None =>
        val cols = colsToProcess
        val hlCols: Seq[String] =
          cols.indices.foldLeft(Seq.empty[String])((arr, i) =>
            arr ++ Seq(settings.backComp.keyFormatter(i + offset), settings.backComp.valueFormatter(i + offset)))
        cols.zipWithIndex
          .foldLeft(df) {
            case (curr, (col, i)) =>
              curr
                .withColumn(settings.backComp.keyFormatter(i + offset), lit(col))
                .withColumnRenamed(col, settings.backComp.valueFormatter(i + offset))
          }
          .select((hlCols ++ colsToRemain).map(col): _*)
    }

    HdfsWriter.saveDF(target, headless)(sparkSes, fs, settings)

    new HdfsFile(target)
  }
}
