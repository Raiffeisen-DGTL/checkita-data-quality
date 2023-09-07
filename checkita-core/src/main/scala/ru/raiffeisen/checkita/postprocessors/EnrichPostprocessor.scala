package ru.raiffeisen.checkita.postprocessors

import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import ru.raiffeisen.checkita.checks.CheckResult
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.metrics.MetricResult
import ru.raiffeisen.checkita.sources.HdfsFile
import ru.raiffeisen.checkita.targets.HdfsTargetConfig
import ru.raiffeisen.checkita.utils.io.{HdfsReader, HdfsWriter}
import ru.raiffeisen.checkita.utils.{DQSettings, parseTargetConfig}

import java.util
import scala.util.Try
import scala.collection.JavaConversions._

final class EnrichPostprocessor(config: Config, settings: DQSettings)
  extends BasicPostprocessor(config, settings) {

  private val vs: Option[String] = Try(config.getString("source")).toOption
  private val metrics: util.List[String] = config.getStringList("metrics")
  private val checks: util.List[String] = config.getStringList("checks")
  private val extra = config.getObject("extra").toMap

  private val target: HdfsTargetConfig = {
    val conf = config.getConfig("saveTo")
    parseTargetConfig(conf)(settings).get
  }

  override def process(vsRef: Set[HdfsFile], metRes: Seq[MetricResult], chkRes: Seq[CheckResult])
                      (implicit fs: FileSystem, sparkSes: SparkSession, settings: DQSettings): HdfsFile = {

    import sparkSes.implicits._

    val df: DataFrame = vs match {
      case Some(vsource) =>
        val reqVS: HdfsFile = vsRef.filter(vr => vr.id == vsource).head
        HdfsReader.load(reqVS).head
      case None =>
        sparkSes.sparkContext.parallelize(Seq(1)).toDF("teapot")
    }

    val reqMet: Seq[(String, Double)] = metRes
      .filter(mr => metrics.contains(mr.metricId))
      .map(mr => mr.metricId -> mr.result)
    val reqCheck: Seq[(String, String)] = chkRes
      .filter(cr => checks.contains(cr.checkId))
      .map(cr => cr.checkId -> cr.status)

    if (reqMet.size != metrics.size())
      throw IllegalParameterException("Some of stated metrics are missing!")
    if (reqCheck.size != checks.size())
      throw IllegalParameterException("Some of stated checks are missing!")

    val dfWithMet: DataFrame =
      reqMet.foldLeft(df)((df, met) => df.withColumn(met._1, lit(met._2)))
    val dfWithChecks = reqCheck.foldLeft(dfWithMet)((df, met) =>
      df.withColumn(met._1, lit(met._2)))
    val dfWithExtra = extra.foldLeft(dfWithChecks)((df, ex) =>
      df.withColumn(ex._1, lit(ex._2.unwrapped())))

    HdfsWriter.saveDF(target,dfWithExtra.drop("teapot"))(sparkSes, fs, settings)

    new HdfsFile(target)
  }
}
