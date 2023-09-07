package ru.raiffeisen.checkita.postprocessors

import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import ru.raiffeisen.checkita.checks.CheckResult
import ru.raiffeisen.checkita.metrics.MetricResult
import ru.raiffeisen.checkita.sources.HdfsFile
import ru.raiffeisen.checkita.targets.HdfsTargetConfig
import ru.raiffeisen.checkita.utils.io.{HdfsReader, HdfsWriter}
import ru.raiffeisen.checkita.utils.{DQSettings, parseTargetConfig}

import scala.collection.JavaConversions._

final class TransposePostprocessor(config: Config, settings: DQSettings)
  extends BasicPostprocessor(config, settings: DQSettings) {
  private val vs = config.getString("source")
  private val keys = config.getStringList("keyColumns")
  private val target: HdfsTargetConfig = {
    val conf = config.getConfig("saveTo")
    parseTargetConfig(conf)(settings).get
  }

  override def process(vsRef: Set[HdfsFile], metRes: Seq[MetricResult], chkRes: Seq[CheckResult])
                      (implicit fs: FileSystem, sparkSes: SparkSession, settings: DQSettings): HdfsFile = {

    import sparkSes.implicits._

    def toLong(df: DataFrame, by: Seq[String]): DataFrame = {
      val (cols, types) = df.dtypes.filter { case (c, _) => !by.contains(c) }.unzip
      require(types.distinct.length == 1)

      val kvs = explode(
        array(
          cols.map(c => struct(lit(c).alias(settings.backComp.trKeyName),
            col(c).alias(settings.backComp.trValueName))): _*
        ))

      val byExprs = by.map(col)

      df.select(byExprs :+ kvs.alias("_kvs"): _*)
        .select(byExprs ++ Seq($"_kvs.${settings.backComp.trKeyName}", $"_kvs.${settings.backComp.trValueName}"): _*)
    }

    val reqVS: HdfsFile = vsRef.filter(vr => vr.id == vs).head
    val df: DataFrame = HdfsReader.load(reqVS).head

    val transposed: DataFrame = toLong(df, keys)

    HdfsWriter.saveDF(target, transposed)(sparkSes, fs, settings)

    new HdfsFile(target)
  }

}
