package ru.raiffeisen.checkita.postprocessors

import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.checks.CheckResult
import ru.raiffeisen.checkita.metrics.MetricResult
import ru.raiffeisen.checkita.sources.HdfsFile
import ru.raiffeisen.checkita.utils.DQSettings

import scala.language.implicitConversions


object PostprocessorType extends Enumeration {
  val enrich: PostprocessorVal =
    PostprocessorVal("enrich", classOf[EnrichPostprocessor])
  val transpose: PostprocessorVal =
    PostprocessorVal("transpose_by_key", classOf[TransposePostprocessor])
  val headless: PostprocessorVal =
    PostprocessorVal("transpose_by_column", classOf[TransposeByColumnPostprocessor])
  val arrange: PostprocessorVal =
    PostprocessorVal("arrange", classOf[ArrangePostprocessor])

  protected case class PostprocessorVal(name: String,
                                        service: Class[_ <: BasicPostprocessor])
    extends super.Val() {
    override def toString(): String = this.name
  }
  implicit def convert(value: Value): PostprocessorVal =
    value.asInstanceOf[PostprocessorVal]
}

abstract class BasicPostprocessor(config: Config, settings: DQSettings){
  def process(vsRef: Set[HdfsFile], metRes: Seq[MetricResult], chkRes: Seq[CheckResult])
             (implicit fs: FileSystem, sparkSes: SparkSession, settings: DQSettings): HdfsFile
}
