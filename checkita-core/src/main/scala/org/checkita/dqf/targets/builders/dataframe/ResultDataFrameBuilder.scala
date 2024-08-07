package org.checkita.dqf.targets.builders.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.Enums.ResultTargetType
import org.checkita.dqf.config.jobconf.Targets.ResultTargetConfig
import org.checkita.dqf.storage.Models.ResultSet
import org.checkita.dqf.storage.Serialization.ResultsSerializationOps
import org.checkita.dqf.targets.builders.TargetBuilder
import org.checkita.dqf.utils.ResultUtils._

import scala.jdk.CollectionConverters._
import scala.util.Try

trait ResultDataFrameBuilder[T <: ResultTargetConfig] extends TargetBuilder[T, DataFrame] {

  /**
   * Build target output given the target configuration
   *
   * @param target   Target configuration
   * @param results  All job results
   * @param settings Implicit application settings object
   * @param spark    Implicit spark session object
   * @return Target result in form of Spark DataFrame
   */
  override def build(target: T, results: ResultSet)
                    (implicit settings: AppSettings, spark: SparkSession): Result[DataFrame] = Try {
    val schema = ResultsSerializationOps.unifiedSchema
    val rows = target.resultTypes.value.flatMap {
      case ResultTargetType.RegularMetrics => results.regularMetrics.map(_.toRow)
      case ResultTargetType.ComposedMetrics => results.composedMetrics.map(_.toRow)
      case ResultTargetType.TrendMetrics => results.trendMetrics.map(_.toRow)
      case ResultTargetType.LoadChecks => results.loadChecks.map(_.toRow)
      case ResultTargetType.Checks => results.checks.map(_.toRow)
      case ResultTargetType.JobState => Seq(results.jobConfig.toRow)
    }
    spark.createDataFrame(rows.asJava, schema = schema)
  }.toResult(
    preMsg = s"Unable to prepare dataframe with result targets due to following error:"
  )
}
