package org.checkita.targets.builders.json

import org.apache.spark.sql.SparkSession
import org.checkita.appsettings.AppSettings
import org.checkita.config.Enums.ResultTargetType
import org.checkita.config.jobconf.Targets.ResultTargetConfig
import org.checkita.storage.Models.ResultSet
import org.checkita.storage.Serialization._
import org.checkita.targets.builders.TargetBuilder
import org.checkita.utils.ResultUtils._

import scala.util.Try

trait ResultJsonBuilder[T <: ResultTargetConfig] extends TargetBuilder[T, Seq[String]] {

  /**
   * Build target output given the target configuration
   *
   * @param target   Target configuration
   * @param results  All job results
   * @param settings Implicit application settings object
   * @param spark    Implicit spark session object
   * @return Target result in form of sequence of JSON strings.
   */
  override def build(target: T, results: ResultSet)
                    (implicit settings: AppSettings, spark: SparkSession): Result[Seq[String]] = Try {
    target.resultTypes.value.flatMap {
      case ResultTargetType.RegularMetrics => results.regularMetrics.map(_.toJson)
      case ResultTargetType.ComposedMetrics => results.composedMetrics.map(_.toJson)
      case ResultTargetType.TrendMetrics => results.trendMetrics.map(_.toJson)
      case ResultTargetType.LoadChecks => results.loadChecks.map(_.toJson)
      case ResultTargetType.Checks => results.checks.map(_.toJson)
      case ResultTargetType.JobState => Seq(results.jobConfig.toJson)
    }
  }.toResult(
    preMsg = s"Unable to prepare json data with result targets due to following error:"
  )
}
