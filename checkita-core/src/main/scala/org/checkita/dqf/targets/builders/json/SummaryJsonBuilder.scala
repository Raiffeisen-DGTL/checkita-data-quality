package org.checkita.dqf.targets.builders.json

import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Targets.SummaryTargetConfig
import org.checkita.dqf.storage.Models.ResultSet
import org.checkita.dqf.storage.Serialization._
import org.checkita.dqf.targets.builders.TargetBuilder
import org.checkita.dqf.utils.ResultUtils._

import scala.util.Try

trait SummaryJsonBuilder[T <: SummaryTargetConfig] extends TargetBuilder[T, Seq[String]] {

  /**
   * Build target output given the target configuration
   *
   * @param target   Target configuration
   * @param results  All job results
   * @param settings Implicit application settings object
   * @param spark    Implicit spark session object
   * @return Target result of required type.
   */
  def build(target: T, results: ResultSet)
           (implicit settings: AppSettings, spark: SparkSession): Result[Seq[String]] =
    Try(Seq(results.summaryMetrics.toJson)).toResult(
      preMsg = s"Unable to prepare json data with summary metrics due to following error:"
    )
}
