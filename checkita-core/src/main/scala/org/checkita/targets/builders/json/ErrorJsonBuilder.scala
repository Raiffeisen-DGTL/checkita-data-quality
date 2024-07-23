package org.checkita.targets.builders.json

import org.apache.spark.sql.SparkSession
import org.checkita.appsettings.AppSettings
import org.checkita.config.jobconf.Targets.ErrorCollTargetConfig
import org.checkita.storage.Models.ResultSet
import org.checkita.storage.Serialization._
import org.checkita.targets.builders.{BuildHelpers, TargetBuilder}
import org.checkita.utils.ResultUtils._

import scala.util.Try

trait ErrorJsonBuilder[T <: ErrorCollTargetConfig] extends TargetBuilder[T, Seq[String]] with BuildHelpers {

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
                    (implicit settings: AppSettings, spark: SparkSession): Result[Seq[String]] = Try(
    filterErrors(
      results.metricErrors,
      target.metrics.map(_.value),
      target.dumpSize.map(_.value).getOrElse(settings.errorDumpSize)
    ).map(_.toJson)
  ).toResult(
    preMsg = s"Unable to prepare json data with result targets due to following error:"
  )
}
