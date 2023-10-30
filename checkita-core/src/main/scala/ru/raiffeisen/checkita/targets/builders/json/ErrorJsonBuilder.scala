package ru.raiffeisen.checkita.targets.builders.json

import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Targets.ErrorCollTargetConfig
import ru.raiffeisen.checkita.storage.Models.ResultSet
import ru.raiffeisen.checkita.storage.Serialization._
import ru.raiffeisen.checkita.targets.builders.{BuildHelpers, TargetBuilder}
import ru.raiffeisen.checkita.utils.ResultUtils._

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
    filterErrors(results.metricErrors, target.metrics.map(_.value), target.dumpSize.value).map(_.toJson)
  ).toResult(
    preMsg = s"Unable to prepare json data with result targets due to following error:"
  )
}
