package ru.raiffeisen.checkita.targets.builders.json

import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Targets.SummaryTargetConfig
import ru.raiffeisen.checkita.storage.Models.ResultSet
import ru.raiffeisen.checkita.storage.Serialization._
import ru.raiffeisen.checkita.targets.builders.TargetBuilder
import ru.raiffeisen.checkita.utils.ResultUtils._

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
