package org.checkita.dqf.targets.builders.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Targets.ErrorCollTargetConfig
import org.checkita.dqf.storage.Models.ResultSet
import org.checkita.dqf.storage.Serialization.ResultsSerializationOps
import org.checkita.dqf.targets.builders.{BuildHelpers, TargetBuilder}
import org.checkita.dqf.utils.ResultUtils._

import scala.jdk.CollectionConverters._
import scala.util.Try

trait ErrorDataFrameBuilder[T <: ErrorCollTargetConfig] extends TargetBuilder[T, DataFrame] with BuildHelpers {

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
    spark.createDataFrame(
      filterErrors(
        results.metricErrors,
        target.metrics.map(_.value),
        target.dumpSize.map(_.value).getOrElse(settings.errorDumpSize)
      ).map(_.toRow).asJava,
      schema = ResultsSerializationOps.unifiedSchema
    )
  }.toResult(
    preMsg = s"Unable to prepare dataframe with metric errors due to following error:"
  )
}
