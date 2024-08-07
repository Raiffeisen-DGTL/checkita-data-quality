package org.checkita.dqf.targets.builders

import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Targets.TargetConfig
import org.checkita.dqf.storage.Models.ResultSet
import org.checkita.dqf.utils.ResultUtils.Result

trait TargetBuilder[T <: TargetConfig, R] {
  
  /**
   * Build target output given the target configuration
   * @param target Target configuration
   * @param results All job results
   * @param settings Implicit application settings object
   * @param spark Implicit spark session object
   * @return Target result of required type.
   */
  def build(target: T, results: ResultSet)
           (implicit settings: AppSettings, spark: SparkSession): Result[R]
}
