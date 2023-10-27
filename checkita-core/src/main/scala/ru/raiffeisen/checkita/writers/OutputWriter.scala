package ru.raiffeisen.checkita.writers

import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Outputs.OutputConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.utils.ResultUtils.Result

trait OutputWriter[R, T <: OutputConfig] {
  protected val targetType: String
  
  /**
   * Writes result to required output channel given the output configuration.
   * @param result Result to be written
   * @param target Output configuration
   * @return "Success" string in case of successful write operation or a list of errors.
   */
  def write(target: T, result: R)(implicit jobId: String,
                                  settings: AppSettings,
                                  spark: SparkSession,
                                  connections: Map[String, DQConnection]): Result[String]
}
