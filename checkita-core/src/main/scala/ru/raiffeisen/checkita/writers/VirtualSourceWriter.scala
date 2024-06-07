package ru.raiffeisen.checkita.writers

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Outputs.{FileOutputConfig, SaveToFileConfig}
import ru.raiffeisen.checkita.config.jobconf.Sources.VirtualSourceConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.utils.ResultUtils._

object VirtualSourceWriter {

  /** Provide writer with empty connections map as they are not required to write output to a file. */
  implicit val connections: Map[String, DQConnection] = Map.empty

  /**
   * Target configuration for virtual source: enables virtual source writing
   * @param save Virtual source save configuration
   */
  private case class VSTarget(save: FileOutputConfig) extends SaveToFileConfig

  /**
   * Virtual source writer which requires VSTarget as an input.
   */
  private object VsWriter extends FileWriter[VSTarget] { protected val targetType: String = "virtualSource" }

  /**
   * Safely saves virtual source if save configuration is provided.
   * @param vs Virtual source configuration
   * @param df Virtual source dataframe
   * @param jobId Current job ID
   * @return "Success" string in case of successful write operation or a list of errors.
   */
  def saveVirtualSource[T <: VirtualSourceConfig](vs: T,
                                                  df: DataFrame)(implicit jobId: String,
                                                                 settings: AppSettings,
                                                                 spark: SparkSession): Result[String] =
    if (vs.save.isEmpty) liftToResult("Nothing to save") else {
      VsWriter.write(VSTarget(vs.save.get), df)
    }
}
