package org.checkita.writers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.checkita.appsettings.AppSettings
import org.checkita.config.jobconf.Outputs.{FileOutputConfig, SaveToFileConfig}
import org.checkita.config.jobconf.Sources.VirtualSourceConfig
import org.checkita.connections.DQConnection
import org.checkita.utils.ResultUtils._

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
  private class VsWriter(vsId: String) extends FileWriter[VSTarget] { protected val targetType: String = vsId }

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
      new VsWriter(vs.id.value).write(VSTarget(vs.save.get), df)
    }
}
