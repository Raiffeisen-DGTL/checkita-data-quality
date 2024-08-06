package org.checkita.dqf.writers

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Outputs._
import org.checkita.dqf.connections.DQConnection
import org.checkita.dqf.utils.ResultUtils._

import scala.util.Try

trait FileWriter[T <: SaveToFileConfig] extends OutputWriter[DataFrame, T] {

  /**
   * Writes result to required output channel given the output configuration.
   *
   * @param result Result to be written
   * @param target Output configuration
   * @param jobId Current Job ID
   * @return "Success" string in case of successful write operation or a list of errors.
   */
  override def write(target: T,
                     result: DataFrame)(implicit jobId: String,
                                        settings: AppSettings,
                                        spark: SparkSession,
                                        connections: Map[String, DQConnection]): Result[String] = Try {
    
    val targetDir = s"${jobId}_${targetType}_${settings.referenceDateTime.render}_${settings.executionDateTime.render}"
      .replaceAll("""\W""", "")
    val filePath = s"${target.save.path.value}/$targetDir"

    val dfWriter = result.repartition(settings.outputRepartition).write.mode(SaveMode.Overwrite)
    target.save match {
      case _: ParquetFileOutputConfig => dfWriter.parquet(filePath)
      case _: OrcFileOutputConfig => dfWriter.orc(filePath)
      case _: AvroFileOutputConfig => dfWriter.format("avro").save(filePath)
      case delimited: DelimitedFileOutputConfig =>
        dfWriter.format("csv")
          .option("header", delimited.header)
          .option("delimiter", delimited.delimiter.value)
          .option("quote", delimited.quote.value)
          .option("escape", delimited.escape.value)
          .option("nullValue", "")
          .option("quoteMode", "MINIMAL")
          .save(filePath)
    }
    "Success"
  }.toResult(
    preMsg = s"Unable to write file with '$targetType' output to folder '${target.save.path.value}' due to following error:"
  )
}
