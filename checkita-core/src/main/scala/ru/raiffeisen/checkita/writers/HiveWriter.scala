package ru.raiffeisen.checkita.writers

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Outputs.HiveOutputConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.storage.Serialization.ResultsSerializationOps
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.util.Try

trait HiveWriter[T <: HiveOutputConfig] extends OutputWriter[DataFrame, T] {

  /**
   * Writes result to required output channel given the output configuration.
   *
   * @param result Result to be written
   * @param target Output configuration
   * @return "Success" string in case of successful write operation or a list of errors.
   */
  override def write(target: T,
                     result: DataFrame)(implicit jobId: String,
                                        settings: AppSettings,
                                        spark: SparkSession,
                                        connections: Map[String, DQConnection]): Result[String] = Try {
    val tableName = target.schema.value + "." + target.table.value
    val targetSchema = spark.read.table(tableName).schema
    
    require(
      ResultsSerializationOps.unifiedSchema.zip(targetSchema).forall {
        case (col1, col2) => col1.name == col2.name && col1.dataType == col2.dataType
      }, s"Schema of hive table '$tableName' does not match to unified schema of '$targetType' output." 
    )
    
    result.repartition(settings.outputRepartition)
      .write.mode(SaveMode.Append).format("hive")
      .insertInto(tableName)
    "Success"
  }.toResult(
    preMsg = s"Unable to write '$targetType' output to hive table " +
      s"'${target.schema.value}.${target.table.value}' due to following error:"
  )
}
