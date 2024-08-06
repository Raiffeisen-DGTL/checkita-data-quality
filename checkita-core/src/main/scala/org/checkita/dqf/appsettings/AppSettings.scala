package org.checkita.dqf.appsettings

import org.apache.logging.log4j.Level
import org.apache.spark.SparkConf
import org.checkita.dqf.config.Enums.MetricEngineAPI
import org.checkita.dqf.config.IO.readAppConfig
import org.checkita.dqf.config.Parsers._
import org.checkita.dqf.config.appconf._
import org.checkita.dqf.utils.Common.{paramsSeqToMap, prepareConfig}
import org.checkita.dqf.utils.EnrichedDT
import org.checkita.dqf.utils.ResultUtils._

import java.io.InputStreamReader
import scala.util.Try

/**
 * Application settings
 *
 * @param executionDateTime     Job execution date-time (actual time when job is started)
 * @param referenceDateTime     Reference date-time (for which the job is performed)
 * @param allowNotifications    Enables notifications to be sent from DQ application
 * @param allowSqlQueries       Enables SQL arbitrary queries in virtual sources
 * @param aggregatedKafkaOutput Enables sending aggregates messages for Kafka Targets
 *                              (one per each target type, except checkAlerts where
 *                              one message per checkAlert will be sent)
 * @param enableCaseSensitivity Enables columns case sensitivity
 * @param errorDumpSize         Maximum number of errors to be collected per single metric.
 * @param outputRepartition     Sets the number of partitions when writing outputs. By default writes single file.
 * @param metricEngineAPI       Metric processor API used to process metrics: either Spark RDD or Spark DF.
 * @param storageConfig         Configuration of connection to Data Quality Storage
 * @param emailConfig           Configuration of connection to SMTP server
 * @param mattermostConfig      Configuration of connection to Mattermost API
 * @param streamConfig          Streaming settings (used in streaming applications only)
 * @param encryption            Encryption settings
 * @param sparkConf             Spark configuration parameters
 * @param isLocal               Boolean flag indicating whether spark application must be run locally.
 * @param isShared              Boolean flag indicating whether spark application running within shared spark context.
 * @param doMigration           Boolean flag indication whether DQ storage database migration needs to be run prior result saving.
 * @param applicationName       Name of Checkita Data Quality spark application
 * @param prependVars           Multiline HOCON string with variables to be prepended to configuration files during their parsing.
 * @param loggingLevel          Application logging level
 * @param versionInfo           Information about application and configuration API versions.
 */
final case class AppSettings(
                              executionDateTime: EnrichedDT,
                              referenceDateTime: EnrichedDT,
                              allowNotifications: Boolean,
                              allowSqlQueries: Boolean,
                              aggregatedKafkaOutput: Boolean,
                              enableCaseSensitivity: Boolean,
                              errorDumpSize: Int,
                              outputRepartition: Int,
                              metricEngineAPI: MetricEngineAPI,
                              storageConfig: Option[StorageConfig],
                              emailConfig: Option[EmailConfig],
                              mattermostConfig: Option[MattermostConfig],
                              streamConfig: StreamConfig,
                              encryption: Option[Encryption],
                              sparkConf: SparkConf,
                              isLocal: Boolean,
                              isShared: Boolean,
                              doMigration: Boolean,
                              applicationName: Option[String],
                              prependVars: String,
                              loggingLevel: Level,
                              versionInfo: VersionInfo
                            )

object AppSettings {

  /**
   * Builds application settings object provided with application-level configuration and other required initialization parameters.
   * The whole purpose of this method is to provide additional validation while initializing the application-level settings:
   *   - catch possible errors when parsing execution and reference datetime;
   *   - catch possible errors when setting up spark configuration.
   *
   * @param appConfig        Application-level configuration
   * @param referenceDate    Reference date string
   * @param isLocal          Boolean flag indicating whether spark application must be run locally.
   * @param isShared         Boolean flag indicating whether spark application running within shared spark context.
   * @param doMigration      Boolean flag indication whether DQ storage database migration needs to be run prior result saving.
   * @param prependVariables Collected variables that will be prepended to job configuration file
   *                         (if one will be provided). These variables should already be transformed to a multiline HOCON string. 
   * @param logLvl           Application logging level.
   * @param versionInfo      Information about application and configuration API versions.
   * @return Either application settings object or a list of building errors.
   */
  def build(appConfig: AppConfig,
            referenceDate: Option[String],
            isLocal: Boolean,
            isShared: Boolean,
            doMigration: Boolean,
            prependVariables: String,
            logLvl: Level,
            versionInfo: VersionInfo): Result[AppSettings] = {
      
    val execDateTime = Try(EnrichedDT(
      appConfig.dateTimeOptions.executionDateFormat, appConfig.dateTimeOptions.timeZone
    )).toResult(preMsg = "Cannot parse execution datetime with following error:")
    val refDateTime = Try(EnrichedDT(
      appConfig.dateTimeOptions.referenceDateFormat, appConfig.dateTimeOptions.timeZone, referenceDate
    )).toResult(preMsg =  "Cannot parse reference datetime with following error:")
    
    val sparkConf = Try {
      // spark configuration with required defaults:
      val conf = new SparkConf()
        .set("spark.sql.parquet.compression.codec", "snappy")
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .set("spark.sql.orc.enabled", "true")
        .set("spark.sql.hive.convertMetastoreOrc", "true")
        .set("spark.sql.files.ignoreMissingFiles", "true")
        .set("spark.hadoop.hive.exec.dynamic.partition", "true")
        .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.hadoop.hive.vectorized.execution.enabled", "true")

      paramsSeqToMap(appConfig.defaultSparkOptions.map(_.value)).foreach{
        case (k, v) => conf.set(k, v) 
      }
      if (isLocal) conf.setMaster("local[*]") else conf
    }.toResult(preMsg = "Cannot setup spark configuration with following error:")
    
    // combine validated reads and create an instance of DQSettings:
    sparkConf.combineT2(execDateTime, refDateTime){ (conf, execDT, refDT) =>
      val prependVars = prependVariables + Seq(
        "referenceDate: \"" + refDT.render + "\"",
        "executionDate: \"" + execDT.render + "\""
      ).mkString("", "\n", "\n")
      AppSettings(
        execDT,
        refDT,
        appConfig.enablers.allowNotifications,
        appConfig.enablers.allowSqlQueries,
        appConfig.enablers.aggregatedKafkaOutput,
        appConfig.enablers.enableCaseSensitivity,
        appConfig.enablers.errorDumpSize.value,
        appConfig.enablers.outputRepartition.value,
        appConfig.enablers.metricEngineAPI,
        appConfig.storage,
        appConfig.email,
        appConfig.mattermost,
        appConfig.streaming,
        appConfig.encryption,
        conf,
        isLocal,
        isShared,
        doMigration,
        appConfig.applicationName.map(_.value),
        prependVars,
        logLvl,
        versionInfo
      )
    }
  }

  /**
   * Build application settings object provided with path to an application configuration HOCON file as well as
   * other required initialization parameters.
   *
   * @param appConfig        Path to an application-level configuration file (HOCON)
   * @param referenceDate    Reference date string
   * @param isLocal          Boolean flag indicating whether spark application must be run locally.
   * @param isShared         Boolean flag indicating whether spark application running within shared spark context.
   * @param doMigration      Boolean flag indication whether DQ storage database migration needs to be run prior result saving.
   * @param prependVariables Collected variables that will be prepended to job configuration file
   *                         (if one will be provided). These variables should already be transformed to a multiline HOCON string. 
   * @param logLvl           Application logging level.
   * @return Either application settings object or a list of building errors.
   */
  def build(appConfig: String,
            referenceDate: Option[String],
            isLocal: Boolean,
            isShared: Boolean,
            doMigration: Boolean,
            prependVariables: String,
            logLvl: Level): Result[AppSettings] = for {
      prependVars <- prepareConfig(Seq(appConfig), prependVariables, "application")
      config <- readAppConfig[InputStreamReader](prependVars)
      versionInfo <- VersionInfo.loadVersion
      settings <- build(config, referenceDate, isLocal, isShared, doMigration, prependVariables, logLvl, versionInfo)
    } yield settings

  /**
   * Build default application settings object.
   * Used in cases when application configuration and other initialization
   * parameters are not provided
   * @return Application settings object
   */
  def apply(): AppSettings = {
    val defaultAppConf = AppConfig(None, None, None, None, None)
    val execDateTime = EnrichedDT(
      defaultAppConf.dateTimeOptions.executionDateFormat, defaultAppConf.dateTimeOptions.timeZone
    )
    val refDateTime = EnrichedDT(
      defaultAppConf.dateTimeOptions.executionDateFormat, defaultAppConf.dateTimeOptions.timeZone
    )
    val prependVars = Seq(
      "referenceDate: \"" + refDateTime.render + "\"",
      "executionDate: \"" + execDateTime.render + "\""
    ).mkString("", "\n", "\n")

    val versionInfo = VersionInfo.loadVersion.getOrElse(VersionInfo.unknown)

    AppSettings(
      execDateTime,
      refDateTime,
      defaultAppConf.enablers.allowNotifications,
      defaultAppConf.enablers.allowSqlQueries,
      defaultAppConf.enablers.aggregatedKafkaOutput,
      defaultAppConf.enablers.enableCaseSensitivity,
      defaultAppConf.enablers.errorDumpSize.value,
      defaultAppConf.enablers.outputRepartition.value,
      defaultAppConf.enablers.metricEngineAPI,
      defaultAppConf.storage,
      defaultAppConf.email,
      defaultAppConf.mattermost,
      defaultAppConf.streaming,
      defaultAppConf.encryption,
      new SparkConf(),
      isLocal = false,
      isShared = false,
      doMigration = false,
      None,
      prependVars,
      Level.INFO,
      versionInfo
    )
  }
}
