package ru.raiffeisen.checkita.context

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.IO.readJobConfig
import ru.raiffeisen.checkita.config.Parsers._
import ru.raiffeisen.checkita.config.appconf.AppConfig
import ru.raiffeisen.checkita.config.jobconf.Checks.CheckConfig
import ru.raiffeisen.checkita.config.jobconf.Connections.ConnectionConfig
import ru.raiffeisen.checkita.config.jobconf.JobConfig
import ru.raiffeisen.checkita.config.jobconf.LoadChecks.LoadCheckConfig
import ru.raiffeisen.checkita.config.jobconf.Metrics.{ComposedMetricConfig, RegularMetricConfig}
import ru.raiffeisen.checkita.config.jobconf.Schemas.SchemaConfig
import ru.raiffeisen.checkita.config.jobconf.Sources.{SourceConfig, VirtualSourceConfig}
import ru.raiffeisen.checkita.config.jobconf.Targets.TargetConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.core.Source
import ru.raiffeisen.checkita.readers.ConnectionReaders._
import ru.raiffeisen.checkita.readers.SchemaReaders._
import ru.raiffeisen.checkita.readers.SourceReaders._
import ru.raiffeisen.checkita.readers.VirtualSourceReaders.VirtualSourceReaderOps
import ru.raiffeisen.checkita.storage.Connections.DqStorageConnection
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager
import ru.raiffeisen.checkita.utils.Common.prepareConfig
import ru.raiffeisen.checkita.utils.Logging
import ru.raiffeisen.checkita.utils.ResultUtils._
import ru.raiffeisen.checkita.utils.SparkUtils.{makeFileSystem, makeSparkSession}

import java.io.InputStreamReader
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Checkita Data Quality context.
 * The main purpose of this context is to unify Data Quality job building and running API.
 * Thus, various context builders are available depending on use case.
 * Having a valid data quality context, job can be build, again, using various builders depending on user needs.
 * @param settings Application settings object.
 * @param spark Spark session object.
 * @param fs Hadoop filesystem object.
 */
class DQContext(settings: AppSettings, spark: SparkSession, fs: FileSystem) extends Logging {

  implicit private val sparkSes: SparkSession = spark
  implicit private val fileSystem: FileSystem = fs
  implicit private val appSettings: AppSettings = settings
  
  private val settingsStage: String = RunStage.ApplicationSetup.entryName
  private val connStage: String = RunStage.EstablishConnections.entryName
  private val schemaStage: String = RunStage.ReadSchemas.entryName
  private val sourceStage: String = RunStage.ReadSources.entryName
  private val virtualSourceStage: String = RunStage.ReadVirtualSources.entryName

  /**
   * Trick here is that this value needs to be evaluated when DQ context is created.
   * Thus, evaluation of this value forces logger initialization and app settings logging.
   */
  private val _: Unit = {
    initLogger(settings.loggingLevel)
    
    val logGeneral = Seq(
      "General Checkita Data Quality configuration:", 
      s"* Logging level: ${settings.loggingLevel.toString}"
    )
    val logSparkConf = Seq(
      "* Spark configuration:",
      s"  - Spark Version:                   ${spark.version}",
      s"  - Application name:                ${spark.sparkContext.getConf.get("spark.app.name")}",
      s"  - Default Spark File System:       ${spark.sparkContext.hadoopConfiguration.get("fs.defaultFS")}",
      s"  - Running in local mode:           ${settings.isLocal}",
      s"  - Running in shared spark context: ${settings.isShared}"
    )
    val logTimePref = Seq(
      "* Time settings:",
      s"  - Local time zone:       ${settings.referenceDateTime.timeZone.toString}",
      s"  - Reference date format: ${settings.referenceDateTime.dateFormat.pattern}",
      s"  - Reference date value:  ${settings.referenceDateTime.render}",
      s"  - Execution date format: ${settings.executionDateTime.dateFormat.pattern}",
      s"  - Execution date value:  ${settings.executionDateTime.render}",
    )
    val logEnablers = Seq(
      "* Enablers settings:",
      s"  - allowNotifications:       ${settings.allowNotifications}",
      s"  - allowSqlQueries:          ${settings.allowSqlQueries}",
      s"  - aggregatedKafkaOutput:    ${settings.aggregatedKafkaOutput}",
      s"  - enableCaseSensitivity:    ${settings.enableCaseSensitivity}",
      s"  - errorDumpSize:            ${settings.errorDumpSize}",
      s"  - outputRepartition:        ${settings.outputRepartition}"   
    )
    val logStorageConf = settings.storageConfig match {
      case Some(conf) => Seq(
        "* Storage configuration:",
        s"  - Run DB migration:  ${settings.doMigration}",
        s"  - Data base type:    ${conf.dbType.toString}",
        s"  - Connection url:    ${conf.url.value}",
      ) ++ conf.username.map(
        u => Seq(s"  - Connect as user:   ${u.value} (password is not shown intentionally)")
      ).getOrElse(Seq.empty) ++ conf.schema.map(
        s => Seq(s"  - Connect to schema: ${s.value}")
      ).getOrElse(Seq.empty)
      case None => Seq(
        "* Storage configuration:",
        "  - Configuration is empty. Results will not be saved. " +
          "Also trend checks execution is impossible and will be skipped."
      )
    }
    val logEmailConfig = settings.emailConfig match {
      case Some(conf) => Seq(
        "* Email configuration:",
        s"  - SMTP host:             ${conf.host.value}",
        s"  - SMTP port:             ${conf.port.value}",
        s"  - Sender email address:  ${conf.address.value}",
        s"  - Sender name:           ${conf.name.value}",
        s"  - Enable SSL on connect: ${conf.sslOnConnect}",
        s"  - Enable TLS:            ${conf.tlsEnabled}"
      ) ++ conf.username.map(
        u => Seq(s"  - Connect as user:       ${u.value} (password is not shown intentionally)")
      ).getOrElse(Seq.empty)
      case None => Seq(
        "* Email configuration:",
        "  - Configuration is empty. Emails cannot be sent."
      )
    }
    val logMMConfig = settings.mattermostConfig match {
      case Some(conf) => Seq(
        "* Mattermost configuration:",
        s"  - API URL: ${conf.host.value} (API token is not shown intentionally"        
      )
      case None => Seq(
        "* Mattermost configuration:",
        "  - Configuration is empty. Notification to Mattermost cannot be sent."
      )
    }
    val extraVarsList = settings.prependVars.split("\n").map(_.split(":").head).mkString("[", ", ", "]")
    val logExtraVars = Seq(
      "* Extra variables:",
      s"  - List of all available variables (values are not shown intentionally): $extraVarsList"
    )
    
    (
      logGeneral ++ 
      logSparkConf ++ 
      logTimePref ++ 
      logEnablers ++ 
      logStorageConf ++ 
      logEmailConfig ++ 
      logMMConfig ++
      logExtraVars
    ).foreach(msg => log.info(s"$settingsStage $msg"))
  }

  private def logJobBuildStart(implicit jobId: String): Unit = {
    log.warn("************************************************************************")
    log.warn(s"                   Starting build of job '$jobId'")
    log.warn("************************************************************************")
  }
  
  /**
   * Stops this DQ context by stopping spark session if needed
   * @return Nothing
   */
  def stop(): Result[Unit] = Try {
    if (settings.isShared)
      log.info("Application was run within shared spark context and, therefore, spark session was not terminated.")
    else {
      spark.stop()
      log.info("Spark session was terminated.")
    }
  }.toResult(preMsg = "Unable to terminate spark session due to following error:")
  
  /**
   * Getting storage manager given the storage connection configuration.
   * Storage is an important part of the Checkita Framework. 
   * Apart from just storing results, it allows performing anomaly detection kind of checks
   * (in Checkita we call them `trend` checks). If storage manager is not configured then
   * both trend checks and results saving are disabled.
   * @note if storage connection configuration is provided than it is mandatory to have storage manager.
   *       Therefore, when storage manager cannot be build out of provided connection configuration
   *       we return Left with errors rather than just None.

   * @return Either an option with DQ storage manager or a list of manager initialization errors.
   */
  private def getStorageManager: Result[Option[DqStorageManager]] = settings.storageConfig match {
    case Some(config) => Try(DqStorageManager(DqStorageConnection(config))).toResult(
      preMsg = "Unable to connect to Data Quality storage due to following error:"
    ).map(Some(_))
    case None => liftToResult(None)
  }

  /**
   * Transform sequence of  tuple sequences wrapped into Result into Result of map.
   */
  private def reduceToMap[T](r: Seq[Result[Seq[(String, T)]]]): Result[Map[String, T]] = {
    if (r.isEmpty) liftToResult(Map.empty[String, T])
    else r.reduce((r1, r2) => r1.combine(r2)(_ ++ _)).mapValue(_.toMap)
  }

  /**
   * Reads all connections from configuration and tries to establish them.
   * @note Also executes logging side effects.
   * @param connections Sequence of connections configurations
   * @return Either map of valid connections (connectionId -> DQConnection) or a list of connection errors.
   */
  private def establishConnections(connections: Seq[ConnectionConfig]): Result[Map[String, DQConnection]] =
    reduceToMap(connections.map{ conn =>
      log.info(s"$connStage Establishing connection '${conn.id.value}'...")
      conn.read.mapValue(c => Seq(c.id -> c))
        .tap(_ => log.info(s"$connStage Success!")) // immediate logging of success state
        .mapLeft(_.map(e => s"$connStage $e")) // update error messages with running stage
    })
  /**
   * Safely reads all schemas from configuration.
   * @note Also executes logging side effects.
   * @param schemas Sequence of schemas configurations
   * @return Either a map of valid source schemas (schemaId -> SourceSchema) or a list of reading errors.
   */
  private def readSchemas(schemas: Seq[SchemaConfig]): Result[Map[String, SourceSchema]] =
    reduceToMap(schemas.map{ schema =>
      log.info(s"$schemaStage Reading schema '${schema.id.value}'...")
      schema.read.mapValue(s => Seq(s.id -> s))
        .tap(_ => log.info(s"$schemaStage Success!")) // immediate logging of success state
        .mapLeft(_.map(e => s"$schemaStage $e")) // update error messages with running stage
    })


  /**
   * Safely reads all regular sources from configuration
   * @note Also executes logging side effects.
   * @param sources Sequence of sources configurations
   * @param schemas Map of source schemas (schemaId -> SourceSchema)
   * @param connections Map of connections (connectionId -> DQConnection)
   * @return Either a map of regular sources (sourceId -> Source) or a list of reading errors.
   */
  private def readSources(sources: Seq[SourceConfig],
                          schemas: Map[String, SourceSchema],
                          connections: Map[String, DQConnection]): Result[Map[String, Source]] = {
    implicit val sc: Map[String, SourceSchema] = schemas
    implicit val c: Map[String, DQConnection] = connections
    
    reduceToMap(sources.map{ srcConf =>
      log.info(s"$sourceStage Reading source '${srcConf.id.value}'...")
      srcConf.read.mapValue(s => Seq(s.id -> s))
        .tap(_ => log.info(s"$sourceStage Success!")) // immediate logging of success state
        .mapLeft(_.map(e => s"$sourceStage $e")) // update error messages with running stage
    })
  }

  /**
   * Recursively reads all virtual sources, thus previously read virtual source can be used as a parent for next one.
   * @note Also executes logging side effects.
   * @param virtualSources Sequence of virtual sources configurations (defined in order they need to be processed)
   * @param parentSources Map of parent source (wrapped into Result in order to chain reading attempts)
   * @return Either a map of all (regular + virtual) sources (sourceId -> Source) or a list of reading errors.
   */
  @tailrec
  private def readVirtualSources(virtualSources: Seq[VirtualSourceConfig],
                                 parentSources: Result[Map[String, Source]]): Result[Map[String, Source]] =
    if (virtualSources.isEmpty) parentSources else {
      val newSource = parentSources.flatMap{ parents =>
        val curVsConfig = virtualSources.head
        log.info(s"$virtualSourceStage Reading virtual source '${curVsConfig.id.value}'...")
        val newSrc = curVsConfig.read(parents)
          .tap(_ => log.info(s"$virtualSourceStage Success!")) // immediate logging of success state
          .mapLeft(_.map(e => s"$virtualSourceStage $e")) // update error messages with running stage
        // if persist is required and newSrc reading was successful, then DO persist dataframe:
        curVsConfig.persist.foreach(sLvl => newSrc.foreach{ src =>
          log.info(s"$virtualSourceStage Persisting virtual source '${curVsConfig.id.value}' to ${sLvl.toString}.")
          src.df.persist(sLvl)
        })
        newSrc
      }
      readVirtualSources(
        virtualSources.tail, 
        parentSources.combine(newSource)((curSrc, newSrc) => curSrc + (newSrc.id -> newSrc))
      )
    }

  /**
   * Fundamental Data Quality Job Builder: builds job provided with all job components.
   * @param jobId Job ID
   * @param sources Sequence of sources to check
   * @param metrics Sequence of regular metrics to calculate
   * @param composedMetrics Sequence of composed metrics to calculate on top of regular metrics results.
   * @param checks Sequence of checks to preform based in metrics results.
   * @param loadChecks Sequence of load checks to perform directly over the sources (validate source metadata).
   * @param schemas Sequence of user-defined schemas used primarily to perform loadChecks (i.e. source schema validation).
   * @param targets Sequence of targets to be send/saved (alternative channels to communicate DQ Job results).
   * @param connections Sequence of user-defined connections to external data systems (RDBMS, Kafka, etc..).
   *                    Connections are used primarily to send targets.
   * @return Data Quality Job instances wrapped into Result[_].
   */
  def buildJob(
                jobId: String,
                sources: Seq[Source],
                metrics: Seq[RegularMetricConfig] = Seq.empty,
                composedMetrics: Seq[ComposedMetricConfig] = Seq.empty,
                checks: Seq[CheckConfig] = Seq.empty,
                loadChecks: Seq[LoadCheckConfig] = Seq.empty,
                targets: Seq[TargetConfig] = Seq.empty,
                schemas: Map[String, SourceSchema] = Map.empty,
                connections: Map[String, DQConnection] = Map.empty
              ): Result[DQJob] = {
    implicit val j: String = jobId
    
    logJobBuildStart
    
    getStorageManager.map(storageManager =>
      DQJob(sources, metrics, composedMetrics, checks, loadChecks, targets, schemas, connections, storageManager)
    )
  }

  /**
   * Builds Data Quality job provided with job configuration.
   * @param jobConfig Job configuration
   * @return Data Quality job instance or a list of building errors.
   * @note Data Quality job creation out of job configuration assumes following steps:
   *       - all configured connections are established;
   *       - all schemas defined in job configuration are read;
   *       - all sources (both regular and virtual ones) defined in job configuration are read;
   *       - storage manager is initialized (if configured).
   */
  def buildJob(jobConfig: JobConfig): Result[DQJob] = {
    implicit val jobId: String = jobConfig.jobId.value
    
    logJobBuildStart
    
    val connections = establishConnections(jobConfig.connections.map(_.getAllConnections).getOrElse(Seq.empty))
    val schemas = readSchemas(jobConfig.schemas)
    val regularSources = connections.combine(schemas)((c, s) => (c, s)).flatMap{
      case (conn, sch) => readSources(jobConfig.sources.map(_.getAllSources).getOrElse(Seq.empty), sch, conn)
    }
    val allSources = readVirtualSources(jobConfig.virtualSources, regularSources)
    
    val regularMetrics: Seq[RegularMetricConfig] = jobConfig.metrics.toSeq.flatMap(_.regular).flatMap(_.getAllRegularMetrics)
    val composedMetrics: Seq[ComposedMetricConfig] = jobConfig.metrics.toSeq.flatMap(_.composed)
    val checks: Seq[CheckConfig] = jobConfig.checks.toSeq.flatMap(_.getAllChecks)
    val loadChecks: Seq[LoadCheckConfig] = jobConfig.loadChecks.toSeq.flatMap(_.getAllLoadChecks)
    val targets: Seq[TargetConfig] = jobConfig.targets.toSeq.flatMap(_.getAllTargets)
    
    allSources.combineT3(connections, schemas, getStorageManager)(
      (sources, conn, sch, manager) => DQJob(
        sources.values.toSeq,
        regularMetrics,
        composedMetrics,
        checks,
        loadChecks,
        targets,
        sch,
        conn,
        manager
      )
    ).mapLeft(_.distinct) // there is some error message duplication that needs to be eliminated.
  }

  /**
   * Build Data Quality job provided with sequence if paths to job configuration HOCON files.
   * @param jobConfigs Path to a job-level configuration file (HOCON)
   * @return Data Quality job instance or a list of building errors.
   * @note HOCON format support configuration merging. Thus, it is also allowed to define different parts of
   *       job configuration in different files and merge them prior parsing. This will allow, for example,
   *       not to duplicate sections with common connections configurations or other sections
   *       that are common among several jobs.
   */
  def buildJob(jobConfigs: Seq[String]): Result[DQJob] =
    prepareConfig(jobConfigs, settings.prependVars, "job")
      .flatMap(readJobConfig[InputStreamReader]).flatMap(buildJob)
  
}

object DQContext {
  
  /**
   * Builds default Data Quality context using default application settings.
   * @return Default DQ Context
   */
  def default: Result[DQContext] = build(AppSettings())

  /**
   * Builds Checkita Data Quality context provided only with application settings.
   * @param settings Application settings object
   * @return Either Data Quality context or a list of building errors.
   * @note This builder will create new spark session object or get one from shared spark context.
   *       If it is required to use already existing variable pointing to a spark session,
   *       then use other builder that have explicit argument to pass spark session object.
   */
  def build(settings: AppSettings): Result[DQContext] = for {
    spark <- makeSparkSession(settings.sparkConf, settings.applicationName)
    fs <- makeFileSystem(spark)
  } yield new DQContext(settings, spark, fs)

  /**
   * Builds Checkita Data Quality context provided with application settings and existing spark session.
   * @param settings Application settings object
   * @param spark Spark session object
   * @return Either Data Quality context or a list of building errors.
   */
  def build(settings: AppSettings, spark: SparkSession): Result[DQContext] =
    makeFileSystem(spark).map(fs => new DQContext(settings, spark, fs))

  /**
   * Builds Checkita Data Quality context provided with application-level configuration 
   * and other required initialization parameters.
   * @param appConfig Application-level configuration
   * @param referenceDate Reference date string
   * @param isLocal Boolean flag indicating whether spark application must be run locally.
   * @param isShared Boolean flag indicating whether spark application running within shared spark context.
   * @param doMigration Boolean flag indication whether DQ storage database migration needs to be run prior result saving.
   * @param prependVariables Collected variables that will be prepended to job configuration file
   *                         (if one will be provided). These variables should already be transformed to a multiline HOCON string. 
   * @param logLvl Application logging level.
   * @return Either Data Quality context or a list of building errors.
   */
  def build(appConfig: AppConfig,
            referenceDate: Option[String],
            isLocal: Boolean,
            isShared: Boolean,
            doMigration: Boolean,
            prependVariables: String,
            logLvl: Level): Result[DQContext] =
    AppSettings.build(appConfig, referenceDate, isLocal, isShared, doMigration, prependVariables, logLvl)
      .flatMap(settings => build(settings))

  /**
   * Builds Checkita Data Quality context provided with path to an application-level configuration file 
   * and other required initialization parameters.
   * @param appConfig Path to an application-level configuration file (HOCON)
   * @param referenceDate Reference date string
   * @param isLocal Boolean flag indicating whether spark application must be run locally.
   * @param isShared Boolean flag indicating whether spark application running within shared spark context.
   * @param doMigration Boolean flag indication whether DQ storage database migration needs to be run prior result saving.
   * @param extraVariables User-defined map of extra variables to be prepended to both application-level and job-level
   *                       configuration files prior their parsing.
   * @param logLvl Application logging level
   * @return Either Data Quality context or a list of building errors.
   * @note This builder also collects system and java-runtime environment variables and looks for variables related to
   *       Checkita Data Quality Framework: following regex is used to find such variables: `^(?i)(DQ)[a-z0-9_-]+$`.
   *       These variables are combined with the ones provided in `extraVariables` argument and further used during
   *       configuration files parsing.
   */
  def build(appConfig: String, 
            referenceDate: Option[String], 
            isLocal: Boolean = false,
            isShared: Boolean = false,
            doMigration: Boolean = false,
            extraVariables: Map[String, String] = Map.empty, // no limitations on variables names.
            logLvl: Level = Level.INFO): Result[DQContext] = {
    val variablesRegex = "^(?i)(DQ)[a-z0-9_-]+$" // system variables must match the given regex.
    val systemVariables = System.getProperties.asScala.filterKeys(_ matches variablesRegex)
    val prependVariables = (systemVariables ++ extraVariables).map {
      case (k, v) => k + ": \"" + v + "\""
      }.mkString("", "\n", "\n")
    AppSettings.build(appConfig, referenceDate, isLocal, isShared, doMigration, prependVariables, logLvl)
      .flatMap(settings => build(settings))
  }
  
}
