package org.checkita.context

import org.apache.hadoop.fs.FileSystem
import org.apache.logging.log4j.Level
import org.apache.spark.sql.SparkSession
import org.checkita.appsettings.{AppSettings, VersionInfo}
import org.checkita.config.IO.readJobConfig
import org.checkita.config.Parsers._
import org.checkita.config.appconf.AppConfig
import org.checkita.config.jobconf.Checks.CheckConfig
import org.checkita.config.jobconf.Connections.ConnectionConfig
import org.checkita.config.jobconf.JobConfig
import org.checkita.config.jobconf.LoadChecks.LoadCheckConfig
import org.checkita.config.jobconf.Metrics.{ComposedMetricConfig, RegularMetricConfig, TrendMetricConfig}
import org.checkita.config.jobconf.Schemas.SchemaConfig
import org.checkita.config.jobconf.Sources.{SourceConfig, VirtualSourceConfig}
import org.checkita.config.jobconf.Targets.TargetConfig
import org.checkita.connections.DQConnection
import org.checkita.core.Source
import org.checkita.core.streaming.CheckpointIO
import org.checkita.core.streaming.Checkpoints.Checkpoint
import org.checkita.readers.ConnectionReaders._
import org.checkita.readers.SchemaReaders._
import org.checkita.readers.SourceReaders._
import org.checkita.readers.VirtualSourceReaders.VirtualSourceReaderOps
import org.checkita.storage.Connections.DqStorageConnection
import org.checkita.storage.Managers.DqStorageManager
import org.checkita.utils.Common.{getPrependVars, prepareConfig}
import org.checkita.utils.Logging
import org.checkita.utils.ResultUtils._
import org.checkita.utils.SparkUtils.{makeFileSystem, makeSparkSession}
import org.checkita.writers.VirtualSourceWriter.saveVirtualSource

import java.io.InputStreamReader
import scala.annotation.tailrec
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
  private val checkpointStage: String = RunStage.CheckpointStage.entryName

  /**
   * Trick here is that this value needs to be evaluated when DQ context is created.
   * Thus, evaluation of this value forces logger initialization and app settings logging.
   */
  locally {
    initLogger(settings.loggingLevel)
    val logGeneral = Seq(
      "General Checkita Data Quality configuration:",
      s"* Checkita application version:  ${settings.versionInfo.appVersion}",
      s"* Job Configuration API version: ${settings.versionInfo.configAPIVersion}",
      s"* Logging level:                 ${settings.loggingLevel.toString}",
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
    val logStreamingPref = Seq(
      "* Streaming settings:",
      s"  - Window duration:     ${settings.streamConfig.window.toString}",
      s"  - Trigger interval:    ${settings.streamConfig.trigger.toString}",
      s"  - Watermark interval:  ${settings.streamConfig.watermark.toString}",
      s"  - Allow empty windows: ${settings.streamConfig.allowEmptyWindows}",
      s"  - Checkpoint location: ${settings.streamConfig.checkpointDir.map(_.value).getOrElse("Not specified.")}"
    )
    val logEnablers = Seq(
      "* Enablers settings:",
      s"  - allowNotifications:       ${settings.allowNotifications}",
      s"  - allowSqlQueries:          ${settings.allowSqlQueries}",
      s"  - aggregatedKafkaOutput:    ${settings.aggregatedKafkaOutput}",
      s"  - enableCaseSensitivity:    ${settings.enableCaseSensitivity}",
      s"  - errorDumpSize:            ${settings.errorDumpSize}",
      s"  - outputRepartition:        ${settings.outputRepartition}",
      s"  - metricEngineAPI:          ${settings.metricEngineAPI.entryName}"
    )
    val logStorageConf = settings.storageConfig match {
      case Some(conf) => Seq(
        "* Storage configuration:",
        s"  - Run DB migration:  ${settings.doMigration}",
        s"  - Save Errors to DB: ${conf.saveErrorsToStorage}",
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
          "Also trend metrics and trend checks execution is impossible and will be skipped."
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

    val logEncryption = Seq(
      "* Encryption configuration:"
    ) ++ settings.encryption.map { eCfg =>
      val kf = eCfg.keyFields.mkString("[", ", ", "]")
      Seq(
        s"  - Encrypt configuration keys that contain following substrings: $kf",
        s"  - Encrypt metric errors row data: ${eCfg.encryptErrorData}",
      )
    }.getOrElse(Seq("  - Encryption configuration is not set and, therefore, results will not be encrypted."))

    (
      logGeneral ++
        logSparkConf ++
        logTimePref ++
        logStreamingPref ++
        logEnablers ++
        logStorageConf ++
        logEmailConfig ++
        logMMConfig ++
        logExtraVars ++
        logEncryption
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
        .tap(_.foreach(s => log.debug(s._2.schema.treeString))) // debug print schema
        .mapLeft(_.map(e => s"$schemaStage $e")) // update error messages with running stage
    })


  /**
   * Safely reads all regular sources from configuration
   * @note Also executes logging side effects.
   * @param sources Sequence of sources configurations
   * @param schemas Map of source schemas (schemaId -> SourceSchema)
   * @param connections Map of connections (connectionId -> DQConnection)
   * @param readAsStream Boolean flag indicating that sources should be read as a streams
   * @return Either a map of regular sources (sourceId -> Source) or a list of reading errors.
   */
  private def readSources(sources: Seq[SourceConfig],
                          schemas: Map[String, SourceSchema],
                          connections: Map[String, DQConnection],
                          readAsStream: Boolean = false,
                          checkpoints: Map[String, Checkpoint] = Map.empty): Result[Map[String, Source]] = {
    implicit val sc: Map[String, SourceSchema] = schemas
    implicit val c: Map[String, DQConnection] = connections
    implicit val chk: Map[String, Checkpoint] = checkpoints
    
    reduceToMap(sources.map{ srcConf =>
      log.info(s"$sourceStage Reading source '${srcConf.id.value}'...")
      val source = if (readAsStream) srcConf.readStream else srcConf.read
      source.mapValue(s => Seq(s.id -> s))
        .tap(_ => log.info(s"$sourceStage Success!")) // immediate logging of success state
        .tap(_.foreach(s => log.debug(s._2.df.schema.treeString))) // debug source schema
        .mapLeft(_.map(e => s"$sourceStage $e")) // update error messages with running stage
    })
  }

  /**
   * Gets first virtual source for which we have all parent sources resolved.
   * The goal is to resolve virtual sources in order different from
   * the one they were defined in job configuration file.
   *
   * @param vs      Sequence of unresolved virtual sources
   * @param parents Sequence of resolved parent sources
   * @param idx     Index of virtual sources that being checked at current iteration
   *                and will be returned if all its parents are resolved.
   * @return First virtual source for which all parents are reserved
   *         and the sequence of remaining virtual sources.
   */
  @tailrec
  private def getNextVS(vs: Seq[VirtualSourceConfig],
                        parents: Map[String, Source],
                        idx: Int = 0): (VirtualSourceConfig, Seq[VirtualSourceConfig]) = {
    if (idx == vs.size) throw new NoSuchElementException(
      "Unable to find virtual source with all resolved parents. " +
        s"Currently resolved parents are: ${parents.keys.mkString("[", ",", "]")}. " +
        s"The list of remaining unresolved virtual sources is: ${vs.map(_.id.value).mkString("[", ",", "]")}."
    )
    else {
      val checkedVs = vs(idx)
      if (checkedVs.parents.forall(parents.contains)) checkedVs -> vs.zipWithIndex.filter(_._2 != idx).map(_._1)
      else getNextVS(vs, parents, idx + 1)
    }
  }

  /**
   * Recursively reads all virtual sources, thus previously read virtual source can be used as a parent for next one.
   * @note Also executes logging side effects.
   * @param virtualSources Sequence of virtual sources configurations (defined in order they need to be processed)
   * @param parentSources Map of parent source (wrapped into Result in order to chain reading attempts)
   * @param readAsStream Boolean flag indicating that virtual sources should be read as a streams
   * @return Either a map of all (regular + virtual) sources (sourceId -> Source) or a list of reading errors.
   */
  @tailrec
  private def readVirtualSources(virtualSources: Result[Seq[VirtualSourceConfig]],
                                 parentSources: Result[Map[String, Source]],
                                 readAsStream: Boolean = false)
                                (implicit jobId: String): Result[Map[String, Source]] = virtualSources match {
    case Left(errors) => Left(errors)
    case Right(vs) if vs.isEmpty => parentSources
    case Right(vs) =>
      val newAndRest = for {
        parents <- parentSources
        curVsAndRest <- Try(getNextVS(vs, parents)).toResult()
        newSource <- {
          log.info(s"$virtualSourceStage Reading virtual source '${curVsAndRest._1.id.value}'...")
          (if (readAsStream) curVsAndRest._1.readStream(parents) else curVsAndRest._1.read(parents))
            .tap(_ => log.info(s"$virtualSourceStage Success!")) // immediate logging of success state
            .tap(s => log.debug(s.df.schema.treeString)) // debug source schema
            .flatMap{ s =>
              val saveStatus = if (curVsAndRest._1.save.nonEmpty && !readAsStream) {
                val outputDir = curVsAndRest._1.save.get.path.value
                val outputFmt = curVsAndRest._1.save.get.getClass.getSimpleName.dropRight("FileOutputConfig".length)
                log.info(
                  s"$virtualSourceStage Saving virtual source '${curVsAndRest._1.id.value}' to " +
                    s"$outputDir in $outputFmt format."
                )
                saveVirtualSource(curVsAndRest._1, s.df).tap(_ => log.info(s"$virtualSourceStage Success!"))
              } else liftToResult("Virtual source is not saved.")
              saveStatus.mapValue(_ => s)
            } // save virtual source if save option is configured.
            .mapLeft(_.map(e => s"$virtualSourceStage $e")) // update error messages with running stage
        }
        _ <- Try(curVsAndRest._1.persist.foreach{ sLvl =>
          log.info(s"$virtualSourceStage Persisting virtual source '${curVsAndRest._1.id.value}' to ${sLvl.toString}.")
          newSource.df.persist(sLvl)
        }).toResult()
      } yield newSource -> curVsAndRest._2

      val updatedVirtualSource = newAndRest.map(_._2)
      val updatedParents = parentSources
        .combine(newAndRest.map(_._1))((curSrc, newSrc) => curSrc + (newSrc.id -> newSrc))

      readVirtualSources(updatedVirtualSource, updatedParents, readAsStream)
  }

  /**
   * Fundamental Data Quality batch job builder: builds batch job provided with all job components.
   *
   * @param jobId           Job ID
   * @param sources         Sequence of sources to check
   * @param metrics         Sequence of regular metrics to calculate
   * @param composedMetrics Sequence of composed metrics to calculate on top of regular metrics results.
   * @param trendMetrics    Sequence of trend metrics to calculate
   * @param checks          Sequence of checks to preform based in metrics results.
   * @param loadChecks      Sequence of load checks to perform directly over the sources (validate source metadata).
   * @param schemas         Sequence of user-defined schemas used primarily to perform loadChecks (i.e. source schema validation).
   * @param targets         Sequence of targets to be send/saved (alternative channels to communicate DQ Job results).
   * @param connections     Sequence of user-defined connections to external data systems (RDBMS, Kafka, etc..).
   *                        Connections are used primarily to send targets.
   * @return Data Quality batch job instances wrapped into Result[_].
   */
  def buildBatchJob(
                jobConfig: JobConfig,
                jobId: String,
                sources: Seq[Source],
                metrics: Seq[RegularMetricConfig] = Seq.empty,
                composedMetrics: Seq[ComposedMetricConfig] = Seq.empty,
                trendMetrics: Seq[TrendMetricConfig] = Seq.empty,
                checks: Seq[CheckConfig] = Seq.empty,
                loadChecks: Seq[LoadCheckConfig] = Seq.empty,
                targets: Seq[TargetConfig] = Seq.empty,
                schemas: Map[String, SourceSchema] = Map.empty,
                connections: Map[String, DQConnection] = Map.empty
              ): Result[DQBatchJob] = {
    implicit val j: String = jobId
    
    logJobBuildStart
    
    getStorageManager.map(storageManager =>
      DQBatchJob(jobConfig, sources, metrics, composedMetrics, trendMetrics, checks, loadChecks, targets, schemas, connections, storageManager)
    )
  }

  /**
   * Fundamental Data Quality stream job builder: builds stream job provided with all job components.
   *
   * @param jobId           Job ID
   * @param sources         Sequence of sources to check (streamable sources)
   * @param metrics         Sequence of regular metrics to calculate
   * @param composedMetrics Sequence of composed metrics to calculate on top of regular metrics results.
   * @param trendMetrics    Sequence of trend metrics to calculate
   * @param checks          Sequence of checks to preform based in metrics results.
   * @param schemas         Sequence of user-defined schemas used primarily to perform loadChecks (i.e. source schema validation).
   * @param targets         Sequence of targets to be send/saved (alternative channels to communicate DQ Job results).
   * @param connections     Sequence of user-defined connections to external data systems (RDBMS, Kafka, etc..).
   *                        Connections are used primarily to send targets.
   * @return Data Quality stream job instances wrapped into Result[_].
   */
  def buildStreamJob(jobConfig: JobConfig,
                     jobId: String,
                     sources: Seq[Source],
                     metrics: Seq[RegularMetricConfig] = Seq.empty,
                     composedMetrics: Seq[ComposedMetricConfig] = Seq.empty,
                     trendMetrics: Seq[TrendMetricConfig] = Seq.empty,
                     checks: Seq[CheckConfig] = Seq.empty,
                     targets: Seq[TargetConfig] = Seq.empty,
                     schemas: Map[String, SourceSchema] = Map.empty,
                     connections: Map[String, DQConnection] = Map.empty
                   ): Result[DQStreamJob] = {
    implicit val j: String = jobId
    val sourceChecker = (source: Source) => Try {
      if (!source.isStreaming) throw new IllegalArgumentException(
        s"Source '${source.id}' is not streaming and cannot be used in streaming job."
      )
      val requiredColumns = Seq(settings.streamConfig.eventTsCol, settings.streamConfig.windowTsCol)
      if (!requiredColumns.forall(source.df.columns.contains)) throw new IllegalArgumentException(
        s"source '${source.id}' does not contain all required columns: " +
          "either event timestamp or window timestamp column (or both) is missing."
      )
    }.toResult("Streaming source validation failed:")

    logJobBuildStart

    sources.foldLeft(liftToResult(()))((r, src) => r.flatMap(_ => sourceChecker(src)))
      .flatMap(_ => getStorageManager)
      .map(storageManager =>
        DQStreamJob(
          jobConfig,
          sources,
          metrics,
          composedMetrics,
          trendMetrics,
          Seq.empty[LoadCheckConfig], // no load checks are run in streaming jobs
          checks,
          targets,
          Map.empty[String, SourceSchema], // as no need to run load checks then schemas are not needed as well
          connections,
          storageManager
      ))
  }

  /**
   * Builds Data Quality batch job provided with job configuration.
   * @param jobConfig Job configuration
   * @return Data Quality job instance or a list of building errors.
   * @note Data Quality job creation out of job configuration assumes following steps:
   *       - all configured connections are established;
   *       - all schemas defined in job configuration are read;
   *       - all sources (both regular and virtual ones) defined in job configuration are read;
   *       - storage manager is initialized (if configured).
   */
  def buildBatchJob(jobConfig: JobConfig): Result[DQBatchJob] = {
    implicit val jobId: String = jobConfig.jobId.value
    
    logJobBuildStart
    
    val connections = establishConnections(jobConfig.connections.map(_.getAllConnections).getOrElse(Seq.empty))
    val schemas = readSchemas(jobConfig.schemas)
    val regularSources = connections.combine(schemas)((c, s) => (c, s)).flatMap{
      case (conn, sch) => readSources(jobConfig.sources.map(_.getAllSources).getOrElse(Seq.empty), sch, conn)
    }
    val allSources = readVirtualSources(liftToResult(jobConfig.virtualSources), regularSources)
    
    val regularMetrics: Seq[RegularMetricConfig] = jobConfig.metrics.toSeq.flatMap(_.regular).flatMap(_.getAllRegularMetrics)
    val composedMetrics: Seq[ComposedMetricConfig] = jobConfig.metrics.toSeq.flatMap(_.composed)
    val trendMetrics: Seq[TrendMetricConfig] = jobConfig.metrics.toSeq.flatMap(_.trend)
    val checks: Seq[CheckConfig] = jobConfig.checks.toSeq.flatMap(_.getAllChecks)
    val loadChecks: Seq[LoadCheckConfig] = jobConfig.loadChecks.toSeq.flatMap(_.getAllLoadChecks)
    val targets: Seq[TargetConfig] = jobConfig.targets.toSeq.flatMap(_.getAllTargets)
    
    allSources.combineT3(connections, schemas, getStorageManager)(
      (sources, conn, sch, manager) => DQBatchJob(
        jobConfig,
        sources.values.toSeq,
        regularMetrics,
        composedMetrics,
        trendMetrics,
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
   * Builds Data Quality stream job provided with job configuration.
   *
   * @param jobConfig Job configuration
   * @return Data Quality job instance or a list of building errors.
   * @note Data Quality job creation out of job configuration assumes following steps:
   *       - all configured connections are established;
   *       - all schemas defined in job configuration are read;
   *       - all sources (both regular and virtual ones) defined in job configuration are read;
   *       - storage manager is initialized (if configured).
   */
  def buildStreamJob(jobConfig: JobConfig): Result[DQStreamJob] = {
    implicit val jobId: String = jobConfig.jobId.value

    logJobBuildStart
    
    log.info(s"$checkpointStage Reading checkpoint.")
    val bufferCheckpoint = jobConfig.getJobHash.mapValue { jh =>
      settings.streamConfig.checkpointDir match {
        case Some(dir) => CheckpointIO.readCheckpoint(dir.value, jobId, jh)
        case None => 
          log.info(s"$checkpointStage Checkpoint directory is not set.")
          None
      }
    }.tap{
      case Some(_) => log.info(s"$checkpointStage Checkpoints retrieved successfully.")
      case None => log.info(s"$checkpointStage No checkpoint was retrieved. Proceed with empty stream processor buffer.")
    }.mapLeft(_.map(e => s"$checkpointStage $e")) // update error messages with running stage
    
    val checkpoints = bufferCheckpoint.map(optBuffer =>
      optBuffer.map(_.checkpoints.readOnlySnapshot().toMap).getOrElse(Map.empty)
    )
    
    val connections = establishConnections(jobConfig.connections.map(_.getAllConnections).getOrElse(Seq.empty))
    val schemas = readSchemas(jobConfig.schemas)
    val regularSources = connections.union(schemas, checkpoints).flatMap {
      case (conn, sch, checkpoints) => readSources(
        jobConfig.streams.map(_.getAllSources).getOrElse(Seq.empty), sch, conn, 
        readAsStream = true,
        checkpoints = checkpoints
      )
    }

    val allSources = readVirtualSources(liftToResult(jobConfig.virtualStreams), regularSources, readAsStream = true)

    val regularMetrics: Seq[RegularMetricConfig] = jobConfig.metrics.toSeq.flatMap(_.regular).flatMap(_.getAllRegularMetrics)
    val composedMetrics: Seq[ComposedMetricConfig] = jobConfig.metrics.toSeq.flatMap(_.composed)
    val trendMetrics: Seq[TrendMetricConfig] = jobConfig.metrics.toSeq.flatMap(_.trend)
    val checks: Seq[CheckConfig] = jobConfig.checks.toSeq.flatMap(_.getAllChecks)
    val targets: Seq[TargetConfig] = jobConfig.targets.toSeq.flatMap(_.getAllTargets)

    allSources.combineT4(connections, schemas, getStorageManager, bufferCheckpoint)(
      (sources, conn, _, manager, buffer) => DQStreamJob(
        jobConfig,
        sources.values.toSeq,
        regularMetrics,
        composedMetrics,
        trendMetrics,
        Seq.empty[LoadCheckConfig], // no load checks are run in streaming jobs
        checks,
        targets,
        Map.empty[String, SourceSchema], // as no need to run load checks then schemas are not needed as well
        conn,
        manager,
        buffer
      )
    ).mapLeft(_.distinct) // there is some error message duplication that needs to be eliminated.
  }

  /**
   * Build Data Quality batch job provided with sequence of paths to job configuration HOCON files.
   * @param jobConfigs Path to a job-level configuration file (HOCON)
   * @return Data Quality job instance or a list of building errors.
   * @note HOCON format support configuration merging. Thus, it is also allowed to define different parts of
   *       job configuration in different files and merge them prior parsing. This will allow, for example,
   *       not to duplicate sections with common connections configurations or other sections
   *       that are common among several jobs.
   */
  def buildBatchJob(jobConfigs: Seq[String]): Result[DQBatchJob] =
    prepareConfig(jobConfigs, settings.prependVars, "job")
      .flatMap(readJobConfig[InputStreamReader]).flatMap(buildBatchJob)

  /**
   * Build Data Quality stream job provided with sequence of paths to job configuration HOCON files.
   *
   * @param jobConfigs Path to a job-level configuration file (HOCON)
   * @return Data Quality job instance or a list of building errors.
   * @note HOCON format support configuration merging. Thus, it is also allowed to define different parts of
   *       job configuration in different files and merge them prior parsing. This will allow, for example,
   *       not to duplicate sections with common connections configurations or other sections
   *       that are common among several jobs.
   */
  def buildStreamJob(jobConfigs: Seq[String]): Result[DQStreamJob] =
    prepareConfig(jobConfigs, settings.prependVars, "job")
      .flatMap(readJobConfig[InputStreamReader]).flatMap(buildStreamJob)
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
   * @return Either Data Quality context or a list of building errors.
   */
  def build(appConfig: AppConfig,
            referenceDate: Option[String],
            isLocal: Boolean,
            isShared: Boolean,
            doMigration: Boolean,
            prependVariables: String,
            logLvl: Level,
            versionInfo: VersionInfo): Result[DQContext] =
    AppSettings.build(appConfig, referenceDate, isLocal, isShared, doMigration, prependVariables, logLvl, versionInfo)
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
    val prependVariables = getPrependVars(extraVariables)
    AppSettings.build(appConfig, referenceDate, isLocal, isShared, doMigration, prependVariables, logLvl)
      .flatMap(settings => build(settings))
  }
  
}
