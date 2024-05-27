package ru.raiffeisen.checkita.context

import com.typesafe.config.ConfigRenderOptions
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.ConfigEncryptor
import ru.raiffeisen.checkita.config.jobconf.Checks.{CheckConfig, SnapshotCheckConfig, TrendCheckConfig}
import ru.raiffeisen.checkita.config.jobconf.LoadChecks.LoadCheckConfig
import ru.raiffeisen.checkita.config.jobconf.Metrics.{ComposedMetricConfig, RegularMetricConfig}
import ru.raiffeisen.checkita.config.jobconf.Targets.TargetConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.core.Results.ResultType
import ru.raiffeisen.checkita.core.{CalculatorStatus, Source}
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema
import ru.raiffeisen.checkita.storage.Connections.DqStorageJdbcConnection
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager
import ru.raiffeisen.checkita.storage.MigrationRunner
import ru.raiffeisen.checkita.targets.TargetProcessors._
import ru.raiffeisen.checkita.storage.Models._
import ru.raiffeisen.checkita.utils.Logging
import ru.raiffeisen.checkita.utils.ResultUtils._
import ru.raiffeisen.checkita.config.IO.{writeEncryptedJobConfig, writeJobConfig}
import ru.raiffeisen.checkita.config.jobconf.JobConfig
import ru.raiffeisen.checkita.core.metrics.BasicMetricProcessor.{MetricResults, processComposedMetrics}

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

/**
 * Base trait defining basic functionality of Data Quality Job
 */
trait DQJob extends Logging {

  val jobConfig: JobConfig
  val sources: Seq[Source]
  val metrics: Seq[RegularMetricConfig]
  val composedMetrics: Seq[ComposedMetricConfig]
  val checks: Seq[CheckConfig]
  val loadChecks: Seq[LoadCheckConfig]
  val targets: Seq[TargetConfig]
  val schemas: Map[String, SourceSchema]
  val connections: Map[String, DQConnection]
  val storageManager: Option[DqStorageManager]

  protected val loadChecksBySources: Map[String, Seq[LoadCheckConfig]] = loadChecks.groupBy(_.source.value)
  protected val metricsMap: Map[String, RegularMetricConfig] = metrics.map(m => m.id.value -> m).toMap
  protected val composedMetricsMap: Map[String, ComposedMetricConfig] = composedMetrics.map(m => m.id.value -> m).toMap
  protected val metricsBySources: Map[String, Seq[RegularMetricConfig]] = metrics.groupBy(_.metricSource)

  implicit val jobId: String
  implicit val spark: SparkSession
  implicit val fs: FileSystem
  implicit val manager: Option[DqStorageManager] = storageManager

  protected val metricStage: String = RunStage.MetricCalculation.entryName
  protected val loadCheckStage: String = RunStage.PerformLoadChecks.entryName
  protected val checksStage: String = RunStage.PerformChecks.entryName
  protected val targetsStage: String = RunStage.ProcessTargets.entryName
  protected val storageStage: String = RunStage.SaveResults.entryName

  /**
   * Metrics processed differently for batch and streaming job.
   * Therefore, to generalize metric processing API, we introduce this trait
   * that common method to process all regular metrics.
   */
  protected trait RegularMetricsProcessor {
    /**
     * Processes all regular metrics
     * @param stage Stage indication used for logging.
     * @return Either a map of metric results or a list of metric processing errors.
     */
    def run(stage: String): Result[MetricResults]
  }

  /**
   * Runs database migration provided with storage manager.
   *
   * @param stage Stage indication used for logging.
   * @param settings Implicit application settings object
   * @return Nothing in case of successful migration or a list of migration errors.
   */
  protected def runStorageMigration(stage: String)
                                   (implicit settings: AppSettings): Result[String] = manager match {
    case Some(mgr) =>
      if (settings.doMigration) {
        log.info(s"$stage Running storage database migration...")
        Try {
          mgr.getConnection match {
            case jdbcConn: DqStorageJdbcConnection =>
              val runner = new MigrationRunner(jdbcConn)
              runner.run()
            case other => throw new UnsupportedOperationException(
              "Storage database migration can only be performed for supported relational database via JDBC connection. " +
                s"But current storage configuration has ${other.getClass.getSimpleName} type of connection."
            )
          }
          "Success"
        }.toResult(preMsg = "Unable to perform Data Quality storage database migration due to following error:").tap(
          _ => log.info(s"$stage Migration successful."),
          _ => log.error(s"$stage Migration failed (error messages are printed at the end of app execution).")
        ).mapLeft(_.map(e => s"$stage $e"))
      } else liftToResult("No migration is required")
    case None =>
      log.warn(s"$storageStage There is no connection to results storage: results will not be saved.")
      liftToResult("No migration was run since there is no connection to results storage.")
  }

  /**
   * Logs metric calculation results.
   * Used during metric processing, to immediately log their calculation status.
   * @param stage Stage indication used for logging.
   * @param metType Type of the metrics being calculated (either regular or composed)
   * @param mr Map with metric results
   */
  protected def logMetricResults(stage: String, metType: String, mr: MetricResults): Unit = mr.foreach {
    case (mId, calcResults) => calcResults.foreach(r => r.errors match {
      case None => log.info(s"$stage ${metType.capitalize} metric '$mId' calculation completed without any errors.")
      case Some(e) if e.errors.isEmpty => log.info(
        s"$stage ${metType.capitalize} metric '$mId' calculation completed without any errors."
      )
      case Some(e) =>
        log.warn(s"$stage ${metType.capitalize} metric '$mId' calculation yielded ${e.errors.size} errors.")
        log.debug(stage + " Error data are collected for following columns:" + e.columns.mkString("[", ", ", "]"))
        e.errors.foreach{ errRow =>
          log.debug(stage + " Error message: " + errRow.message)
          log.debug(stage + " Collected row data: " + errRow.rowData.mkString("[", ", ", "]"))
        }
    })
  }

  /**
   * Calculates all composed metrics provided with regular metric results.
   * @param stage Stage indication used for logging.
   * @param regularMetricResults Map of regular metric results
   * @return Either a map of composed metric results or a list of calculation errors.
   */
  protected def calculateComposedMetrics(stage: String,
                                         regularMetricResults: Result[MetricResults]): Result[MetricResults] =
    regularMetricResults.mapValue { results =>
      if (composedMetrics.nonEmpty) {
        log.info(s"$stage Calculating composed metrics...")
        val compMetRes = processComposedMetrics(composedMetrics, results.toSeq.flatMap(_._2))
        logMetricResults(stage, "composed", compMetRes)
        compMetRes
      } else {
        log.info(s"$stage No composed metrics are defined.")
        Map.empty
      }
    }

  /**
   * Performs all load checks
   * @param stage Stage indication used for logging.
   * @param settings Implicit application settings object
   * @return Sequence of load check results.
   */
  protected def performLoadChecks(stage: String)
                                 (implicit settings: AppSettings): Seq[ResultCheckLoad] =
    if (loadChecksBySources.isEmpty) {
      log.info(s"$stage There are no load checks for this job.")
      Seq.empty
    } else {
      log.info(s"$stage Processing load checks...")
      sources.flatMap { src =>
        loadChecksBySources.get(src.id) match {
          case Some(lcs) =>
            log.info(s"$stage There are ${lcs.size} load checks found for source '${src.id}'.")
            lcs.map { lc: LoadCheckConfig =>
              log.info(s"$stage Running load check '${lc.id.value}'...")
              val calculator = lc.getCalculator
              val lcResult = calculator.run(src, schemas)

              lcResult.status match {
                case CalculatorStatus.Success => log.info(s"$stage Load check is passed.")
                case CalculatorStatus.Failure => log.warn(s"$stage Load check failed with message: ${lcResult.message}")
                case CalculatorStatus.Error => log.warn(s"$stage Load check calculation error: ${lcResult.message}")
              }

              lcResult.finalize(lc.description.map(_.value), lc.metadataString)
            }
          case None =>
            log.info(s"$stage There are no load checks found for source '${src.id}'.")
            Seq.empty[ResultCheckLoad]
        }
      }
    }

  /**
   * Performs all checks
   * @param stage Stage indication used for logging.
   * @param metricResults Map with metric results (both regular and composed)
   * @param settings Implicit application settings object
   * @return Either sequence of check results or a list of check evaluation errors.
   */
  protected def performChecks(stage: String, metricResults: Result[MetricResults])
                             (implicit settings: AppSettings): Result[Seq[ResultCheck]] =
    metricResults.mapValue { metResults =>
      if (checks.nonEmpty) {
        log.info(s"$stage Processing checks...")

        val filterOutTrendChecks = (allChecks: Seq[CheckConfig]) => storageManager match {
          case Some(_) => allChecks
          case None =>
            log.warn(s"$stage There is no connection to results storage: calculation of all trend checks will be skipped.")
            allChecks.filter(c => c.isInstanceOf[TrendCheckConfig]).foreach(c =>
              log.warn(s"$stage Skipping calculation of trend check '${c.id.value}'.")
            )
            allChecks.filterNot(c => c.isInstanceOf[TrendCheckConfig])
        }

        val filterOutMissingMetricRefs = (allChecks: Seq[CheckConfig]) =>
          if (settings.streamConfig.allowEmptyWindows) allChecks.filter{ chk =>
            val metricId = chk.metric.value
            val compareMetricId = chk match {
              case config: SnapshotCheckConfig => config.compareMetric.map(_.value)
              case _ => None
            }
            val predicate = (compareMetricId.toSeq :+ metricId).forall(metResults.contains)
            if (!predicate) log.warn(
              s"$stage Didn't got all required metric results for check '${chk.id.value}'. " +
                "Streaming configuration parameter 'allowEmptyWindows' is set to 'true'. " +
                "Therefore, calculation if this check is skipped."
            )
            predicate
          } else allChecks

        val checksToRun = filterOutTrendChecks.andThen(filterOutMissingMetricRefs)(checks)

        checksToRun.map { chk =>
          log.info(s"$stage Running check '${chk.id.value}'...")
          val chkResult = chk.getCalculator.run(metResults)

          chkResult.status match {
            case CalculatorStatus.Success => log.info(s"$stage Check is passed.")
            case CalculatorStatus.Failure => log.warn(s"$stage Check failed with message: ${chkResult.message}")
            case CalculatorStatus.Error => log.warn(s"$stage Check calculation error: ${chkResult.message}")
          }

          chkResult.finalize(chk.description.map(_.value), chk.metadataString)
        }
      } else {
        log.info(s"$stage No checks are defined.")
        Seq.empty
      }
    }

  /**
   * Finalizes regular metric results: selects only regular metrics results and converts them to
   * final regular metric results representation ready for writing into storage DB or sending via targets.
   * @param stage Stage indication used for logging.
   * @param metricResults Map with all metric results
   * @param settings Implicit application settings object
   * @return Either a finalized sequence of regular metric results or a list of conversion errors.
   */
  protected def finalizeRegularMetrics(stage: String, metricResults: Result[MetricResults])
                                      (implicit settings: AppSettings): Result[Seq[ResultMetricRegular]] =
    metricResults.mapValue { metResults =>
      log.info(s"$stage Finalize regular metric results...")
      metResults.toSeq.flatMap(_._2).filter(_.resultType == ResultType.RegularMetric)
        .map { r =>
          val mConfig = metricsMap.get(r.metricId)
          val desc = mConfig.flatMap(_.description).map(_.value)
          val params = mConfig.flatMap(_.paramString)
          val metadata = mConfig.flatMap(_.metadataString)
          r.finalizeAsRegular(desc, params, metadata)
        }
    }

  /**
   * Finalizes composed metric results: selects only composed metrics results and converts them to
   * final composed metric results representation ready for writing into storage DB or sending via targets.
   *
   * @param stage         Stage indication used for logging.
   * @param metricResults Map with all metric results
   * @param settings      Implicit application settings object
   * @return Either a finalized sequence of composed metric results or a list of conversion errors.
   */
  protected def finalizeComposedMetrics(stage: String, metricResults: Result[MetricResults])
                                       (implicit settings: AppSettings): Result[Seq[ResultMetricComposed]] =
    metricResults.mapValue { metResults =>
      log.info(s"$stage Finalize composed metric results...")
      metResults.toSeq.flatMap(_._2).filter(_.resultType == ResultType.ComposedMetric)
        .map { r =>
          val mConfig = composedMetricsMap.get(r.metricId)
          val desc = mConfig.flatMap(_.description).map(_.value)
          val formula = mConfig.map(_.formula.value).getOrElse("")
          val metadata = mConfig.flatMap(_.metadataString)
          r.finalizeAsComposed(desc, formula, metadata)
        }
    }

  /**
   * Finalizes metric errors: retrieves metrics errors from results and converts them to
   * final metric errors representation ready for writing into storage DB or sending via targets.
   *
   * @param stage         Stage indication used for logging.
   * @param metricResults Map with all metric results
   * @param settings      Implicit application settings object
   * @return Either a finalized sequence of metric errors or a list of conversion errors.
   *
   * @note There could be a situations when metric errors hold the same error data.
   *       We are not interested in sending repeating data neither to storage database nor to Targets.
   *       Therefore, sequence of metric errors is deduplicated by unique constraint.
   */
  protected def finalizeMetricErrors(stage: String, metricResults: Result[MetricResults])
                                    (implicit settings: AppSettings): Result[Seq[ResultMetricError]] =
    metricResults.mapValue { metResults =>
      log.info(s"$stage Finalize metric errors...")
      metResults.toSeq.flatMap(_._2)
        .flatMap(r => r.finalizeMetricErrors)
        .groupBy(_.errorHash).values.map(_.head).toSeq
      // todo: is deduplication a performance issue for large amount of errors?
      // remove records that violate unique constraint - only one record per unique key.
    }

  /**
   * Encrypts rowData field in metric errors if requested per application configuration.
   * Row data field in metric errors contains excerpt from data source and, therefore, these
   * data can contain some sensitive information. In order to protect it, users can configure
   * encryption chapter in application configuration and store encrypted rowData in DQ storage.
   * @param errors Sequence of metric errors to encrypt
   * @param settings Implicit application settings object
   * @return Sequence of metric errors with encrypted rowData field (if requested per configuration) or
   *         a list of encryption errors.
   *
   * @note When metric errors are send via targets rowData field is never encrypted.
   */
  protected def encryptMetricErrors(errors: Seq[ResultMetricError])
                                   (implicit settings: AppSettings): Result[Seq[ResultMetricError]] =
    if (settings.encryption.exists(_.encryptErrorData) && errors.nonEmpty) {
      settings.encryption.toResult("Unable to retrieve encryption configuration:")
        .map(enCfg => new ConfigEncryptor(enCfg.secret, enCfg.keyFields))
        .flatMap { e =>
          Try { errors.map { err =>
            val encryptedRowData = e.encrypt(err.rowData)
            err.copy(rowData = encryptedRowData)
          }}.toResult(preMsg = "Unable to encrypt metric errors' rowData fields due to following error:")
        }
    } else liftToResult(errors)

  /**
   * Finalizes job state: select whether to encrypt or not and converts to the final representation
   * ready for writing into storage DB or sending via targets.
   *
   * @param jobConfig   Parsed Data Quality job configuration
   * @param settings    Implicit application settings object
   * @param jobId       Current Job ID
   * @return Either a finalized of job state.
   */
  protected def finalizeJobState(jobConfig: JobConfig)
                                (implicit settings: AppSettings, jobId: String): Result[JobState] = {

    val renderOpts = ConfigRenderOptions.defaults().setComments(false).setOriginComments(false).setFormatted(false)

    val writeFunc = (jc: JobConfig) => settings.encryption match {
      case Some(e) => writeEncryptedJobConfig(jc)(new ConfigEncryptor(e.secret, e.keyFields))
      case None => writeJobConfig(jc)
    }

    writeFunc(jobConfig).map(jc => JobState(
      jobId,
      jc.root().render(renderOpts),
      settings.versionInfo.asJsonString,
      settings.referenceDateTime.getUtcTS,
      settings.executionDateTime.getUtcTS
    ))
  }


  /**
   * Combines all results into a final result set.
   *
   * @param stage                 Stage indication used for logging.
   * @param loadCheckResults      Sequence of load check results
   * @param checkResults          Sequence of check results (wrapped into Either)
   * @param regularMetricResults  Sequence of regular metric results (wrapped into Either)
   * @param composedMetricResults Sequence of composed metric results (wrapped into Either)
   * @param metricErrors          Sequence of metric errors (wrapped into Either)
   * @param settings              Implicit application settings object
   * @return Combined results in form of ResultSet
   */
  protected def combineResults(stage: String,
                               loadCheckResults: Seq[ResultCheckLoad],
                               checkResults: Result[Seq[ResultCheck]],
                               jobState: Result[JobState],
                               regularMetricResults: Result[Seq[ResultMetricRegular]],
                               composedMetricResults: Result[Seq[ResultMetricComposed]],
                               metricErrors: Result[Seq[ResultMetricError]])
                              (implicit settings: AppSettings): Result[ResultSet] =
    liftToResult(loadCheckResults).combineT5(
      checkResults, jobState, regularMetricResults, composedMetricResults, metricErrors
    ) {
      case (lcChkRes, chkRes, jobRes, regMetRes, compMetRes, metErrs) =>
        log.info(s"$stage Summarize results...")
        ResultSet(sources.size, regMetRes, compMetRes, chkRes, lcChkRes, jobRes, metErrs)
    }

  /**
   * Saves results into Data Quality storage
   * @param stage Stage indication used for logging.
   * @param resultSet Final results set
   * @return Either a status string or a list of saving errors.
   */
  protected def saveResults(stage: String, resultSet: Result[ResultSet])
                           (implicit settings: AppSettings): Result[String] =
    resultSet.flatMap { results =>
      storageManager match {
        case Some(mgr) =>
          import mgr.tables.TableImplicits._

          /** Save results with some logging */
          def saveWithLogs[R <: DQEntity : TypeTag](results: Seq[R], resultsType: String)
                                                   (implicit ops: mgr.tables.DQTableOps[R]): Result[String] =
            Try {
              log.info(s"$stage Saving $resultsType results...")
              mgr.saveResults(results)
            }.toResult().tap(
              r => log.info(s"$stage $r"),
              _ => log.error(s"$stage Failed to write results (error messages are printed at the end of app execution).")
            )

          log.info(s"$stage Saving results...")
          if (!mgr.saveErrors) log.info(
            s"$stage Metric errors will not be saved to storage database as per application configuration. " +
              "Set parameter `saveErrorsToStorage` to `true` in order to save metric errors to storage database."
          )
          // save all results and combine the write operation statuses:
          Seq(
            saveWithLogs(results.regularMetrics, "regular metrics"),
            saveWithLogs(results.composedMetrics, "composed metrics"),
            saveWithLogs(results.loadChecks, "load checks"),
            saveWithLogs(results.checks, "checks"),
            saveWithLogs(Seq(results.jobConfig), "job state"),
            if (mgr.saveErrors)
              encryptMetricErrors(results.metricErrors).flatMap(errs => saveWithLogs(errs, "metric errors"))
            else liftToResult("Metric errors are not saved in storage")
          ).reduce((r1, r2) => r1.combine(r2)((_, _) => "Success"))
            .mapLeft(_.map(e => s"$stage $e")) // update error messages with running stage
        case None =>
          log.warn(s"$stage There is no connection to results storage: results will not be saved.")
          liftToResult("Nothing to save")
      }
    }

  /**
   * Processes all targets
   *
   * @param stage     Stage indication used for logging.
   * @param resultSet Final results set
   * @param settings  Implicit application settings object
   * @return Either unit or a list of target processing errors.
   */
  protected def processTargets(stage: String, resultSet: Result[ResultSet])
                              (implicit settings: AppSettings): Result[Unit] =
    resultSet.flatMap { results =>
      log.info(s"$stage Sending/saving targets...")
      implicit val conn: Map[String, DQConnection] = connections
      targets.map { target =>
        log.info(s"$stage Processing ${target.getClass.getSimpleName.replace("Config", "")}...")
        target.process(results).mapValue(_ => ()).tap(
          _ => log.info(s"$stage Success."),
          _ => log.error(s"$stage Failure (error messages are printed at the end of app execution).")
        ).mapLeft(_.map(e => s"$stage $e")) // update error messages with running stage
      } match {
        case results if results.nonEmpty => results.reduce((t1, t2) => t1.combine(t2)((_, _) => ()))
        case _ =>
          log.info(s"$stage No targets configuration found. Nothing to save.")
          liftToResult(())
      }
    }

  /**
   * Top-level processing function: aggregates and runs all processing stages in required order.
   *
   * @param regularMetricsProcessor Regular metric processor used to calculate regular metric results.
   * @param migrationState          Status of storage migration run
   * @param stagePrefix             Prefix to stage names. Used for logging in streaming applications to
   *                                indicate window for which results are processed.
   * @param settings                Implicit application settings object
   * @return Either final results set or a list of processing errors
   */
  protected def processAll(regularMetricsProcessor: RegularMetricsProcessor,
                           migrationState: Result[String],
                           stagePrefix: Option[String] = None)
                          (implicit settings: AppSettings): Result[ResultSet] = {

    val getStage = (stage: String) => stagePrefix.map(p => s"$p $stage").getOrElse(stage)

    // Perform load checks:
    // This is the first thing that we need to do since load checks verify sources metadata.
    val loadCheckResults = performLoadChecks(getStage(loadCheckStage))
    loadCheckResults.foreach(_ => ()) // evaluate load checks eagerly

    // Calculate regular metric results:
    val regMetCalcResults = regularMetricsProcessor.run(getStage(metricStage))

    // Calculate composed metrics results:
    val compMetCalcResults: Result[MetricResults] =
      calculateComposedMetrics(getStage(metricStage), regMetCalcResults)

    // Combine all metric results together:
    val allMetricCalcResults = regMetCalcResults.combine(compMetCalcResults)(_ ++ _)

    // Perform checks:
    val checkResults = performChecks(getStage(checksStage), allMetricCalcResults)
    // Finalize results:
    val regularMetricResults = finalizeRegularMetrics(getStage(storageStage), allMetricCalcResults)
    val composedMetricResults = finalizeComposedMetrics(getStage(storageStage), allMetricCalcResults)
    val metricErrors = finalizeMetricErrors(getStage(storageStage), allMetricCalcResults)
    val jobState = finalizeJobState(jobConfig)
    // Combine all results:
    val resSet = combineResults(
      getStage(storageStage), loadCheckResults, checkResults, jobState, regularMetricResults, composedMetricResults, metricErrors
    )
    // Save results to storage
    val resSaveState = migrationState.flatMap(_ => saveResults(getStage(storageStage), resSet))

    // process targets:
    val saveTargetsState = processTargets(getStage(targetsStage), resSet)

    resSet.combineT2(resSaveState, saveTargetsState)((results, _, _) => results)
      .mapLeft(_.distinct) // there is some error message duplication that needs to be eliminated.
  }
}
