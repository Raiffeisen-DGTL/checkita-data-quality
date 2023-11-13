package ru.raiffeisen.checkita.context

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Checks.{CheckConfig, TrendCheckConfig}
import ru.raiffeisen.checkita.config.jobconf.LoadChecks.LoadCheckConfig
import ru.raiffeisen.checkita.config.jobconf.Metrics.{ComposedMetricConfig, RegularMetricConfig}
import ru.raiffeisen.checkita.config.jobconf.Targets.TargetConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.core.Results.ResultType
import ru.raiffeisen.checkita.core.metrics.MetricProcessor.{MetricResults, processComposedMetrics, processRegularMetrics}
import ru.raiffeisen.checkita.core.{CalculatorStatus, Source}
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema
import ru.raiffeisen.checkita.storage.Connections.DqStorageJdbcConnection
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager
import ru.raiffeisen.checkita.storage.MigrationRunner
import ru.raiffeisen.checkita.storage.Models.{DQEntity, ResultCheckLoad, ResultSet}
import ru.raiffeisen.checkita.targets.TargetProcessors._
import ru.raiffeisen.checkita.utils.ResultUtils._
import ru.raiffeisen.checkita.utils.Logging

import scala.{Traversable, collection}
import scala.collection.Traversable
import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

case class DQJob(sources: Seq[Source],
                 metrics: Seq[RegularMetricConfig],
                 composedMetrics: Seq[ComposedMetricConfig] = Seq.empty,
                 checks: Seq[CheckConfig] = Seq.empty,
                 loadChecks: Seq[LoadCheckConfig] = Seq.empty,
                 targets: Seq[TargetConfig] = Seq.empty,
                 schemas: Map[String, SourceSchema] = Map.empty,
                 connections: Map[String, DQConnection] = Map.empty,
                 storageManager: Option[DqStorageManager] = None
                )(implicit jobId: String,
                  settings: AppSettings,
                  spark: SparkSession,
                  fs: FileSystem) extends Logging {
  
  
  private val metricStage: String = RunStage.MetricCalculation.entryName
  private val loadCheckStage: String = RunStage.PerformLoadChecks.entryName
  private val checksStage: String = RunStage.PerformChecks.entryName
  private val targetsStage: String = RunStage.ProcessTargets.entryName
  private val storageStage: String = RunStage.SaveResults.entryName

  private def logPreMsg(): Unit = {
    log.info("************************************************************************")
    log.info(s"               Starting execution of job '$jobId'")
    log.info("************************************************************************")
  }

  private def logPostMsg(): Unit = {
    log.info("************************************************************************")
    log.info(s"               Finishing execution of job '$jobId'")
    log.info("************************************************************************")
  }

  /**
   * Runs database migration provided with storage manager.
   * @param storage Data Quality storage manager
   * @return Nothing in case of successful migration or a list of migration errors.
   */
  private def runStorageMigration(storage: DqStorageManager): Result[Unit] = Try{
    storage.getConnection match {
      case jdbcConn: DqStorageJdbcConnection =>
        val runner = new MigrationRunner(jdbcConn)
        runner.run()
      case other => throw new UnsupportedOperationException(
        "Storage database migration can only be performed for supported relational database via JDBC connection. " +
          s"But current storage configuration has ${other.getClass.getSimpleName} type of connection."
      )
    }
  }.toResult(
    preMsg = "Unable to perform Data Quality storage database migration due to following error:"
  )
  
  /**
   * Runs Data Quality job. Job include following stages
   * (some of them can be omitted depending on the configuration):
   *   - processing load checks
   *   - calculating regular metrics
   *   - calculating composed composed metrics
   *   - processing checks
   *   - saving results
   *   - sending/saving targets
   * @return Either a set of job results or a list of errors that occurred during job run.
   */
  def run: Result[ResultSet] = {
    implicit val dumpSize: Int = settings.errorDumpSize
    implicit val caseSensitive: Boolean = settings.enableCaseSensitivity
    implicit val manager: Option[DqStorageManager] = storageManager
    
    logPreMsg()
    
    val loadChecksBySources: Map[String, Seq[LoadCheckConfig]] = loadChecks.groupBy(_.source.value)
    val metricsMap: Map[String, RegularMetricConfig] = metrics.map(m => m.id.value -> m).toMap
    val composedMetricsMap: Map[String, ComposedMetricConfig] = composedMetrics.map(m => m.id.value -> m).toMap
    val metricsBySources: Map[String, Seq[RegularMetricConfig]] = metrics.groupBy(_.metricSource)
    
    // Perform load checks:
    // This is the first thing that we need to do since load checks verify sources metadata.
    log.info(s"$loadCheckStage Processing load checks...")
    val loadCheckResults: Seq[ResultCheckLoad] = sources.flatMap { src =>
      loadChecksBySources.get(src.id) match {
        case Some(lcs) => 
          log.info(s"$loadCheckStage There are ${lcs.size} load checks found for source '${src.id}'.")
          lcs.map { lc: LoadCheckConfig =>
            log.info(s"$loadCheckStage Running load check '${lc.id.value}'...")
            val calculator = lc.getCalculator
            val lcResult = calculator.run(src, schemas)
            
            lcResult.status match {
              case CalculatorStatus.Success => log.info(s"$loadCheckStage Load check is passed.")
              case CalculatorStatus.Failure => log.warn(s"$loadCheckStage Load check failed with message: ${lcResult.message}")
              case CalculatorStatus.Error => log.warn(s"$loadCheckStage Load check calculation error: ${lcResult.message}")
            }

            lcResult.finalize
          }
        case None => 
          log.info(s"$loadCheckStage There are no load checks found for source '${src.id}'.")
          Seq.empty[ResultCheckLoad]
      }
    }
    loadCheckResults.foreach(_ => ()) // evaluate load checks eagerly
    
    // Calculate regular metrics:
    log.info(s"$metricStage Calculating regular metrics...")
    val metCalcResults: Result[MetricResults] = sources.map { src =>
      metricsBySources.get(src.id) match {
        case Some(metrics) =>
          log.info(s"$metricStage There are ${metrics.size} regular metrics found for source '${src.id}'.")
          
          processRegularMetrics(src, metrics).tap(results => results.foreach {
            case (mId, calcResults) => calcResults.foreach(r => r.errors match {
                case None => log.info(s"$metricStage Metric '$mId' calculation completed without any errors.")
                case Some(e) => 
                  if (e.errors.isEmpty) log.info(s"$metricStage Metric '$mId' calculation completed without any errors.")
                  else log.warn(s"$metricStage Metric '$mId' calculation yielded ${e.errors.size} errors.")
              }
            ) // immediate logging of of metric calculation state
          }).mapLeft(_.map(e => s"$metricStage $e")) // update error messages with running stage
        case None => 
          log.info(s"$metricStage There are no regular metrics found for source '${src.id}'.")
          Right(Map.empty).asInstanceOf[Result[MetricResults]]
      }
    } match {
      case results if results.nonEmpty => results.reduce((r1, r2) => r1.combine(r2)(_ ++ _))
      case _ => liftToResult(Map.empty)
    }
    
    // Calculate composed metrics:
    val compMetCalcResults: Result[MetricResults] = metCalcResults.mapValue { results =>
      if (composedMetrics.nonEmpty) {
        log.info(s"$metricStage Calculating composed metrics...")
        val compMetRes = processComposedMetrics(composedMetrics, results.toSeq.flatMap(_._2))
        compMetRes.foreach {
          case (mId, calcResults) => calcResults.foreach(r => r.errors match {
            case None => log.info(s"$metricStage Composed metric '$mId' calculation completed without any errors.")
            case Some(e) =>
              if (e.errors.isEmpty) log.info(s"$metricStage Composed metric '$mId' calculation completed without any errors.")
              else log.warn(s"$metricStage Composed metric '$mId' calculation yielded ${e.errors.size} errors.")
          })
        } // immediate logging of of composed metric calculation state
        compMetRes
      } else {
        log.info(s"$metricStage No composed metrics are defined.")
        Map.empty
      }
    }
    
    val allMetricCalcResults = metCalcResults.combine(compMetCalcResults)(_ ++ _)
    
    // Perform checks:
    val checkResults = allMetricCalcResults.mapValue{ metResults =>
      if (checks.nonEmpty) {
        log.info(s"$checksStage Processing checks...")

        val checksToRun = storageManager match {
          case Some(_) => checks
          case None =>
            log.warn(s"$checksStage There is no connection to results storage: calculation of all trend checks will be skipped.")
            checks.filter(c => c.isInstanceOf[TrendCheckConfig]).foreach(c =>
              log.warn(s"$checksStage Skipping calculation of trend check '${c.id.value}'.")
            )
            checks.filterNot(c => c.isInstanceOf[TrendCheckConfig])
        }

        checksToRun.map { chk =>
          log.info(s"$checksStage Running check '${chk.id.value}'...")
          val chkResult = chk.getCalculator.run(metResults)

          chkResult.status match {
            case CalculatorStatus.Success => log.info(s"$checksStage Check is passed.")
            case CalculatorStatus.Failure => log.warn(s"$checksStage Check failed with message: ${chkResult.message}")
            case CalculatorStatus.Error => log.warn(s"$checksStage Check calculation error: ${chkResult.message}")
          }

          chkResult.finalize(chk.description.map(_.value))
        }
      } else {
        log.info(s"$checksStage No checks are defined.")
        Seq.empty
      }
    }

    // Finalize regular metric results:
    val regularMetricResults = allMetricCalcResults.mapValue { metResults =>
      log.info(s"$storageStage Finalize regular metric results...")
      metResults.toSeq.flatMap(_._2).filter(_.resultType == ResultType.RegularMetric)
        .map { r =>
          val mConfig = metricsMap.get(r.metricId)
          val desc = mConfig.flatMap(_.description).map(_.value)
          val params = mConfig.flatMap(_.paramString)
          r.finalizeAsRegular(desc, params)
        }
    }
    
    // Finalize composed metrics results:
    val composedMetricResults = allMetricCalcResults.mapValue { metResults =>
      log.info(s"$storageStage Finalize composed metric results...")
      metResults.toSeq.flatMap(_._2).filter(_.resultType == ResultType.ComposedMetric)
        .map { r =>
          val mConfig = composedMetricsMap.get(r.metricId)
          val desc = mConfig.flatMap(_.description).map(_.value)
          val formula = mConfig.map(_.formula.value).getOrElse("")
          r.finalizeAsComposed(desc, formula)
        }
    }
    
    // Finalize metric errors:
    val metricErrors = allMetricCalcResults.mapValue { metResults =>
      log.info(s"$storageStage Finalize metric errors...")
      metResults.toSeq.flatMap(_._2).flatMap(r => r.finalizeMetricErrors)
    }
    
    // Combine all results:
    val resSet = liftToResult(loadCheckResults).combineT4(
      checkResults, regularMetricResults, composedMetricResults, metricErrors
    ){
      case (lcChkRes, chkRes, regMetRes, compMetRes, metErrs) =>
        log.info(s"$storageStage Summarize results...")
        ResultSet(sources.size, regMetRes, compMetRes, chkRes, lcChkRes, metErrs)
    }
    
    // Save results to storage:
    val resSaveState = resSet.flatMap{ results =>
      storageManager match {
        case Some(mgr) =>
          import mgr.tables.TableImplicits._

          /** Save results with some logging */
          def saveWithLogs[R <: DQEntity : TypeTag](results: Seq[R], resultsType: String)
                                                   (implicit ops: mgr.tables.DQTableOps[R]): Result[String] =
            Try {
              log.info(s"$storageStage Saving $resultsType results...")
              mgr.saveResults(results)
            }.toResult().tap(
              r => log.info(s"$storageStage $r"),
              _ => log.error(s"$storageStage Failed to write results (error messages are printed at the end of app execution).")
            )
            
          
          val migrationState = 
            if (settings.doMigration) {
              log.info(s"$storageStage Running storage database migration...")
              runStorageMigration(mgr).map(_ => "Success")
                .tap(
                  _ => log.info(s"$storageStage Migration successful."), 
                  _ => log.error(s"$storageStage Migration failed (error messages are printed at the end of app execution).")
                )
                .mapLeft(_.map(e => s"$storageStage $e")) // update error messages with running stage
            }
            else liftToResult("No migration is required")
          
          migrationState.flatMap { _ =>
            log.info(s"$storageStage Saving results...")
            // save all results and combine the write operation results:
            Seq(
              saveWithLogs(results.regularMetrics, "regular metrics"),
              saveWithLogs(results.composedMetrics, "composed metrics"),
              saveWithLogs(results.loadChecks, "load checks"),
              saveWithLogs(results.checks, "checks")
            ).reduce((r1, r2) => r1.combine(r2)((_, _) => "Success"))
              .mapLeft(_.map(e => s"$storageStage $e")) // update error messages with running stage
          }
        case None => 
          log.warn(s"$storageStage There is no connection to results storage: results will not be saved.")
          liftToResult("Nothing to save")
      }
    }

    // process targets:
    val saveTargetsState = resSet.flatMap{ results =>
      log.info(s"$targetsStage Sending/saving targets...")
      implicit val conn: Map[String, DQConnection] = connections
      targets.map{ target =>
        log.info(s"$targetsStage Processing ${target.getClass.getSimpleName.replace("Config", "")}...")
        target.process(results).mapValue(_ => ())
          .tap(
            _ => log.info(s"$targetsStage Success."),
            _ => log.error(s"$targetsStage Failure (error messages are printed at the end of app execution).")
          ) // immediate logging of save operation state state
          .mapLeft(_.map(e => s"$targetsStage $e")) // update error messages with running stage
      } match {
        case results if results.nonEmpty => results.reduce((t1, t2) => t1.combine(t2)((_, _) => ()))
        case _ => 
          log.info(s"$targetsStage No targets configuration found. Nothing to save.")
          liftToResult(())
      }
    }
    
    resSet.combineT2(resSaveState, saveTargetsState)((results, _, _) => results).tap(_ => logPostMsg())
      .mapLeft(_.distinct) // there is some error message duplication that needs to be eliminated.
  }
}