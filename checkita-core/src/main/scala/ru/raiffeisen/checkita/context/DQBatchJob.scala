package ru.raiffeisen.checkita.context

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Checks.CheckConfig
import ru.raiffeisen.checkita.config.jobconf.JobConfig
import ru.raiffeisen.checkita.config.jobconf.LoadChecks.LoadCheckConfig
import ru.raiffeisen.checkita.config.jobconf.Metrics.{ComposedMetricConfig, RegularMetricConfig}
import ru.raiffeisen.checkita.config.jobconf.Targets.TargetConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.core.Source
import ru.raiffeisen.checkita.core.metrics.MetricBatchProcessor.processRegularMetrics
import ru.raiffeisen.checkita.core.metrics.MetricProcessor.MetricResults
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager
import ru.raiffeisen.checkita.storage.Models.ResultSet
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.language.higherKinds

/**
 * Data Quality Batch Job: provides all required functionality to calculate quality metrics, perform checks,
 * save results and send targets for static data sources.
 *
 * @param sources         Sequence of sources to process
 * @param metrics         Sequence of metrics to calculate
 * @param composedMetrics Sequence of composed metrics to calculate
 * @param checks          Sequence of checks to perform
 * @param loadChecks      Sequence of load checks to perform
 * @param targets         Sequence of targets to send
 * @param schemas         Map of user-defined schemas (used for load checks evaluation)
 * @param connections     Map of connections to external systems (used to send targets)
 * @param storageManager  Data Quality Storage manager (used to save results)
 * @param jobId           Implicit job ID
 * @param settings        Implicit application settings object
 * @param spark           Implicit spark session object
 * @param fs              Implicit hadoop file system object
 */
final case class DQBatchJob(jobConfig: JobConfig,
                            sources: Seq[Source],
                            metrics: Seq[RegularMetricConfig],
                            composedMetrics: Seq[ComposedMetricConfig] = Seq.empty,
                            checks: Seq[CheckConfig] = Seq.empty,
                            loadChecks: Seq[LoadCheckConfig] = Seq.empty,
                            targets: Seq[TargetConfig] = Seq.empty,
                            schemas: Map[String, SourceSchema] = Map.empty,
                            connections: Map[String, DQConnection] = Map.empty,
                            storageManager: Option[DqStorageManager] = None
                           )(implicit val jobId: String,
                             val settings: AppSettings,
                             val spark: SparkSession,
                             val fs: FileSystem) extends DQJob {

  implicit val dumpSize: Int = settings.errorDumpSize
  implicit val caseSensitive: Boolean = settings.enableCaseSensitivity

  protected def logPreMsg(): Unit = {
    log.info("************************************************************************")
    log.info(s"               Starting execution of job '$jobId'")
    log.info("************************************************************************")
  }

  protected def logPostMsg(): Unit = {
    log.info("************************************************************************")
    log.info(s"               Finishing execution of job '$jobId'")
    log.info("************************************************************************")
  }

  /**
   * Regular metric processor used to calculate metrics over a static data sources.
   */
  private object BatchRegularMetricProcessor extends RegularMetricsProcessor {
    def run(stage: String): Result[MetricResults] = {
      log.info(s"$metricStage Calculating regular metrics...")
      sources.map { src =>
        metricsBySources.get(src.id) match {
          case Some(metrics) =>
            log.info(s"$stage There are ${metrics.size} regular metrics found for source '${src.id}'.")
            processRegularMetrics(src, metrics)
              .tap(results => logMetricResults(stage, "regular", results))
              .mapLeft(_.map(e => s"$stage $e")) // update error messages with running stage
          case None =>
            log.info(s"$stage There are no regular metrics found for source '${src.id}'.")
            Right(Map.empty).asInstanceOf[Result[MetricResults]]
        }
      } match {
        case results if results.nonEmpty => results.reduce((r1, r2) => r1.combine(r2)(_ ++ _))
        case _ => liftToResult(Map.empty)
      }
    }
  }

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
    logPreMsg()
    // Run storage migration if necessary
    val migrationState = runStorageMigration(storageStage)
    val resSet = processAll(BatchRegularMetricProcessor, migrationState)
    resSet.tap(_ => logPostMsg())
  }
}