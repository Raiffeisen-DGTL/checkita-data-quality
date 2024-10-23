package org.checkita.dqf.context

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.Enums.MetricEngineAPI
import org.checkita.dqf.config.jobconf.Checks.CheckConfig
import org.checkita.dqf.config.jobconf.JobConfig
import org.checkita.dqf.config.jobconf.LoadChecks.LoadCheckConfig
import org.checkita.dqf.config.jobconf.Metrics.{ComposedMetricConfig, RegularMetricConfig, TrendMetricConfig}
import org.checkita.dqf.config.jobconf.Targets.TargetConfig
import org.checkita.dqf.connections.DQConnection
import org.checkita.dqf.core.Source
import org.checkita.dqf.core.metrics.rdd.RDDMetricBatchProcessor.{processRegularMetrics => processRegularMetricsRDD}
import org.checkita.dqf.core.metrics.df.DFMetricProcessor.{processRegularMetrics => processRegularMetricsDF}
import org.checkita.dqf.core.metrics.BasicMetricProcessor.MetricResults
import org.checkita.dqf.core.metrics.RegularMetric
import org.checkita.dqf.readers.SchemaReaders.SourceSchema
import org.checkita.dqf.storage.Managers.DqStorageManager
import org.checkita.dqf.storage.Models.ResultSet
import org.checkita.dqf.utils.ResultUtils._

import scala.language.higherKinds

/**
 * Data Quality Batch Job: provides all required functionality to calculate quality metrics, perform checks,
 * save results and send targets for static data sources.
 *
 * @param sources         Sequence of sources to process
 * @param metrics         Sequence of metrics to calculate
 * @param composedMetrics Sequence of composed metrics to calculate
 * @param trendMetrics    Sequence of trend metrics to calculate
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
                            trendMetrics: Seq[TrendMetricConfig] = Seq.empty,
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
   * Redirects regular metric processing to processor with
   * requested Spark API.
   *
   * @param source        Source to process metrics for
   * @param sourceMetrics Sequence of metrics defined for the given source
   * @param dumpSize      Implicit value of maximum number of metric failure (or errors) to be collected
   *                      (per metric and per partition). Used to prevent OOM errors.
   * @param caseSensitive Implicit flag defining whether column names are case sensitive or not.
   * @return Map of metricId to a sequence of metric results for this metricId (some metrics yield multiple results).
   */
  private def processRegularMetrics(source: Source,
                                    sourceMetrics: Seq[RegularMetric])
                                   (implicit dumpSize: Int,
                                    caseSensitive: Boolean): Result[MetricResults] =
    settings.metricEngineAPI match {
      case MetricEngineAPI.RDD => processRegularMetricsRDD(source, sourceMetrics)
      case MetricEngineAPI.DF => processRegularMetricsDF(source, sourceMetrics)
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
    // Continue to run job only if storage migration was successful (or omitted):
    val resSet = migrationState.flatMap(_ => processAll(BatchRegularMetricProcessor))
    resSet.tap(_ => logPostMsg())
  }
}