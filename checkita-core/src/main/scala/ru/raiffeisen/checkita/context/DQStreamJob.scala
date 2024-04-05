package ru.raiffeisen.checkita.context

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.appconf.StreamConfig
import ru.raiffeisen.checkita.config.jobconf.Checks.CheckConfig
import ru.raiffeisen.checkita.config.jobconf.JobConfig
import ru.raiffeisen.checkita.config.jobconf.LoadChecks.LoadCheckConfig
import ru.raiffeisen.checkita.config.jobconf.Metrics.{ComposedMetricConfig, RegularMetricConfig}
import ru.raiffeisen.checkita.config.jobconf.Targets.TargetConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.core.Source
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricStreamProcessor.{ProcessorBuffer, processRegularMetrics}
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager
import ru.raiffeisen.checkita.utils.Logging
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.language.higherKinds
import scala.util.Try

/**
 * Data Quality Streaming Job: provides all required functionality to calculate quality metrics, perform checks,
 * save results and send targets for streaming data sources.
 *
 * @param sources         Sequence of sources to process
 * @param metrics         Sequence of metrics to calculate
 * @param composedMetrics Sequence of composed metrics to calculate
 * @param loadChecks      Sequence of load checks to perform
 * @param checks          Sequence of checks to perform
 * @param targets         Sequence of targets to send
 * @param schemas         Map of user-defined schemas (used for load checks evaluation)
 * @param connections     Map of connections to external systems (used to send targets)
 * @param storageManager  Data Quality Storage manager (used to save results)
 * @param jobId           Implicit job ID
 * @param settings        Implicit application settings object
 * @param spark           Implicit spark session object
 * @param fs              Implicit hadoop file system object
 */
final case class DQStreamJob(jobConfig: JobConfig,
                             sources: Seq[Source],
                             metrics: Seq[RegularMetricConfig],
                             composedMetrics: Seq[ComposedMetricConfig] = Seq.empty,
                             loadChecks: Seq[LoadCheckConfig] = Seq.empty,
                             checks: Seq[CheckConfig] = Seq.empty,
                             targets: Seq[TargetConfig] = Seq.empty,
                             schemas: Map[String, SourceSchema] = Map.empty,
                             connections: Map[String, DQConnection] = Map.empty,
                             storageManager: Option[DqStorageManager] = None
                            )(implicit jobId: String,
                              settings: AppSettings,
                              spark: SparkSession,
                              fs: FileSystem) extends Logging {

  // todo: load checks are currently not supported in streaming applications
  //       since use cases for load checks in streams are quite unclear for now.

  private val metricsBySources: Map[String, Seq[RegularMetricConfig]] = metrics.groupBy(_.metricSource)
  private implicit val streamConfig: StreamConfig = settings.streamConfig
  implicit val dumpSize: Int = settings.errorDumpSize
  implicit val caseSensitive: Boolean = settings.enableCaseSensitivity



  protected def logPreMsg(): Unit = {
    log.info("************************************************************************")
    log.info(s"               Starting streaming job '$jobId'")
    log.info("************************************************************************")
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
  def run(): Result[Unit] = Try {

    // we will process only those streams which have metrics associated with them.
    // e.g. there can be a regular stream and virtual one, created from it.
    // Thus, metrics can only be associated with virtual stream. Therefore, it is not
    // necessary to create foreachBatch sink for the parent one.
    val processedSources = sources.filter(src => metricsBySources.contains(src.id))

    implicit val processorBuffer: ProcessorBuffer = ProcessorBuffer.init(processedSources.map(_.id))

    val windowJob = DQStreamWindowJob(
      jobConfig,
      settings,
      sources,
      metrics,
      composedMetrics,
      checks,
      Seq.empty[LoadCheckConfig], // no load checks are run within window job
      targets,
      Map.empty[String, SourceSchema], // as no need to run load checks then schemas are not needed as well
      connections,
      storageManager
    )

    logPreMsg()

    val streamSinks = processedSources.map(src =>
      src.df.writeStream
        .queryName(src.id)
        .trigger(Trigger.ProcessingTime(streamConfig.trigger))
        .foreachBatch(processRegularMetrics(src.id, src.keyFields, metricsBySources(src.id)))
        .start()
    )
    streamSinks.foreach(q => log.info(s"[STARTING STREAMING QUERY] Starting query '${q.name}'..."))

    // start window results processing in a separate thread:
    windowJob.start()

    // await streams termination:
    log.info("[AWAITING STREAMING QUERY TERMINATION]")
    spark.streams.awaitAnyTermination()
  }.toResult(preMsg = s"Streaming job '$jobId' was terminated due to following errors:")
}
