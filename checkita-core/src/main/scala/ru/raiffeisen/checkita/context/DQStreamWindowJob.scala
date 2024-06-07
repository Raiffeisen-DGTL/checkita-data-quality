package ru.raiffeisen.checkita.context

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.appconf.StreamConfig
import ru.raiffeisen.checkita.config.jobconf.Checks.CheckConfig
import ru.raiffeisen.checkita.config.jobconf.JobConfig
import ru.raiffeisen.checkita.config.jobconf.LoadChecks.LoadCheckConfig
import ru.raiffeisen.checkita.config.jobconf.Metrics.{ComposedMetricConfig, RegularMetricConfig}
import ru.raiffeisen.checkita.config.jobconf.Targets.TargetConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.core.Source
import ru.raiffeisen.checkita.core.metrics.BasicMetricProcessor.MetricResults
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.AccumulatedErrors
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricProcessor.GroupedCalculators
import ru.raiffeisen.checkita.core.metrics.rdd.RDDMetricStreamProcessor.processWindowResults
import ru.raiffeisen.checkita.core.streaming.{CheckpointIO, ProcessorBuffer}
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager
import ru.raiffeisen.checkita.storage.Models.ResultSet
import ru.raiffeisen.checkita.utils.EnrichedDT
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.util.Try

/**
 * Data Quality Window Job: provides all required functionality to calculate quality metrics, perform checks,
 * save results and send targets for windows of processed streams: runs in a separate thread and monitors
 * streaming processor buffer for windows that are ready to be processed. Once such windows appear, processes
 * them one by one in time-order. This job is started from within a data quality stream job.
 *
 * @param settings        Application settings object. Settings are passed explicitly as they will be updated
 *                        for each window with actual execution and reference datetime.
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
 * @param spark           Implicit spark session object
 * @param fs              Implicit hadoop file system object
 * @param dumpSize        Implicit value of maximum number of metric failures (or errors) to be collected (per metric).
 *                        Used to prevent OOM errors.
 */
final case class DQStreamWindowJob(jobConfig: JobConfig,
                                   settings: AppSettings,
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
                                    val spark: SparkSession,
                                    val fs: FileSystem,
                                    val buffer: ProcessorBuffer,
                                    val dumpSize: Int) extends Thread with DQJob {

  private implicit val streamConfig: StreamConfig = settings.streamConfig
  private val processedSources: Map[String, Source] = sources.filter(src => metricsBySources.contains(src.id))
    .map(src => src.id -> src).toMap
  private val bufferStage: String = RunStage.CheckProcessorBuffer.entryName

  /**
   * Returns copy of the application settings object with modified execution and reference dates:
   *   - reference date is set to the window start time
   *   - execution date is set to current time (time when windows processing has started)
   *
   * @param windowId Window start time as unix epoch
   * @return Copy of the application settings with updated reference and execution datetime.
   */
  private def copySettings(windowId: Long): AppSettings = settings.copy(
    executionDateTime = settings.executionDateTime.resetToCurrentTime,
    referenceDateTime = settings.referenceDateTime.setTo(windowId)
    //EnrichedDT(settings.referenceDateTime.dateFormat, settings.referenceDateTime.timeZone, windowId)
  )

  /**
   * Retrieves minimum watermark from all of the processed streams
   * and searches for the windows that are entirely below this watermark.
   *
   * @return Sequence of windows ready to be processed.
   */
  private def getWindowsToProcess: Seq[Long] = {
    val currentWatermarks = buffer.watermarks.readOnlySnapshot()
      .filter{ case (k, _) => processedSources.contains(k) }
      .filter{ case (_, v) => v != Long.MinValue }
      .values.toSeq

    // -1 if none of the streams has updated its watermark value:
    val minWatermark = if (currentWatermarks.isEmpty) -1 else currentWatermarks.min

    log.debug(s"$bufferStage Minimum watermark: $minWatermark")

    val filterWindows = (windows: Iterable[(String, Long)]) =>
      windows.map(_._2).filter(_ + streamConfig.window.toSeconds < minWatermark).toSet

    val calcBufferKeys = buffer.calculators.readOnlySnapshot().keys
    val errBufferKeys = buffer.calculators.readOnlySnapshot().keys

    log.debug(s"$bufferStage Calculators buffered keys: ${calcBufferKeys.toSeq}")
    log.debug(s"$bufferStage Errors buffered keys: ${errBufferKeys.toSeq}")

    val calcWindows = filterWindows(calcBufferKeys)
    val errWindows = filterWindows(errBufferKeys)

    log.debug(s"$bufferStage Calculators windows below watermark: $calcWindows")
    log.debug(s"$bufferStage Errors windows below watermark: $errWindows")
    log.debug(s"$bufferStage Final windows below watermark: ${calcWindows.intersect(errWindows).toSeq}")

    calcWindows.intersect(errWindows).toSeq.sorted
  }

  /**
   * Retrieves results from processor buffer provided with sequence of windows ready for processing
   *
   * @param windows Sequence of windows ready for processing
   * @return Results from processor buffer ready for processing
   */
  private def getResultsToProcess(
                                   windows: Seq[Long]
                                 ): Seq[(Long, Seq[(String, (GroupedCalculators, Seq[AccumulatedErrors]))])] = {
    val calculators = buffer.calculators.readOnlySnapshot()
    val errors = buffer.errors.readOnlySnapshot()
    val sourceIds = processedSources.keys.toSeq

    windows.map(wId => wId -> sourceIds.map { sId =>
      val thisWindowCalculators = calculators.getOrElse((sId, wId), Map.empty)
      val thisWindowErrors = errors.getOrElse((sId, wId), Seq.empty)
      sId -> (thisWindowCalculators, thisWindowErrors)
    })
  }

  /**
   * Streaming metric processor used to calculate metrics for a particular stream window
   *
   * @param windowStart   Window start time
   *                      (rendered as a string in format configured for reference datetime representation)
   * @param windowResults Processor buffer results for this window
   */
  private case class StreamRegularMetricsProcessor(
                                                    windowStart: String,
                                                    windowResults: Seq[(String, (GroupedCalculators, Seq[AccumulatedErrors]))]
                                                  ) extends RegularMetricsProcessor {
    def run(stage: String): Result[MetricResults] = {
      log.info(s"$stage Processing regular metrics...")
      windowResults.map {
        case (sId, results) =>
          log.info(s"$stage Collecting regular metric results for stream '$sId'...")
          processWindowResults(results._1, results._2, windowStart, sId, processedSources(sId).keyFields)
            .tap(results => logMetricResults(stage, "regular", results))
            .mapLeft(_.map(e => s"$stage $e")) // update error messages with running stage
      } match {
        case results if results.nonEmpty => results.reduce((r1, r2) => r1.combine(r2)(_ ++ _))
        case _ => liftToResult(Map.empty)
      }
    }
  }

  /**
   * Runs windows processing job in a separate thread
   */
  override def run(): Unit = {
    log.info(s"$bufferStage Starting stream windows processing...")
    val migrationState = runStorageMigration(storageStage)(settings)
    var continueRun: Boolean = true
    while (continueRun) {
      val windowsToProcess = getWindowsToProcess
      if (windowsToProcess.isEmpty) {
        log.info(s"$bufferStage There are no windows ready for processing. Waiting...")
        Thread.sleep(streamConfig.trigger.toMillis)
      }
      else {
        log.info(s"$bufferStage Following windows are ready: $windowsToProcess. Processing...")
        val resultsToProcess = getResultsToProcess(windowsToProcess)

        resultsToProcess.foreach {
          case (wId, resultsPerWindow) =>
            implicit val windowSettings: AppSettings = copySettings(wId)

            val windowStart = windowSettings.referenceDateTime.render
            val windowStage = s">>> WINDOW @ $windowStart <<<"
            val regularMetricsProcessor = StreamRegularMetricsProcessor(windowStart, resultsPerWindow)

            log.info(s"$windowStage Results buffer got all results for this window, starting to process them.")

            val resSet: Result[ResultSet] = processAll(regularMetricsProcessor, migrationState, Some(windowStage))

            val windowStatus = resSet.mapValue { _ => // cleaning buffer
              log.info(s"$windowStage DQ Results processed successfully. Cleaning processor buffer...")
              resultsPerWindow.map(_._1).foreach { sId =>
                log.info(s"$windowStage Removing key ($sId, $wId) from buffer...")
                buffer.calculators.remove(sId -> wId)
                buffer.errors.remove(sId -> wId)
              }
              log.info(s"$windowStage Successfully removed results for this window from processor buffer.")
              log.debug(s"$windowStage CALCULATORS buffer now contains following windows: ${buffer.calculators.keys.toSeq}")
              log.debug(s"$windowStage ERRORS buffer now contains following windows: ${buffer.calculators.keys.toSeq}")
            }.union(jobConfig.getJobHash).flatMap { // writing checkpoint
              case (_, jh) => windowSettings.streamConfig.checkpointDir match {
                case Some(dir) =>
                  log.info(s"$windowStage Writing checkpoint to ${dir.value}/$jobId ...")
                  CheckpointIO.writeCheckpoint(
                    buffer,
                    windowSettings.executionDateTime.getUtcTS.toInstant.toEpochMilli,
                    dir.value,
                    jobId,
                    jh
                  )
                case None => liftToResult(
                  log.info(s"$windowStage Checkpoint directory is not set. Continuing without checkpoints.")
                )
              }
            }

            windowStatus match {
              case Right(_) =>
                log.info(s"$windowStage Window results processed successfully.")
              case Left(errs) =>
                log.error(s"$windowStage Window results processing yielded following errors:")
                errs.foreach(log.error)
                Try(spark.streams.active.head.stop()) // todo: implement a graceful query stop with status reporting to main application
                continueRun = false
            }
        }
      }
    }
  }
}
