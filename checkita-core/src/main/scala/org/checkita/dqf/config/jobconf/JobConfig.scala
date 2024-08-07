package org.checkita.dqf.config.jobconf

import eu.timepit.refined.types.string.NonEmptyString
import org.checkita.dqf.config.IO.{RenderOptions, writeJobConfig}
import org.checkita.dqf.config.RefinedTypes.{ID, SparkParam}
import org.checkita.dqf.config.jobconf.Checks.ChecksConfig
import org.checkita.dqf.config.jobconf.Connections.ConnectionsConfig
import org.checkita.dqf.config.jobconf.LoadChecks.LoadChecksConfig
import org.checkita.dqf.config.jobconf.Metrics.MetricsConfig
import org.checkita.dqf.config.jobconf.Schemas.SchemaConfig
import org.checkita.dqf.config.jobconf.Sources.{SourcesConfig, StreamSourcesConfig, VirtualSourceConfig}
import org.checkita.dqf.config.jobconf.Targets.TargetsConfig
import org.checkita.dqf.utils.Common.getStringHash
import org.checkita.dqf.utils.ResultUtils._

/**
 * Data Quality job-level configuration
 * @param jobId Job ID
 * @param jobDescription Job description
 * @param connections Connections to external data systems (RDBMS, Message Brokers, etc.)
 * @param schemas Various schema definitions
 * @param sources Data sources processed within current job (only applicable to batch jobs).
 * @param streams Stream sources processed within current job (only applicable to streaming jobs).
 * @param virtualSources Virtual sources to be created from regular sources.
 * @param virtualStreams Virtual stream to be created from regular streams.
 * @param loadChecks Load checks to be performed on data sources before reading data itself
 * @param metrics Metrics to be calculated for data sources
 * @param checks Checks to be performed over metrics
 * @param targets Targets that define various job result outputs to a multiple channels
 * @param jobMetadata List of metadata parameters
 */
final case class JobConfig(
                            jobId: ID,
                            jobDescription: Option[NonEmptyString],
                            connections: Option[ConnectionsConfig],
                            schemas: Seq[SchemaConfig] = Seq.empty,
                            sources: Option[SourcesConfig],
                            streams: Option[StreamSourcesConfig],
                            virtualSources: Seq[VirtualSourceConfig] = Seq.empty,
                            virtualStreams: Seq[VirtualSourceConfig] = Seq.empty,
                            loadChecks: Option[LoadChecksConfig],
                            metrics: Option[MetricsConfig],
                            checks: Option[ChecksConfig],
                            targets: Option[TargetsConfig],
                            jobMetadata: Seq[SparkParam] = Seq.empty
                          ) {
  
  lazy val rendered: Result[String] = writeJobConfig(this).mapValue(jc => jc.root().render(RenderOptions.COMPACT))
  def getJobHash: Result[String] = rendered.mapValue(getStringHash)
}
