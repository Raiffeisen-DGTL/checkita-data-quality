package ru.raiffeisen.checkita.config.jobconf

import ru.raiffeisen.checkita.config.RefinedTypes.{ID, SparkParam}
import ru.raiffeisen.checkita.config.jobconf.Checks.ChecksConfig
import ru.raiffeisen.checkita.config.jobconf.Connections.ConnectionsConfig
import ru.raiffeisen.checkita.config.jobconf.LoadChecks.LoadChecksConfig
import ru.raiffeisen.checkita.config.jobconf.Metrics.MetricsConfig
import ru.raiffeisen.checkita.config.jobconf.Schemas.SchemaConfig
import ru.raiffeisen.checkita.config.jobconf.Sources.{SourcesConfig, VirtualSourceConfig}
import ru.raiffeisen.checkita.config.jobconf.Targets.TargetsConfig

/**
 * Data Quality job-level configuration
 * @param jobId Job ID
 * @param connections Connections to external data systems (RDBMS, Message Brokers, etc.)
 * @param schemas Various schema definitions
 * @param sources Data sources processed within current job.
 * @param virtualSources Virtual sources to be created from basic sources
 * @param loadChecks Load checks to be performed on data sources before reading data itself
 * @param metrics Metrics to be calculated for data sources
 * @param checks Checks to be performed over metrics
 * @param targets Targets that define various job result outputs to a multiple channels
 */
final case class JobConfig(
                            jobId: ID,
                            connections: Option[ConnectionsConfig],
                            schemas: Seq[SchemaConfig] = Seq.empty,
                            sources: Option[SourcesConfig],
                            virtualSources: Seq[VirtualSourceConfig] = Seq.empty,
                            loadChecks: Option[LoadChecksConfig],
                            metrics: Option[MetricsConfig],
                            checks: Option[ChecksConfig],
                            targets: Option[TargetsConfig]
                          )
