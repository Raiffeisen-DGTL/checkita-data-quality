package org.checkita.dqf.config.appconf

import org.checkita.dqf.config.RefinedTypes.PositiveInt
import eu.timepit.refined.auto._
import org.checkita.dqf.config.Enums.{CheckFailureTolerance, MetricEngineAPI}

/**
 * Application-level configuration for switchers (enablers)
 *
 * @param allowSqlQueries       Enables arbitrary SQL queries in virtual sources
 * @param allowNotifications    Enables notifications to be sent from DQ application
 * @param aggregatedKafkaOutput Enables sending aggregates messages for Kafka Targets
 *                              (one per each target type, except checkAlerts where
 *                              one message per checkAlert will be sent)
 * @param enableCaseSensitivity Enable columns case sensitivity
 * @param errorDumpSize         Maximum number of errors to be collected per single metric per partition.
 * @param outputRepartition     Sets the number of partitions when writing outputs. By default writes single file.
 * @param metricEngineAPI       Metric processor API used to process metrics: either Spark RDD or Spark DF.
 * @param checkFailureTolerance Returns the failure status if any of the checks fail.
 */
final case class Enablers(
                           allowSqlQueries: Boolean = false,
                           allowNotifications: Boolean = false,
                           aggregatedKafkaOutput: Boolean = false,
                           enableCaseSensitivity: Boolean = false,
                           errorDumpSize: PositiveInt = 10000,
                           outputRepartition: PositiveInt = 1,
                           metricEngineAPI: MetricEngineAPI = MetricEngineAPI.RDD,
                           checkFailureTolerance: CheckFailureTolerance = CheckFailureTolerance.None
                         )
