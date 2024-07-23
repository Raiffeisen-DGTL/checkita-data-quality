package org.checkita.config.jobconf

import org.json4s.jackson.Serialization.write
import eu.timepit.refined.types.string.NonEmptyString
import org.checkita.config.RefinedTypes.{ID, SparkParam}
import org.checkita.utils.Common.jsonFormats

import scala.util.Try

/**
 * Base trait to configure DQ entities.
 * Every entity within a configuration must have and ID and might have a description.
 * In addition to that we allow users to set their own list of additional parameters
 * that can be used for integration purposes. These parameters should be listed in
 * metadata field as strings of format "key=value".
 *
 * Targets is the only configuration sections that does not conform to this contract.
 * This is due to this section basically defines the alternative channels to send results to
 * rather than defines any kind of DQ entities.
 */
private[jobconf] trait JobConfigEntity extends Serializable {
  val id: ID
  val description: Option[NonEmptyString]
  val metadata: Seq[SparkParam]
  val metadataString: Option[String] = Try(write(metadata.map(_.value))).toOption
}
