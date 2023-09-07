package ru.raiffeisen.checkita.utils.versions

import com.typesafe.config.Config
import scala.util.{Success, Try}

/**
  This trait is representing a list of parameters in the configuration which were changing with time.
  So far this is a temporary solution that will be refactored as soon as new ConfigReader 
  will be rewritten in a more functional way.
 */
sealed trait BackCompatibilityConfiguration {
  // Config reader extractors
  val delimiterExtractor: Config => Option[String]
  val quoteModeExtractor: Config => Option[String]

  // Post processing formatters
  val trKeyName: String
  val trValueName: String
  val keyFormatter: Int => String
  val valueFormatter: Int => String

  // Target types
  val fileMetricTargetType: String
  val columnMetricTargetType: String
  val composedMetricTargetType: String
  val checkTargetType: String
  val loadCheckTargetType: String
}

object BackCompatibilityConfiguration {
  def getConfig(version: String): BackCompatibilityConfiguration = version match {
    case x if x >= "2." => new V2()
    case x if x >= "1." => new V1()
    case _ => new V0()
  }
}

// Used in pre-release versions
class V0 extends BackCompatibilityConfiguration {
  override val delimiterExtractor: Config => Option[String] =
    (conf: Config) => Try(conf.getString("delimiter")).toOption
  override val quoteModeExtractor: Config => Option[String] =
    (conf: Config) => Try(conf.getBoolean("quoted")) match {
      case Success(true) => Some("ALL")
      case _ => Some("NONE")
    }

  override val trKeyName: String = "VARIABLE"
  override val trValueName: String = "VARIABLE_VALUE"

  override val keyFormatter: Int => String = (x: Int) => s"KEY$x"
  override val valueFormatter: Int => String = (x: Int) => s"KEY${x}_VALUE"

  override val fileMetricTargetType = "FILE-METRICS"
  override val columnMetricTargetType = "COLUMNAR-METRICS"
  override val composedMetricTargetType = "COMPOSED-METRICS"
  override val checkTargetType = "CHECKS"
  override val loadCheckTargetType = "LOAD-CHECKS"
}

// Used in 1.0 version
class V1 extends V0 {
  override val trKeyName: String = "KEY"
  override val trValueName: String = "VALUE"

  override val keyFormatter: Int => String = (x: Int) => s"KEY_$x"
  override val valueFormatter: Int => String = (x: Int) => s"VALUE_$x"

  override val fileMetricTargetType = "fileMetrics"
  override val columnMetricTargetType = "columnMetrics"
  override val composedMetricTargetType = "composedMetrics"
  override val checkTargetType = "checks"
  override val loadCheckTargetType = "loadChecks"
}

// Used in 1.1 version
class V2 extends V1 {
  override val quoteModeExtractor: Config => Option[String] =
    (conf: Config) => Try(conf.getString("quoteMode")).toOption

  override val fileMetricTargetType = "FILE_METRICS"
  override val columnMetricTargetType = "COLUMN_METRICS"
  override val composedMetricTargetType = "COMPOSED_METRICS"
  override val checkTargetType = "CHECKS"
  override val loadCheckTargetType = "LOAD_CHECKS"
}
