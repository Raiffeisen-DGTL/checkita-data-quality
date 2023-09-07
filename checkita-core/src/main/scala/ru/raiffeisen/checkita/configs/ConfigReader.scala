package ru.raiffeisen.checkita.configs

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.input.ReaderInputStream
import org.apache.spark.sql.types.StringType
import ru.raiffeisen.checkita.checks.Check
import ru.raiffeisen.checkita.checks.load.LoadCheck
import ru.raiffeisen.checkita.checks.sql.SQLCheck
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.metrics.{ComposedMetric, Metric}
import ru.raiffeisen.checkita.postprocessors.BasicPostprocessor
import ru.raiffeisen.checkita.sources.{DatabaseConfig, KafkaConfig, SourceConfig, VirtualFile}
import ru.raiffeisen.checkita.targets.TargetConfig
import ru.raiffeisen.checkita.utils.{DQSettings, Logging, mapToDataType}
import ru.raiffeisen.checkita.utils.io.dbmanager.DBManager

import java.io.{File, FileInputStream, InputStreamReader, SequenceInputStream, StringReader}
import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._

object ConfigReader {
  def apply(configPath: String)(implicit resultsWriter: DBManager, settings: DQSettings): ConfigReader = {
    if (settings.runConfVersion.startsWith("0.")) new ConfigReader0x(settings.jobConf)
    else if (settings.runConfVersion.startsWith("1.")) new ConfigReader1x(settings.jobConf)
    else throw IllegalParameterException(s"Unsupported configuration file versions: ${settings.runConfVersion}")
  }
}

abstract class ConfigReader(configPath: String)(implicit settings: DQSettings) extends Logging {

  // collect variables to be prepended to config file
  protected val prependString: String = {
    val prependVars = Seq(
      "referenceDateTime: \"" + settings.referenceDateString + "\"",
      "executionDateTime: \"" + settings.executionDateString + "\""
    ) ++ settings.extraVars.map(kv =>  kv._1 + ": \"" + kv._2 + "\"")
    prependVars.mkString("", "\n", "\n")
  }

  // load conf file
  protected val configObj: Config = Try {
    val prependStream = new ReaderInputStream(new StringReader(prependString), StandardCharsets.UTF_8)
    val configStream = new FileInputStream(new File(configPath))
    val configReader = new InputStreamReader(new SequenceInputStream(prependStream, configStream))
    ConfigFactory.parseReader(configReader).resolve()
  } match {
    case Success(config) => config
    case Failure(e) =>
      log.error(e)
      throw IllegalParameterException("Error parsing DQ configuration file")
  }

  val jobId: String = Try(configObj.getString("jobId")).getOrElse(
    throw IllegalParameterException("jobId is not set"))

  /**
   * Config PARSER
   *  - DQ Databases
   *  - DQ MessageBrokers
   *  - DQ Sources & Virtual Sources
   *  - DQ Metrics
   *  - DQ ComposedMetrics
   *  - DQ Checks & SQL Checks
   *  - DQ Targets
   *  - DQ PostProcessors
   */

  /**
   * Config object to be parsed:
   */
  val dbConfigMap: Map[String, DatabaseConfig] = getDatabasesById
  val brokersConfigMap: Map[String, KafkaConfig] = getBrokersById
  val sourcesConfigMap: Map[String, SourceConfig] = getSourcesById
  val virtualSourcesConfigMap: Map[String, VirtualFile] = getVirtualSourcesById
  lazy val loadChecksMap: Map[String, Seq[LoadCheck]] = getLoadChecks
  val metricsBySourceList: List[(String, Metric)] = getMetricsBySource
  val metricsByChecksList: List[(Check, String)] = getMetricByCheck
  lazy val composedMetrics: List[ComposedMetric] = getComposedMetrics
  lazy val targetsConfigMap: Map[String, List[TargetConfig]] = getTargetsConfigMap
  lazy val sqlChecksList: List[SQLCheck] = getSqlChecks
  lazy val getPostprocessors: Seq[BasicPostprocessor] = getPostprocessorConfig

  lazy val metricsBySourceMap: Map[String, List[Metric]] = metricsBySourceList.groupBy(_._1).mapValues(_.map(_._2))
  lazy val metricsByCheckMap: Map[Check, List[String]] = metricsByChecksList.groupBy(_._1).mapValues(_.map(_._2))

  /**
   * Abstract parsing methods to implement in inherited classes
   * @return
   */
  protected def getDatabasesById: Map[String, DatabaseConfig]
  protected def getBrokersById: Map[String, KafkaConfig]
  protected def getSourcesById: Map[String, SourceConfig]
  protected def getVirtualSourcesById: Map[String, VirtualFile]
  protected def getLoadChecks: Map[String, Seq[LoadCheck]]
  protected def getMetricsBySource: List[(String, Metric)]
  protected def getMetricByCheck: List[(Check, String)]
  protected def getComposedMetrics: List[ComposedMetric]
  protected def getTargetsConfigMap: Map[String, List[TargetConfig]]
  protected def getSqlChecks: List[SQLCheck]
  protected def getPostprocessorConfig: Seq[BasicPostprocessor]

  protected def schemaParser(schemaType: String, conf: Config): List[GenStructColumn] =
    schemaType match {
      case SchemaFormats.delimited =>
        conf.getObjectList(ConfigSourceParameters.schema).toList.map { col =>
          val colConfig = col.toConfig
          StructColumn(
            colConfig.getString(SchemaParameters.colName),
            mapToDataType(colConfig.getString(SchemaParameters.colType))
          )
        }
      case SchemaFormats.fixedFull =>
        conf.getObjectList(ConfigSourceParameters.schema).toList.map { col =>
          val colConfig = col.toConfig
          StructFixedColumn(
            colConfig.getString(SchemaParameters.colName),
            mapToDataType(colConfig.getString(SchemaParameters.colType)),
            colConfig.getInt(SchemaParameters.colLength)
          )
        }
      case SchemaFormats.fixedShort =>
        conf.getStringList(ConfigSourceParameters.shortSchema).toList.map { col =>
          val name :: length :: _ = col.split(":").toList
          StructFixedColumn(name, StringType, length.toInt)
        }
    }
}