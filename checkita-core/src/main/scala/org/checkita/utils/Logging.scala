package org.checkita.utils

import org.apache.commons.io.IOUtils.toInputStream

import org.apache.logging.{log4j => log4j2}

import java.io.{FileInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.Properties
import scala.util.Try
import scala.jdk.CollectionConverters._

/**
 * Logging trait that is mixed to the classes that do logging.
 *
 * @note Since Checkita framework works with multiple versions of Spark (2.4 and newer) then
 *       it has to support two major versions of Log4J library for logging. Log4J API has been
 *       significantly changed between version 1 and 2. Checkita framework uses Log4J2 for logging and
 *       also provide bridge to slf4j implementation from Log4J2 in order to get logs from Spark.
 * @note Checkita always looks for log4j2.properties file (which is common name convention for Log4j2)
 *       in either resources directory or working directory. Other locations of log properties file
 *       are not supported: default logging settings will be used.
 */
trait Logging {
  
  @transient lazy val log: log4j2.Logger = log4j2.LogManager.getLogger(getClass.getName)

  /**
   * Gets log4j2 properties from corresponding file in the resources directory.
   * @return Input stream with log4j2 properties
   */
  private def getPropsFromResources: Option[InputStream] = Try(
    getClass.getResourceAsStream("/log4j2.properties")
  ).toOption.flatMap(Option(_)) // take care of null input stream

  /**
   * Gets log4j2 properties from corresponding file in the working directory.
   * @return Input stream with log4j2 properties
   */
  private def getPropsFromWorkDir: Option[InputStream] = Try(
    new FileInputStream("log4j2.properties")
  ).toOption.flatMap(Option(_)) // take care of null input stream

  /**
   * Default logging configuration if user-defined one wasn't provided.
   * @return Input stream with default log4j properties
   */
  private def getDefaultProps: InputStream = {
    val defaultProperties =
      """
        |# Set everything to be logged to the console
        |# Log level for all external packages is "warn" unless defined explicitly.
        |# Log level for Checkita classes is defined explicitly further in this file.
        |rootLogger.level = warn
        |rootLogger.appenderRef.stdout.ref = console
        |
        |appender.console.type = Console
        |appender.console.name = console
        |appender.console.target = SYSTEM_OUT
        |appender.console.layout.type = PatternLayout
        |appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex
        |
        |# Settings to quiet third party logs that are too verbose
        |logger.jetty2.name = org.sparkproject.jetty.util.component.AbstractLifeCycle
        |logger.jetty2.level = error
        |
        |# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs
        |# in SparkSQL with Hive support:
        |logger.metastore.name = org.apache.hadoop.hive.metastore.RetryingHMSHandler
        |logger.metastore.level = fatal
        |logger.hive_functionregistry.name = org.apache.hadoop.hive.ql.exec.FunctionRegistry
        |logger.hive_functionregistry.level = error
        |
        |# Parquet related log level is set to error as well:
        |logger.parquet.name = org.apache.parquet.CorruptStatistics
        |logger.parquet.level = error
        |logger.parquet2.name = parquet.CorruptStatistics
        |logger.parquet2.level = error
        |
        |# EXPLICIT LOG LEVEL FOR CHECKITA CLASSES:
        |logger.checkita.name = org.checkita
        |logger.checkita.level = debug
        |""".stripMargin
    toInputStream(defaultProperties, StandardCharsets.UTF_8)
  }

  /**
   * Refactors log4j properties stream with actual Checkita logger level provided at application start.
   * @param is Input stream with log4j2 properties
   * @param lvl Root logger level
   * @return Input stream with refactored log4j2 properties
   */
  private def refactorProperties(is: InputStream, lvl: log4j2.Level): InputStream = {
    val props = new Properties()
    props.load(is)
    props.setProperty("logger.checkita.level", lvl.toString)
    toInputStream(props.asScala.map{ case (k, v) => s"$k = $v"}.mkString("\n"), StandardCharsets.UTF_8)
  }

  /**
   * Reconfigures logger settings for Log4J2
   *
   * @param is Input stream with logger properties
   */
  private def reconfigureLog4J2(is: InputStream): Unit = {
    val configSource = new log4j2.core.config.ConfigurationSource(is)
    val context = log4j2.core.LoggerContext.getContext(false)
    val config = new log4j2.core.config.properties.PropertiesConfigurationFactory()
      .getConfiguration(context, configSource)
    log4j2.core.config.Configurator.reconfigure(config)
  }

  /**
   * Initialises logger:
   *   - gets log4j properties with following priority:
   *     resources directory -> working directory -> default settings
   *   - updates root logger level with verbosity level defined at application start
   *   - reconfigures Logger
   * @param lvl Root logger level defined at application start
   */
  def initLogger(lvl: log4j2.Level): Unit = {
    val configStream = getPropsFromResources orElse getPropsFromWorkDir getOrElse getDefaultProps
    val refactoredStream = refactorProperties(configStream, lvl)
    reconfigureLog4J2(refactoredStream)
  }
}
