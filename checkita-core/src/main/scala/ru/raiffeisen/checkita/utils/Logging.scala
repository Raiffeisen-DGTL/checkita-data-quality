package ru.raiffeisen.checkita.utils

import org.apache.commons.io.IOUtils.toInputStream
import org.apache.log4j.{Level, Logger}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationFactory
import org.apache.logging.log4j.core.config.{ConfigurationSource, Configurator}

import java.io.{FileInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.Properties
import scala.util.Try
import scala.collection.JavaConverters._

trait Logging {
  
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  /**
   * Gets log4j2 properties from corresponding file in the resources directory.
   * @return Input stream with log4j2 properties
   */
  private def getPropsFromResources: Option[InputStream] = Try(
    getClass.getResourceAsStream("/log4j2.properties")
  ).toOption

  /**
   * Gets log4j2 properties from corresponding file in the working directory.
   * @return Input stream with log4j2 properties
   */
  private def getPropsFromWorkDir: Option[InputStream] = Try(
    new FileInputStream("log4j2.properties")
  ).toOption

  /**
   * Default logging configuration if user-defined one wasn't provided.
   * @return Input stream with default log4j2 properties
   */
  private def getDefaultProps: InputStream = {
    val defaultProperties =
      """
        |rootLogger.level = info
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
        |# in SparkSQL with Hive support
        |logger.metastore.name = org.apache.hadoop.hive.metastore.RetryingHMSHandler
        |logger.metastore.level = fatal
        |logger.hive_functionregistry.name = org.apache.hadoop.hive.ql.exec.FunctionRegistry
        |logger.hive_functionregistry.level = error
        |
        |# Parquet related logging
        |logger.parquet.name = org.apache.parquet.CorruptStatistics
        |logger.parquet.level = error
        |logger.parquet2.name = parquet.CorruptStatistics
        |logger.parquet2.level = error
        |
        |# Suppress verbose logs from all dependencies:
        |logger.org.name = org
        |logger.org.level = warn
        |logger.com.name = com
        |logger.com.level = warn
        |logger.io.name = io
        |logger.io.level = warn
        |logger.net.name = net
        |logger.net.level = warn
        |logger.eu.name = eu
        |logger.eu.level = warn
        |logger.slick.name = slick
        |logger.slick.level = warn
        |""".stripMargin
    toInputStream(defaultProperties, StandardCharsets.UTF_8)
  }

  /**
   * Refactors log4j2 properties stream with actual root logger level provided at application start.
   * @param is Input stream with log4j2 properties
   * @param lvl Root logger level
   * @return Input stream with refactored log4j2 properties
   */
  private def refactorProperties(is: InputStream, lvl: Level): InputStream = {
    val props = new Properties()
    props.load(is)
    props.setProperty("rootLogger.level", lvl.toString)
    toInputStream(props.asScala.map{ case (k, v) => s"$k = $v"}.mkString("\n"), StandardCharsets.UTF_8)
  }

  /**
   * Initialises logger:
   *   - gets log4j2 properties with following priority:
   *     resources directory -> working directory -> default settings
   *   - updates root logger level with verbosity level defined at application start
   *   - reconfigures Logger
   * @param lvl Root logger level defined at application start
   */
  def initLogger(lvl: Level): Unit = {
    val configStream = getPropsFromResources orElse getPropsFromWorkDir getOrElse getDefaultProps
    val refactoredStream = refactorProperties(configStream, lvl)
    val configSource = new ConfigurationSource(refactoredStream)
    val context = LoggerContext.getContext(false)
    val config = new PropertiesConfigurationFactory().getConfiguration(context, configSource)
    Configurator.reconfigure(config)
  }
}
