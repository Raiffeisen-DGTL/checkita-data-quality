package org.checkita.api.server

import org.apache.logging.log4j.Level
import scopt.OptionParser


/**
 * Application command line arguments for API Server
 *
 * @param appConf Path to application settings file.
 * @param migrate Flag indication whether DQ storage database migration needs to be run prior result saving.
 * @param extraVars Extra variables to be used in configuration files.
 * @param logLevel Application logging level
 */
case class CommandLineOptions(
                               appConf: String = CommandLineOptions.DEFAULT_APP_CONF,
                               migrate: Boolean = false,
                               extraVars: Map[String, String] = Map.empty,
                               logLevel: Level = Level.INFO
                             )

object CommandLineOptions {
  
  lazy val DEFAULT_APP_CONF: String = getClass.getResource("/application.conf").getPath
  
  /** Command line arguments parser */
  def parser(): OptionParser[CommandLineOptions] =
    new OptionParser[CommandLineOptions]("CheckitaDataQuality") {
      opt[String]('a', "app-config")
        .optional()
        .action((x, c) => c.copy(appConf = x))
        .text("Path to application configuration file")

      opt[Unit]('m', "migrate")
        .optional()
        .action((_, c) => c.copy(migrate = true))
        .text("Specifies whether the storage database migration needs to be performed on startup")

      opt[Map[String, String]]('e', "extra-vars")
        .optional()
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(extraVars = x))
        .text("Extra variables to be used in both application and job configuration files")

      opt[String]('v', "verbosity")
        .optional()
        .action((x, c) => c.copy(logLevel = Level.toLevel(x, Level.INFO)))
        .text("Application log verbosity (default = info)")
    }
}