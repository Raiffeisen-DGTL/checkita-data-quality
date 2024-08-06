package org.checkita.dqf.apps.cli

import org.apache.logging.log4j.Level
import scopt.OptionParser

/**
 * Application command line arguments
 * @param appConf Path to application settings file.
 * @param jobConf Sequence of paths to job configuration files.
 * @param refDate Reference date string.
 * @param local Flag indication that application should be run in local mode.
 * @param shared Flag indicating that application is running using shared spark context.
 *               For such applications the spark session wont be stopped after job completion.
 * @param migrate Flag indication whether DQ storage database migration needs to be run prior result saving.
 * @param extraVars Extra variables to be used in configuration files.
 * @param logLevel Application logging level
 */
case class CommandLineOptions(
                               appConf: String,
                               jobConf: Seq[String],
                               refDate: Option[String] = None,
                               local: Boolean = false,
                               shared: Boolean = false,
                               migrate: Boolean = false,
                               extraVars: Map[String, String] = Map.empty,
                               logLevel: Level = Level.INFO
                             )

object CommandLineOptions {
  /** Command line arguments parser */
  def parser(): OptionParser[CommandLineOptions] = 
    new OptionParser[CommandLineOptions]("CheckitaDataQuality") {
      opt[String]('a', "app-config")
        .required()
        .action((x, c) => c.copy(appConf = x))
        .text("Path to application configuration file")
  
      opt[Seq[String]]('j', "job-config")
        .required()
        .valueName("<file1>,<file2>...")
        .action((x, c) => c.copy(jobConf = x))
        .text("Sequence of paths to run job configuration files")
  
      opt[String]('d', "date")
        .optional()
        .action((x, c) => c.copy(refDate = Some(x)))
        .text(
          "Reference date at which the DataQuality job will be performed " +
          "(format must conform to one specified in application settings file)"
        )
  
      opt[Unit]('l', "local")
        .optional()
        .action((_, c) => c.copy(local = true))
        .text("Specifies whether the application should be executed in local mode")
      
      opt[Unit]('s', "shared")
        .optional()
        .action((_, c) => c.copy(local = true))
        .text("Specifies whether the application is executed using shared spark context")

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