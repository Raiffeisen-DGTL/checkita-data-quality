package ru.raiffeisen.checkita.utils

import scopt.OptionParser

case class DQCommandLineOptions(appConf: String,
                                jobConf: String,
                                refDate: String = "",
                                repartition: Boolean = false,
                                local: Boolean = false,
                                extraVars: Map[String, String] = Map.empty,
                                extraAppVars: Map[String, String] = Map.empty)

object DQCommandLineOptions {

  def parser(): OptionParser[DQCommandLineOptions] =
    new OptionParser[DQCommandLineOptions]("DataQuality") {

      opt[String]('a', "application-conf") required () action { (x, c) =>
        c.copy(appConf = x)
      } text "Path to application settings file"

      opt[String]('c', "job-conf") required () action { (x, c) =>
        c.copy(jobConf = x)
      } text "Path to run configuration file"

      opt[String]('d', "reference-date") optional () action { (x, c) =>
        c.copy(refDate = x)
      } text ("Indicates the date at which the DataQuality checks will be performed " +
        "(format must conform to one specified in application settings file)")

      opt[Unit]('r', "repartition") optional () action { (_, c) =>
        c.copy(repartition = true)
      } text "Specifies whether the application is repartitioning the input data"

      opt[Unit]('l', "local") optional () action { (_, c) =>
        c.copy(local = true)
      } text "Specifies whether the application is operating in local mode"

      opt[Map[String, String]]('e', "extra-vars") valueName("k1=v1,k2=v2...") optional () action { (x, c) =>
        c.copy(extraVars = x)
      } text "Extra variables to be used in DQ configuration file"

      opt[Map[String, String]]('v', "extra-application-vars") valueName("k1=v1,k2=v2...") optional () action { (x, c) =>
        c.copy(extraAppVars = x)
      } text "Extra variables to be used in DQ application settings file"
    }
}


