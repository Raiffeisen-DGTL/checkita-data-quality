package ru.raiffeisen.checkita.utils

import org.apache.log4j.{Level, Logger}

trait Logging {
  
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
  
  def initLogger(lvl: Level): Unit = {
    val libLvl = if (Seq(Level.OFF, Level.FATAL, Level.ERROR, Level.WARN).contains(lvl)) lvl
    else if (Seq(Level.DEBUG, Level.TRACE, Level.ALL).contains(lvl)) Level.INFO
    else Level.WARN
    
    // always off
    Logger.getLogger("io.netty").setLevel(Level.OFF)
    Logger.getLogger("org.spark-project.jetty").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop.hdfs.KeyProviderCache").setLevel(Level.OFF)

    Logger.getLogger("org").setLevel(libLvl)
    Logger.getLogger("com").setLevel(libLvl)
    Logger.getLogger("io").setLevel(libLvl)
    Logger.getLogger("net").setLevel(libLvl)
    Logger.getLogger("eu").setLevel(libLvl)
  }
}
