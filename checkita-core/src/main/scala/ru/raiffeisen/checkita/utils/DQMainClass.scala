package ru.raiffeisen.checkita.utils

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.utils.io.dbmanager.{DBManager, FileDBManager, HiveDBManager, JdbcDBManager}

import java.util.Locale

trait DQMainClass { this: DQSparkContext with Logging =>

  private def initLogger(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("io.netty").setLevel(Level.OFF)
    Logger.getLogger("org.spark-project.jetty").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop.hdfs.KeyProviderCache").setLevel(Level.OFF)
  }

  private def makeFileSystem(settings: DQSettings, sc: SparkContext): FileSystem = {

    //    if (sc.isLocal) FileSystem.getLocal(sc.hadoopConfiguration)
    if (sc.hadoopConfiguration.get("fs.defaultFS") == "file:///") FileSystem.getLocal(sc.hadoopConfiguration)
    else {
      if (settings.s3Bucket.isDefined) {
        sc.hadoopConfiguration.set("fs.defaultFS", settings.s3Bucket.get)
        sc.hadoopConfiguration.set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      }
      FileSystem.get(sc.hadoopConfiguration)
    }
  }

  protected def body()(implicit fs: FileSystem,
                       sparkContext: SparkContext,
                       sparkSession: SparkSession,
                       resultsWriter: DBManager,
                       settings: DQSettings): Boolean

  def preMessage(task: String): Unit = {
    log.warn("************************************************************************")
    log.warn(s"               Starting execution of task $task")
    log.warn("************************************************************************")
  }

  def postMessage(task: String): Unit = {
    log.warn("************************************************************************")
    log.warn(s"               Finishing execution of task $task")
    log.warn("************************************************************************")
  }

  def main(args: Array[String]): Unit = {
    // set to avoid casting problems in metric result name generation
    Locale.setDefault(Locale.ENGLISH)
    initLogger()

    DQCommandLineOptions.parser().parse(args, DQCommandLineOptions("","")) match {
      case Some(commandLineOptions) =>
        // Load our own config values from the default location, application.conf
        val settings = DQSettings(commandLineOptions)
        val sparkSession = makeSparkSession(settings)
        val sparkContext = makeSparkContext(sparkSession)
        val fs = makeFileSystem(settings, sparkContext)

        // Suppress .crc files creation:
        fs.setWriteChecksum(false)

        settings.logThis()(log)
        log.info(s"[CONF] - Spark Version: ${sparkSession.version}")
        log.info(s"[CONF] - Default Spark File System: ${sparkContext.hadoopConfiguration.get("fs.defaultFS")}")

        val historyDatabase: DBManager = settings.resStorage.map(_.subtype.toUpperCase) match {
          case Some("POSTGRESQL") | Some("ORACLE") | Some("SQLITE") => new JdbcDBManager(settings)
          case Some("HIVE") => new HiveDBManager(settings, sparkSession)
          case Some("FILE") => new FileDBManager(settings.resStorage, sparkSession, fs)
          case Some(s) => throw new IllegalArgumentException(s"Unknown history storage type: $s")
          case None => throw new IllegalArgumentException("History storage subtype is not defined.")
        }

        // Starting application body
        preMessage(s"{${settings.appName}}")
        val startTime = System.currentTimeMillis()
        body()(fs, sparkContext, sparkSession, historyDatabase, settings)
        postMessage(s"{${settings.appName}}")

        log.info(s"Execution finished in [${(System.currentTimeMillis() - startTime) / 60000}] min(s)")
        log.info("Closing application...")


        historyDatabase match {
          case manager: JdbcDBManager => manager.closeConnection()
          case _ => ()
        }

        if (!settings.enableSharedSparkContext) {
          sparkContext.stop()
          sparkSession.stop()
          log.info("Spark session was terminated.")
        }

        log.info("DQ Job is finished.")

      case None =>
        log.error("Wrong parameters provided")
        throw new Exception("Wrong parameters provided")
    }
  }
}