package ru.raiffeisen.checkita

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.storage.Connections.DqStorageConnection
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager
import ru.raiffeisen.checkita.utils.SparkUtils.{makeFileSystem, makeSparkSession}

object Common {
  implicit val jobId: String = "earthquakes_base_job"
  implicit val settings: AppSettings = AppSettings.build(
    getClass.getResource("/test_application.conf").getPath,
    Some("2023-09-25"),
    isLocal = true,
    isShared = false,
    doMigration = false,
    prependVariables = "",  // Map("is_test" -> "true").map{case (k, v) => k + ": \"" + v + "\""}.mkString("", "\n", "\n")
    logLvl = Level.INFO
  ) match {
    case Right(settings) => settings
    case Left(e) => throw new RuntimeException(e.mkString("\n"))
  }
  implicit val spark: SparkSession = makeSparkSession(settings.sparkConf, None) match {
    case Right(spark) => spark
    case Left(e) => throw new RuntimeException(e.mkString("\n"))
  }
  spark.sparkContext.setLogLevel("WARN")
  implicit val fs: FileSystem = makeFileSystem(spark) match {
    case Right(fs) => fs
    case Left(e) => throw new RuntimeException(e.mkString("\n"))
  }
  implicit val storage: Option[DqStorageManager] = settings.storageConfig.map(config =>
    DqStorageManager(DqStorageConnection(config))
  )
}
