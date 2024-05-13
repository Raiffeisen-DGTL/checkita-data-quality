package ru.raiffeisen.checkita

import org.apache.hadoop.fs.FileSystem
import org.apache.logging.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.storage.Connections.DqStorageConnection
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager
import ru.raiffeisen.checkita.utils.SparkUtils.{makeFileSystem, makeSparkSession}

object Common {
  implicit val jobId: String = "earthquakes_base_job"
  implicit val dumpSize: Int = 1000
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
  val sc: SparkContext = spark.sparkContext
  spark.sparkContext.setLogLevel("WARN")
  implicit val fs: FileSystem = makeFileSystem(spark) match {
    case Right(fs) => fs
    case Left(e) => throw new RuntimeException(e.mkString("\n"))
  }
  implicit val storage: Option[DqStorageManager] = settings.storageConfig.map(config =>
    DqStorageManager(DqStorageConnection(config))
  )

  implicit class Tuple4Ops[T1, T2, T3, T4](value: (Seq[T1], Seq[T2], Seq[T3], Seq[T4])) {
    def zipped: Seq[(T1, T2, T3, T4)] = (value._1, value._2, value._3).zipped.toSeq.zip(value._4).map {
      case (t, v) => (t._1, t._2, t._3, v)
    }
  }

  implicit class Tuple5Ops[T1, T2, T3, T4, T5](value: (Seq[T1], Seq[T2], Seq[T3], Seq[T4], Seq[T5])) {
    def zipped: Seq[(T1, T2, T3, T4, T5)] = (
      (value._1, value._2, value._3).zipped.toSeq,
      value._4,
      value._5
    ).zipped.toSeq.map {
      case (t, v4, v5) => (t._1, t._2, t._3, v4, v5)
    }
  }
}
