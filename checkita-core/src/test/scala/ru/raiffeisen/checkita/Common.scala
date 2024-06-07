package ru.raiffeisen.checkita

import org.apache.hadoop.fs.FileSystem
import org.apache.logging.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.storage.Connections.DqStorageConnection
import ru.raiffeisen.checkita.storage.Managers.DqStorageManager
import ru.raiffeisen.checkita.utils.SparkUtils.{makeFileSystem, makeSparkSession}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import ru.raiffeisen.checkita.core.serialization.API.{encode, decode}
import ru.raiffeisen.checkita.core.serialization.SerDe

import scala.collection.compat._

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

  def checkSerDe[T](input: T)(implicit serDe: SerDe[T]): Unit = {
    val bytes = encode(input)
    val decoded = decode[T](bytes)
    decoded shouldEqual input
  }
  
  def zipT[T1, T2](t1: Seq[T1], t2: Seq[T2]): Seq[(T1, T2)] = (t1 lazyZip t2).toSeq
  
  def zipT[T1, T2, T3](t1: Seq[T1], t2: Seq[T2], t3: Seq[T3]): Seq[(T1, T2, T3)] = (t1 lazyZip t2 lazyZip t3).toSeq
  
  def zipT[T1, T2, T3, T4](t1: Seq[T1], t2: Seq[T2], t3: Seq[T3], t4: Seq[T4]): Seq[(T1, T2, T3, T4)] = {
    val seqT3 = zipT(t1, t2, t3)
    (seqT3 lazyZip t4).map{ case (tt1, tt2) => (tt1._1, tt1._2, tt1._3, tt2)}.toSeq
  }

  def zipT[T1, T2, T3, T4, T5](t1: Seq[T1], t2: Seq[T2], t3: Seq[T3], t4: Seq[T4], t5: Seq[T5]): Seq[(T1, T2, T3, T4, T5)] = {
    val seqT3 = zipT(t1, t2, t3)
    (seqT3 lazyZip t4 lazyZip t5).map{ 
      case (tt1, tt2, tt3) => (tt1._1, tt1._2, tt1._3, tt2, tt3)
    }.toSeq
  }
}
