package ru.raiffeisen.checkita.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.input.ReaderInputStream
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.targets.HdfsTargetConfig
import ru.raiffeisen.checkita.utils.io.dbmanager.DBManagerConfig
import ru.raiffeisen.checkita.utils.io.mattermost.MMModels.MMConfig
import ru.raiffeisen.checkita.utils.mailing.MailerConfiguration
import ru.raiffeisen.checkita.utils.versions.BackCompatibilityConfiguration

import java.io.{File, FileInputStream, InputStreamReader, SequenceInputStream, StringReader}
import java.nio.charset.StandardCharsets
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.TimeZone
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}


object DQSettings {
  def getConfigOption[T: TypeTag](path: String, conf: Config): Option[T] = {
    val values = typeOf[T] match {
      case x if x =:= typeOf[String] => Try(conf.getString(path)).toOption.filter(_.nonEmpty)
      case x if x =:= typeOf[Int] => Try(conf.getInt(path)).toOption
      case x if x =:= typeOf[Boolean] => Try(conf.getBoolean(path)).toOption
      case _ => None
    }

    values.map(_.asInstanceOf[T])
  }

  def apply(commandLineOpts: DQCommandLineOptions): DQSettings = {

    val prependVars = commandLineOpts.extraAppVars
      .map(kv =>  kv._1 + ": \"" + kv._2 + "\"").mkString("", "\n", "\n")

    val appConfObj: Config = Try {
      val prependStream = new ReaderInputStream(new StringReader(prependVars), StandardCharsets.UTF_8)
      val appConfigStream = new FileInputStream(new File(commandLineOpts.appConf))
      val appConfReader = new InputStreamReader(new SequenceInputStream(prependStream, appConfigStream))
      ConfigFactory.parseReader(appConfReader).resolve()
    } match {
      case Success(config) => config
      case Failure(e) =>
        log.error(e)
        throw IllegalParameterException("Error parsing DQ application settings file")
    }

    new DQSettings(
      appConfObj.getConfig("data_quality").resolve(),
      commandLineOpts.jobConf,
      commandLineOpts.repartition,
      commandLineOpts.local,
      commandLineOpts.refDate,
      commandLineOpts.extraVars
    )
  }
}

class DQSettings(conf: Config,
                 val jobConf: String,
                 val repartition: Boolean,
                 val local: Boolean,
                 val refDate: String,
                 val extraVars: Map[String, String]) {

  private val defaultDateTimeFormat: String = "yyyy-MM-dd'T'HH:mm:ss.SSS"

  val tz: ZoneId = Try(
    TimeZone.getTimeZone(conf.getString("timeZone")).toZoneId
  ).getOrElse(ZoneId.systemDefault())

  val executionDateFormat: DateTimeFormatter = Try(
    DateTimeFormatter.ofPattern(conf.getString("executionDateFormat")).withZone(tz)
  ).getOrElse(
    DateTimeFormatter.ofPattern(defaultDateTimeFormat).withZone(tz)
  )
  val referenceDateFormat: DateTimeFormatter = Try(
    DateTimeFormatter.ofPattern(conf.getString("referenceDateFormat")).withZone(tz)
  ).getOrElse(
    DateTimeFormatter.ofPattern(defaultDateTimeFormat).withZone(tz)
  )

  val executionDate: ZonedDateTime = ZonedDateTime.now(tz)

  val referenceDate: ZonedDateTime =
    if (refDate.isEmpty) executionDate
    else {
      val temporalAccessor = referenceDateFormat.parse(refDate)

      val tryDateTime = Try(LocalDateTime.from(temporalAccessor).atZone(tz))
      val tryDate = Try(LocalDate.from(temporalAccessor).atStartOfDay(tz))
      val tryTime = Try(LocalTime.from(temporalAccessor).atDate(LocalDate.now).atZone(tz))

      tryDateTime orElse tryDate orElse tryTime getOrElse executionDate
    }

  val referenceDateString: String = referenceDate.format(referenceDateFormat)
  val executionDateString: String = executionDate.format(executionDateFormat)

  /* application.conf parameters */
  val appName: String = Try(conf.getString("application_name")).toOption.getOrElse("Checkita Data Quality")
  val runConfVersion: String = Try(conf.getString("run_configuration_version")).toOption.getOrElse("0.9.0")

  // External sources
  val s3Bucket: Option[String] = DQSettings.getConfigOption[String]("s3_bucket", conf)
  val sqlDir: Option[String] = DQSettings.getConfigOption[String]("sql_warehouse_path", conf)
  val hbaseHost: Option[String] = DQSettings.getConfigOption[String]("hbase_host", conf)

  // Temp files parameters
  val localTmpPath: Option[String] = DQSettings.getConfigOption[String]("tmp_files_management.local_fs_path", conf)
  val hdfsTmpPath: Option[String] = DQSettings.getConfigOption[String]("tmp_files_management.hdfs_path", conf)
  val tmpFileDelimiter: Option[String] = DQSettings.getConfigOption[String]("tmp_files_management.delimiter", conf)

  // Error managements parameters
  val errorFolderPath: Option[String] = DQSettings.getConfigOption[String]("metric_error_management.dump_directory_path", conf)
  val errorDumpSize: Int =  DQSettings.getConfigOption[Int]("metric_error_management.dump_size", conf).getOrElse(10000)
  val errorDumpEmptyFile: Boolean =  DQSettings.getConfigOption[Boolean]("metric_error_management.empty_file", conf).getOrElse(false)

  val errorFileFormat: String = DQSettings.getConfigOption[String]("metric_error_management.file_config.format", conf).getOrElse("csv")
  val errorFileDelimiter: Option[String] = DQSettings.getConfigOption[String]("metric_error_management.file_config.delimiter", conf)
  val errorFileQuote: Option[String] = DQSettings.getConfigOption[String]("metric_error_management.file_config.quote", conf)
  val errorFileEscape: Option[String] = DQSettings.getConfigOption[String]("metric_error_management.file_config.escape", conf)
  val errorFileQuoteMode: Option[String] = DQSettings.getConfigOption[String]("metric_error_management.file_config.quote_mode", conf)

  // Virtual sources parameters
  val vsDumpConfig: Option[HdfsTargetConfig] = Try {
    val obj: Config = conf.getConfig("virtual_sources_management")
    val path = DQSettings.getConfigOption[String]("dump_directory_path", obj).get
    val fileFormat = DQSettings.getConfigOption[String]("file_format", obj).get
    val delimiter = DQSettings.getConfigOption[String]("delimiter", obj)
    HdfsTargetConfig.apply("vsd", fileFormat, path, delimiter)
  }.toOption

  val mailingMode: Option[String] = DQSettings.getConfigOption[String]("mailing.mode", conf)
  val scriptPath: Option[String] = DQSettings.getConfigOption[String]("mailing.mail_script_path", conf)
  val mailingConfig: Option[MailerConfiguration] = {
    val monfig = Try(conf.getConfig("mailing.config")).toOption
    monfig match {
      case Some(c) => Try(new MailerConfiguration(c)).toOption
      case None    => None
    }
  }
  val notifications: Boolean = DQSettings.getConfigOption[Boolean]("mailing.notifications", conf).getOrElse(false)

  val mmConfig: Option[MMConfig] = Try(conf.getConfig("mattermost")) match {
    case Success(c) => Try(MMConfig(c)).toOption
    case Failure(_) => None
  }

  // sparkConfig is moved to setting in order to use its parameters for history DataBase config creation.
  val sparkConf: SparkConf = new SparkConf()
    .setAppName(appName)
    .set("spark.sql.parquet.compression.codec", "snappy")
    .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .set("spark.sql.orc.enabled", "true")
    .set("spark.sql.hive.convertMetastoreOrc", "true")
    .set("spark.sql.files.ignoreMissingFiles", "true")
    .set("spark.hadoop.hive.exec.dynamic.partition", "true")
    .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    .set("spark.hadoop.hive.merge.mapfiles", "true")
    .set("spark.hadoop.hive.merge.mapredfiles", "true")
    .set("spark.hadoop.hive.merge.tezfiles", "true")
    .set("spark.hadoop.hive.merge.smallfiles.avgsize", "128000000")
    .set("spark.hadoop.hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager")
    .set("spark.hadoop.hive.vectorized.execution.enabled", "true")

  if (local) sparkConf.setMaster("local[*]")
  if (s3Bucket.isDefined)
    sparkConf.set("spark.sql.warehouse.dir",  s3Bucket.get + "/data_quality_output/spark/warehouse")
  // HiveContext is deprecated for Spark 2.0 and newer.
  // Replacing hive warehouse config property with sql warehouse.
  if (sqlDir.isDefined)
    sparkConf.set("spark.sql.warehouse.dir", sqlDir.get)
  else
    sparkConf.set("spark.sql.warehouse.dir", s"file://${FileUtils.getUserDirectoryPath}/spark-warehouse")
  if (hbaseHost.isDefined) sparkConf.set("spark.hbase.host", hbaseHost.get)

  val resStorage: Option[DBManagerConfig] = conf.getString("storage.type") match {
    case "APP_CONFIG"   => Some(new DBManagerConfig(conf.getConfig("storage.config")))
    case "SPARK_CONFIG" => Some(DBManagerConfig(
      id = "",
      subtype = sparkConf.get("spark.jdbc.db_type").toUpperCase,
      host = Try(sparkConf.get("spark.jdbc.host")).toOption,
      path = Try(sparkConf.get("spark.jdbc.path")).toOption,
      port = Try(sparkConf.get("spark.jdbc.port")).toOption,
      service = Try(sparkConf.get("spark.jdbc.service")).toOption,
      user = Try(sparkConf.get("spark.jdbc.login")).toOption,
      password = Try(sparkConf.get("spark.jdbc.password")).toOption,
      schema = Try(sparkConf.get("spark.jdbc.db_schema")).toOption
    ))
    case "NONE" => None
    case x      => throw IllegalParameterException(x)
  }

  val aggregatedKafkaOutput: Boolean = Try(conf.getBoolean("aggregatedKafkaOutput")).getOrElse(false)
  val enableSharedSparkContext: Boolean = Try(conf.getBoolean("enableSharedSparkContext")).getOrElse(false)

  // Internal values
  val backComp: BackCompatibilityConfiguration = versions.BackCompatibilityConfiguration.getConfig(runConfVersion)
  val stageNum: Int = 9

  def logThis()(implicit log: Logger): Unit = {
    log.info(s"[CONF] General application configuration:")
    log.info(s"[CONF] - Run configuration version: ${this.runConfVersion}")
    log.info(s"[CONF] - Local time zone: ${this.tz.toString}")
    log.info(s"[CONF] - External source parameters:")
    log.info(s"[CONF]   - S3 Bucket: ${this.s3Bucket}")
    log.info(s"[CONF]   - HBase host: ${this.hbaseHost}")
    log.info(s"[CONF]   - SQL warehouse path: ${this.sqlDir}")
    log.info(s"[CONF] - Metric error management configuration:")
    log.info(s"[CONF]   - Dump directory path path: ${this.errorFolderPath}")
    log.info(s"[CONF]   - Dump size: ${this.errorDumpSize}")
    log.info(s"[CONF]   - Empty file: ${this.errorDumpEmptyFile}")
    log.info(s"[CONF] - Temporary files management configuration:")
    log.info(s"[CONF]   - Local FS path: ${this.localTmpPath}")
    log.info(s"[CONF]   - HDFS path: ${this.hdfsTmpPath}")
    log.info(s"[CONF] - Virtual sources management configuration:")
    log.info(s"[CONF]   - Dump path: ${this.vsDumpConfig.map(_.path)}")
    log.info(s"[CONF]   - File format: ${this.vsDumpConfig.map(_.fileFormat)}")
    log.info(s"[CONF]   - Delimiter: ${this.vsDumpConfig.map(_.delimiter)}")
    log.info(s"[CONF] - Storage configuration:")
    log.info(s"[CONF]   - Mode: ${conf.getString("storage.type")}")
    log.info(s"[CONF] - Mailing configuration:")
    log.info(s"[CONF]   - Mode: ${this.mailingMode}")
    log.info(s"[CONF]   - Script path: ${this.scriptPath}")
    log.info(s"[CONF]   - Notifications: ${this.notifications}")
  }

}
