package ru.raiffeisen.checkita.configs

import com.typesafe.config.{Config, ConfigObject, ConfigValue}
import org.apache.spark.storage.StorageLevel
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import ru.raiffeisen.checkita.checks.Check
import ru.raiffeisen.checkita.checks.load.{LoadCheck, LoadCheckEnum}
import ru.raiffeisen.checkita.checks.snapshot._
import ru.raiffeisen.checkita.checks.sql.SQLCheck
import ru.raiffeisen.checkita.checks.trend._
import ru.raiffeisen.checkita.exceptions.{IllegalParameterException, MissingParameterInException}
import ru.raiffeisen.checkita.metrics.{ColumnMetric, ComposedMetric, FileMetric, Metric, MetricResult}
import ru.raiffeisen.checkita.postprocessors.{BasicPostprocessor, PostprocessorType}
import ru.raiffeisen.checkita.sources._
import ru.raiffeisen.checkita.targets._
import ru.raiffeisen.checkita.utils.{DQSettings, generateMetricSubId}
import ru.raiffeisen.checkita.utils.io.dbmanager.DBManager

import java.util.Map.Entry
import scala.util.Try
import scala.collection.JavaConversions._

class ConfigReader0x(configPath: String)(implicit resultsWriter: DBManager, settings: DQSettings)
  extends ConfigReader(configPath: String) {

  /**
   * Parses sources from configuration file
   * @return Map of (source_id, source_config)
   */
  override protected def getSourcesById: Map[String, SourceConfig] = {
    val sourcesList: List[ConfigObject] =
      configObj.getObjectList("Sources").toList

    def parseDateFromPath(path: String): String = {

      val length = path.length
      val sub    = path.substring(length - 8, length) // YYYYMMDD

      val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
      val outputFormat                 = DateTimeFormat.forPattern("yyyy-MM-dd")

      formatter.parseDateTime(sub).toString(outputFormat)
    }

    sourcesList.map { src =>
      val generalConfig = src.toConfig
      val keyFieldList: scala.Seq[String] =
        if (generalConfig.hasPath("keyFields"))
          generalConfig.getStringList("keyFields")
        else Seq.empty
      generalConfig.getString("type") match {
        case "HDFS" =>
          val id       = generalConfig.getString("id")
          val path     = generalConfig.getString("path")
          val fileType = generalConfig.getString("fileType")

          val header = Try(generalConfig.getBoolean("header")).getOrElse(false)

          val delimiter: Option[String] = settings.backComp.delimiterExtractor(generalConfig)
          val quote     = Try(generalConfig.getString("quote")).toOption
          val escape    = Try(generalConfig.getString("escape")).toOption

          val date = Try(parseDateFromPath(path))
            .getOrElse(Try(generalConfig.getString("date")).getOrElse(settings.referenceDateString))

          val schema = generalConfig.getString("fileType") match {
            case "fixed" =>
              val schemaType =
                if (generalConfig.hasPath("schema")) SchemaFormats.fixedFull
                else if (generalConfig.hasPath("fieldLengths")) SchemaFormats.fixedShort
                else throw IllegalParameterException(s"No schema found for fixed-length file $id.")

              Some(schemaParser(schemaType, generalConfig))
            case "delimited" =>
              if (header) None
              else if (generalConfig.hasPath("schema"))
                Some(schemaParser(SchemaFormats.delimited, generalConfig))
              else throw IllegalParameterException(s"No schema found for delimited file $id.")
            case "avro"      => Try(generalConfig.getString("schema")).toOption
            case "parquet"   => None
            case "orc"       => None
            case x           => throw IllegalParameterException(x)
          }

          id -> HdfsFile(id, path, fileType, header, delimiter, quote, escape, schema, keyFieldList)
        // TODO: Do we need this source type?
        case "OUTPUT" =>
          val path = generalConfig.getString("path")
          "OUTPUT" -> OutputFile("OUTPUT", path, "csv", Some("|"), true)
        case "TABLE" =>
          val id         = generalConfig.getString("id")
          val databaseId = generalConfig.getString("database")
          val table      = generalConfig.getString("table")
          val username   = Try { generalConfig.getString("username") }.toOption
          val password   = Try { generalConfig.getString("password") }.toOption

          id -> TableConfig(id, databaseId, table, username, password, keyFieldList)
        case "HIVE" =>
          val id    = generalConfig.getString("id")
          val query = generalConfig.getString("query")

          id -> HiveTableConfig(id, query, keyFieldList)
        case "HBASE" =>
          val id        = generalConfig.getString("id")
          val table     = generalConfig.getString("table")
          val hbColumns = generalConfig.getStringList("columns")
          id -> HBaseSrcConfig(id, table, hbColumns)
        case x => throw IllegalParameterException(x)
      }
    }.toMap
  }

  override protected def getVirtualSourcesById: Map[String, VirtualFile] = {

    if (configObj.hasPath("VirtualSources")) {
      val sourcesList: List[ConfigObject] =
        configObj.getObjectList("VirtualSources").toList

      sourcesList.map { src =>
        val generalConfig = src.toConfig
        val keyFieldList: scala.Seq[String] =
          if (generalConfig.hasPath("keyFields"))
            generalConfig.getStringList("keyFields")
          else Seq()

        val parentSourcesIds: scala.Seq[String] =
          generalConfig.getStringList("parentSources")

        val isSave: Boolean = Try { generalConfig.getBoolean("save") }.toOption.getOrElse(false)
        val id = generalConfig.getString("id")
        generalConfig.getString("type") match {
          case "FILTER-SQL" =>
            val sql = generalConfig.getString("sql")
            val persist: Option[StorageLevel] =
              if (generalConfig.hasPath("persist"))
                Some(StorageLevel.fromString(generalConfig.getString("persist")))
              else None
            id -> VirtualFileSelect(id, parentSourcesIds, sql, keyFieldList, None, persist) // broken compatibility
          case "JOIN-SQL" =>
            val sql = generalConfig.getString("sql")
            val persist: Option[StorageLevel] =
              if (generalConfig.hasPath("persist"))
                Some(StorageLevel.fromString(generalConfig.getString("persist")))
              else None
            id -> VirtualFileJoinSql(id, parentSourcesIds, sql, keyFieldList, None, persist) // broken compatibility
          case "JOIN" =>
            val joiningColumns = generalConfig.getStringList("joiningColumns")
            val joinType       = generalConfig.getString("joinType")
            val persist: Option[StorageLevel] =
              if (generalConfig.hasPath("persist"))
                Some(StorageLevel.fromString(generalConfig.getString("persist")))
              else None
            id -> VirtualFileJoin(id, parentSourcesIds, joiningColumns, joinType, keyFieldList, None, persist) // broken compatibility

          case x => throw IllegalParameterException(x)
        }
      }.toMap
    } else {
      Map.empty
    }

  }

  /**
   * Parses databases from configuration file
   * @return Map of (db_id, db_config)
   */
  override protected def getDatabasesById: Map[String, DatabaseConfig] = {
    val dbList: List[ConfigObject] = Try {
      configObj.getObjectList("Databases").toList
    }.getOrElse(List.empty)

    dbList.map { db =>
      val generalConfig = db.toConfig
      val outerConfig   = generalConfig.getConfig("config")
      val id            = generalConfig.getString("id")
      val subtype       = generalConfig.getString("subtype")
      val host          = outerConfig.getString("host")

      val port     = Try(outerConfig.getString("port")).toOption
      val service  = Try(outerConfig.getString("service")).toOption
      val user     = Try(outerConfig.getString("user")).toOption
      val password = Try(outerConfig.getString("password")).toOption
      val schema   = Try(outerConfig.getString("schema")).toOption

      id -> DatabaseConfig(id, subtype, host, port, service, user, password, schema)
    }.toMap
  }

  /**
   * Parses metrics from configuration file
   * @return Map of (file_id, metric)
   */
  override protected def getMetricsBySource: List[(String, Metric)] = {
    val metricsList: List[ConfigObject] =
      configObj.getObjectList("Metrics").toList

    val metricFileList: List[(String, Metric)] = metricsList.map { mts =>
      val outerConf  = mts.toConfig
      val metricType = outerConf.getString("type")
      val id         = outerConf.getString("id")
      val name       = outerConf.getString("name")
      val descr      = outerConf.getString("description")
      val intConfig  = outerConf.getObject("config").toConfig
      val params     = getParams(intConfig)
      val applyFile  = intConfig.getString("file")

      metricType match {
        case "COLUMN" =>
          val applyColumns = intConfig.getStringList("columns")
          val columnPos: scala.Seq[Int] =
            Try(intConfig.getIntList("positions").toSeq.map(_.toInt)).toOption
              .getOrElse(Seq.empty)
          applyFile -> ColumnMetric(id, name, descr, applyFile, applyColumns, params, columnPos)
        case "FILE" =>
          applyFile -> FileMetric(id, name, descr, applyFile, params)
        case x => throw IllegalParameterException(x)
      }
    }

    metricFileList
  }

  /**
   * Parses sql checks from configuration file
   * @return List of SQL checks
   */
  override protected def getSqlChecks: List[SQLCheck] = {
    val checkList: List[ConfigObject] = configObj.getObjectList("Checks").toList
    val sqlChecks = checkList.flatMap { check =>
      val outerConf = check.toConfig
      val checkType = outerConf.getString("type")
      checkType.toUpperCase match {
        case "SQL" =>
          val description =
            Try {
              outerConf.getString("description")
            }.toOption
          val subtype   = outerConf.getString("subtype")
          val innerConf = outerConf.getConfig("config")
          val source    = innerConf.getString("source")
          val query     = innerConf.getString("query")
          val id = Try {
            outerConf.getString("id")
          }.toOption.getOrElse(subtype + ":" + checkType + ":" + source + ":" + query.hashCode)
          val date = Try {
            outerConf.getString("date")
          }.toOption.getOrElse(settings.referenceDateString)
          val sourceConf: DatabaseConfig = this.dbConfigMap(source) // "ORACLE"
          List(SQLCheck(id, description.getOrElse(""), subtype, source, sourceConf, query))
        case "SNAPSHOT" | "TREND" => List.empty
        case x                    => throw IllegalParameterException(x)
      }
    }
    sqlChecks
  }

  /**
   * Parses checks connected with metric from configuration file
   * @return List of (Check, MetricId)
   */
  override protected def getMetricByCheck: List[(Check, String)] = {
    val checksList: List[ConfigObject] = configObj.getObjectList("Checks").toList

    val metricListByCheck = checksList.flatMap { chks =>
      val outerConf = chks.toConfig
      val checkType = outerConf.getString("type")
      val descr = Try {
        outerConf.getString("description")
      }.toOption
      val subtype   = outerConf.getString("subtype")
      val intConfig = outerConf.getObject("config").toConfig

      val params = getParams(intConfig)
      val metricListByCheck: List[(Check, String)] =
        checkType.toUpperCase match {
          case "SNAPSHOT" =>
            val metrics = intConfig.getStringList("metrics")
            val id = Try {
              outerConf.getString("id")
            }.toOption.getOrElse(subtype + ":" + checkType + ":" + metrics
              .mkString("+") + ":" + params.values.mkString(","))
            subtype match {
              // There also a way to use check name, but with additional comparsment rule
              case "BASIC_NUMERIC" =>
                val compRule = params("compareRule").toString
                compRule.toUpperCase match {
                  case "GT" =>
                    if (params.contains("threshold"))
                      metrics.map { m =>
                        GreaterThanThresholdCheck(id,
                          descr.getOrElse(""),
                          Seq.empty[MetricResult],
                          params("threshold").toString.toDouble) -> m
                      }.toList
                    else if (params.contains("compareMetric"))
                      metrics.map { m =>
                        GreaterThanMetricCheck(id,
                          descr.getOrElse(""),
                          Seq.empty[MetricResult],
                          params("compareMetric").toString) -> m
                      }.toList
                    else throw MissingParameterInException(subtype)
                  case "LT" =>
                    if (params.contains("threshold"))
                      metrics.map { m =>
                        LessThanThresholdCheck(id,
                          descr.getOrElse(""),
                          Seq.empty[MetricResult],
                          params("threshold").toString.toDouble) -> m
                      }.toList
                    else if (params.contains("compareMetric"))
                      metrics.map { m =>
                        LessThanMetricCheck(id,
                          descr.getOrElse(""),
                          Seq.empty[MetricResult],
                          params("compareMetric").toString) -> m
                      }.toList
                    else throw MissingParameterInException(subtype)
                  case "EQ" =>
                    if (params.contains("threshold"))
                      metrics.map { m =>
                        EqualToThresholdCheck(id,
                          descr.getOrElse(""),
                          Seq.empty[MetricResult],
                          params("threshold").toString.toDouble) -> m
                      }.toList
                    else if (params.contains("compareMetric"))
                      metrics.map { m =>
                        EqualToMetricCheck(id,
                          descr.getOrElse(""),
                          Seq.empty[MetricResult],
                          params("compareMetric").toString) -> m
                      }.toList
                    else throw MissingParameterInException(subtype)
                }
              case "GREATER_THAN" =>
                if (params.contains("threshold"))
                  metrics.map { m =>
                    GreaterThanThresholdCheck(id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      params("threshold").toString.toDouble) -> m
                  }.toList
                else if (params.contains("compareMetric"))
                  metrics.map { m =>
                    GreaterThanMetricCheck(id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      params("compareMetric").toString) -> m
                  }.toList
                else throw MissingParameterInException(subtype)
              case "LESS_THAN" =>
                if (params.contains("threshold"))
                  metrics.map { m =>
                    LessThanThresholdCheck(id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      params("threshold").toString.toDouble) -> m
                  }.toList
                else if (params.contains("compareMetric"))
                  metrics.map { m =>
                    LessThanMetricCheck(id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      params("compareMetric").toString) -> m
                  }.toList
                else throw MissingParameterInException(subtype)
              case "EQUAL_TO" =>
                if (params.contains("threshold"))
                  metrics.map { m =>
                    EqualToThresholdCheck(id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      params("threshold").toString.toDouble) -> m
                  }.toList
                else if (params.contains("compareMetric"))
                  metrics.map { m =>
                    EqualToMetricCheck(id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      params("compareMetric").toString) -> m
                  }.toList
                else throw MissingParameterInException(subtype)
              case "DIFFER_BY_LT" =>
                if (params.contains("threshold") && params.contains("compareMetric"))
                  metrics.map { m =>
                    DifferByLTMetricCheck(id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      params("compareMetric").toString,
                      params("threshold").toString.toDouble) -> m
                  }.toList
                else throw MissingParameterInException(subtype)
              case x => throw IllegalParameterException(x)
            }
          case "TREND" =>
            val metrics = intConfig.getStringList("metrics")
            val id = Try {
              outerConf.getString("id")
            }.toOption.getOrElse(subtype + ":" + checkType + ":" + metrics
              .mkString("+") + ":" + params.values.mkString(","))
            val rule      = intConfig.getString("rule")
            val startDate = Try { params("startDate").toString }.toOption
            subtype match {
              case "TOP_N_RANK_CHECK" =>
                if (params.contains("threshold") && params.contains("timewindow"))
                  metrics.flatMap { m =>
                  {
                    val basecheck =
                      TopNRankCheck(id,
                        descr.getOrElse(""),
                        Seq.empty[MetricResult],
                        rule,
                        params("threshold").toString.toDouble,
                        params("timewindow").toString.toInt)
                    generateMetricSubId(m, params("targetNumber").toString.toInt)
                      .map(x => basecheck -> x)
                  }
                  }.toList
                else throw MissingParameterInException(subtype)
              case "AVERAGE_BOUND_FULL_CHECK" =>
                if (params.contains("threshold") && params.contains("timewindow"))
                  metrics.map { m =>
                    AverageBoundFullCheck(
                      id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      rule,
                      params("threshold").toString.toDouble,
                      params("timewindow").toString.toInt
                    ) -> m
                  }.toList
                else throw MissingParameterInException(subtype)
              case "AVERAGE_BOUND_RANGE_CHECK" =>
                if (params.contains("thresholdUpper") && params.contains("thresholdLower") && params.contains(
                  "timewindow"))
                  metrics.map { m =>
                    AverageBoundRangeCheck(
                      id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      rule,
                      params("thresholdUpper").toString.toDouble,
                      params("thresholdLower").toString.toDouble,
                      params("timewindow").toString.toInt
                    ) -> m
                  }.toList
                else throw MissingParameterInException(subtype)

              case "AVERAGE_BOUND_UPPER_CHECK" =>
                if (params.contains("threshold") && params.contains("timewindow"))
                  metrics.map { m =>
                    AverageBoundUpperCheck(
                      id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      rule,
                      params("threshold").toString.toDouble,
                      params("timewindow").toString.toInt
                    ) -> m
                  }.toList
                else throw MissingParameterInException(subtype)
              case "AVERAGE_BOUND_LOWER_CHECK" =>
                if (params.contains("threshold") && params.contains("timewindow"))
                  metrics.map { m =>
                    AverageBoundLowerCheck(
                      id,
                      descr.getOrElse(""),
                      Seq.empty[MetricResult],
                      rule,
                      params("threshold").toString.toDouble,
                      params("timewindow").toString.toInt
                    ) -> m
                  }.toList
                else throw MissingParameterInException(subtype)
              case x => throw IllegalParameterException(x)
            }
          case "SQL" => List.empty
          case x     => throw IllegalParameterException(x)
        }

      metricListByCheck
    }

    metricListByCheck
  }

  /**
   * Parses targets from configuration file
   * @return Map of (target_id, target_config)
   */
  override protected def getTargetsConfigMap: Map[String, List[TargetConfig]] = {

    val optionalTargetList = Try { configObj.getObjectList("Targets").toList }.toOption
    optionalTargetList match {
      case Some(targetList) =>
        val parsedList = targetList.map { trg =>
          val outerConf = trg.toConfig
          val inConfig   = outerConf.getObject("config").toConfig

          val tipo      = outerConf.getString("type")
          val name = Try(outerConf.getString("id")).getOrElse(tipo)

          val fileFormat = inConfig.getString("fileFormat")
          val date = Try(inConfig.getString("date")).toOption

          fileFormat.toUpperCase match {
            case "HIVE" =>
              val schema = inConfig.getString("schema")
              val table = inConfig.getString("table")
              val datePartitionCol = Try(inConfig.getString("datePartitionColumn")).toOption

              tipo -> HiveTargetConfig(schema, table, date, datePartitionCol)
            case _ =>
              val path       = inConfig.getString("path")
              val delimiter: Option[String] = settings.backComp.delimiterExtractor(inConfig)
              val quote: Option[String] = Try(inConfig.getString("quote")).toOption
              val escape: Option[String] = Try(inConfig.getString("escape")).toOption
              val quoteMode: Option[String] = settings.backComp.quoteModeExtractor(inConfig)

              val hdfsTargetConfig = HdfsTargetConfig(
                name,  fileFormat, path, delimiter, quote, escape, date, quoteMode
              )

              tipo.toUpperCase match {
                case "SYSTEM" =>
                  val checkList: Seq[String] = outerConf.getStringList("checkList").toList
                  val mailList: Seq[String] = outerConf.getStringList("mailingList").toList
                  tipo -> SystemTargetConfig(name, checkList, mailList, hdfsTargetConfig)
                case _ =>
                  tipo -> hdfsTargetConfig
              }
          }
        }
        parsedList.groupBy(_._1).map { case (k, v) => (k, v.map(_._2)) }
      case None => Map.empty
    }
  }

  override protected def getLoadChecks: Map[String, Seq[LoadCheck]] = {
    val checkListConfOpt = Try { configObj.getObjectList("LoadChecks").toList }.toOption
    checkListConfOpt match {
      case Some(checkList) =>
        checkList
          .map(x => {
            val conf = x.toConfig

            val id: String     = conf.getString("id")
            val tipo: String   = conf.getString("type")
            val source: String = conf.getString("source")
            val result: AnyRef = conf.getAnyRef("option")

            if (LoadCheckEnum.contains(tipo)) {
              val check: LoadCheck = LoadCheckEnum
                .getCheckClass(tipo)
                .getConstructor(classOf[String], classOf[String], classOf[String], classOf[AnyRef])
                .newInstance(id, tipo, source, result)
                .asInstanceOf[LoadCheck]

              (source, check)
            } else throw new IllegalArgumentException(s"Unknown Load Check type: $tipo")

          })
          .groupBy(_._1)
          .map { case (k, v) => (k, v.map(_._2)) }
      case None => Map.empty[String, Seq[LoadCheck]]
    }

  }

  /**
   * Utilities
   */
  /**
   * Processes parameter sub-configuration
   * Made to prevent unexpected parameters and their values
   * @param ccf parameter configuration
   * @return mapped parameter
   */
  private def getParams(ccf: Config): Map[String, Any] = {
    Try { ccf.getConfig("params") }.toOption match {
      case Some(p) =>
        (for {
          entry: Entry[String, ConfigValue] <- p.entrySet()
          key = entry.getKey
          value = key match {
            case "threshold"      => p.getDouble(key)
            case "thresholdUpper" => p.getDouble(key)
            case "thresholdLower" => p.getDouble(key)
            case "timewindow"     => p.getInt(key)
            case "compareMetric"  => p.getString(key)
            case "compareValue"   => p.getString(key)
            case "lowerCompareValue" => p.getDouble(key)
            case "upperCompareValue" => p.getDouble(key)
            case "targetValue"    => p.getString(key)
            case "maxCapacity"    => p.getInt(key)
            case "accuracyError"  => p.getDouble(key)
            case "targetNumber"   => p.getInt(key)
            case "targetSideNumber" =>
              p.getDouble(key) // move to irrelevant params
            case "domain"     => p.getStringList(key).toSet
            case "startDate"  => p.getString(key)
            case "compareRule"   => p.getString(key)
            case "dateFormat" => p.getString(key)
            case "regex"      => p.getString(key)
            case "length"     => p.getInt(key)
            case "precision"  => p.getInt(key)
            case "scale"      => p.getInt(key)
            case "includeBound" => p.getBoolean(key)
            case "includeEmptyStrings" => p.getString(key)
            case x =>
              log.error(s"${key.toUpperCase} is an unexpected parameters from config!")
              throw IllegalParameterException(x)
          }
        } yield (key, value)).toMap
      case _ => Map.empty[String, Any]
    }
  }

  /**
   * Parses composed metrics from configuration file
   * @return List of composed metrics
   */
  override protected def getComposedMetrics: List[ComposedMetric] = {
    val metricsList: List[ConfigObject] =
      Try(configObj.getObjectList("ComposedMetrics").toList)
        .getOrElse(List.empty[ConfigObject])

    val metricFileList: List[ComposedMetric] = metricsList.map { mts =>
      val outerConf = mts.toConfig
      val id        = outerConf.getString("id")
      val name      = outerConf.getString("name")
      val descr     = outerConf.getString("description")
      val formula   = outerConf.getString("formula")

      ComposedMetric(
        id,
        name,
        descr,
        formula,
        Map.empty
      )
    }

    metricFileList
  }
  
  override protected def getPostprocessorConfig: Seq[BasicPostprocessor] = {
    val ppConfs: Seq[ConfigObject] = Try(configObj.getObjectList("Postprocessing").toList).getOrElse(List.empty)
    log.info(s"Found ${ppConfs.size} configurations.")
    ppConfs.map(ppc => {
      val conf = ppc.toConfig
      val mode = conf.getString("mode")
      Try(PostprocessorType.withName(mode.toLowerCase)).toOption match {
        case Some(meta) =>
          log.info(s"Found configuration with mode : $mode")
          val inner: Config = conf.getConfig("config")

          meta.service.getConstructors.head
            .newInstance(inner, settings)
            .asInstanceOf[BasicPostprocessor]
        case None =>
          log.warn("Wrong mode name!")
          throw IllegalParameterException(mode)
      }
    })
  }

  /**
   * Dummy abstract method implementation: message brokers were not supported in initial configuration structure.
   * @return empty map
   */
  override protected def getBrokersById: Map[String, KafkaConfig] = Map.empty
}