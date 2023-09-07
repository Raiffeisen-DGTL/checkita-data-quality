package ru.raiffeisen.checkita.configs

import com.typesafe.config.{Config, ConfigObject}
import org.apache.spark.storage.StorageLevel
import ru.raiffeisen.checkita.checks.load.{ColumnLoadCheck, EncodingLoadCheck, ExistLoadCheck, FileTypeLoadCheck, LoadCheck, LoadCheckEnum, MinColumnLoadCheck}
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.checks.Check
import ru.raiffeisen.checkita.checks.snapshot._
import ru.raiffeisen.checkita.checks.sql.SQLCheck
import ru.raiffeisen.checkita.checks.trend._
import ru.raiffeisen.checkita.exceptions.{IllegalParameterException, MissingParameterInException}
import ru.raiffeisen.checkita.metrics.{ColumnMetric, ComposedMetric, FileMetric, Metric, MetricResult}
import ru.raiffeisen.checkita.postprocessors.{BasicPostprocessor, PostprocessorType}
import ru.raiffeisen.checkita.sources._
import ru.raiffeisen.checkita.targets._
import ru.raiffeisen.checkita.utils.enums.{KafkaFormats, ResultTargets}
import ru.raiffeisen.checkita.utils.{DQSettings, camelToUnderscores, generateMetricSubId}
import ru.raiffeisen.checkita.utils.io.dbmanager.DBManager

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._

class ConfigReader1x(configPath: String)(implicit resultsWriter: DBManager, settings: DQSettings)
  extends ConfigReader(configPath: String) {

  /**
   * Parses databases from configuration file
   * @return Map of (db_id, db_config)
   */
  override protected def getDatabasesById: Map[String, DatabaseConfig] =
    Try(configObj.getObject(ConfigSections.databases)) match {
      case Success(dbConfig) =>
        // check for invalid database subtypes:
        if (dbConfig.keySet().exists(!ConfigDbSubTypes.contains(_)))
          throw IllegalParameterException("Config -> databases: Invalid database subtypes are found.")

        ConfigDbSubTypes.values.flatMap(
          dbSubType => databaseParser(dbSubType)(
            Try(dbConfig.toConfig.getObjectList(dbSubType).toList).getOrElse(List.empty)
          )
        ).toMap
      case Failure(_) => Map.empty
    }

  /**
   * Parses message brokers from the configuration file.
   * Kafka is the only supported message broker for now.
   * @return Map of (brokerId, KafkaConfig)
   */
  override protected def getBrokersById: Map[String, KafkaConfig] =
    Try(configObj.getObject(ConfigSections.messageBrokers)) match {
      case Success(brokersConfig) =>
        // check for invalid database subtypes:
        if (brokersConfig.keySet().exists(!ConfigBrokerTypes.contains(_)))
          throw IllegalParameterException("Config -> messageBrokers: Invalid broker types are found.")

        ConfigBrokerTypes.values.flatMap(
          brokerType => brokerParser(brokerType)(
            Try(brokersConfig.toConfig.getObjectList(brokerType).toList).getOrElse(List.empty)
          )
        ).toMap
      case Failure(_) => Map.empty
    }

  /**
   * Parses sources from configuration file
   * @return Map of (source_id, source_config)
   */
  override protected def getSourcesById: Map[String, SourceConfig] =
    Try(configObj.getObject(ConfigSections.sources)) match {
      case Success(sourceConfig) =>
        // check for invalid source subtypes:
        if (sourceConfig.keySet().exists(!ConfigSourceSubTypes.contains(_)))
          throw IllegalParameterException("Config -> sources: Invalid source subtypes are found.")

        ConfigSourceSubTypes.values.flatMap {
          case subType@ConfigSourceSubTypes.hdfs =>
            Try(sourceConfig.toConfig.getObject(subType)) match {
              case Success(hdfsSourceConfig) =>
                // check for invalid hdfs sources subtypes:
                if (hdfsSourceConfig.keySet().exists(!ConfigHdfsFileTypes.contains(_)))
                  throw IllegalParameterException("Config -> sources -> hdfs: Invalid hdfs file types are found.")

                ConfigHdfsFileTypes.values.flatMap {
                  hdfsFileType => fileSourceParser(hdfsFileType)(
                    Try(hdfsSourceConfig.toConfig.getObjectList(hdfsFileType).toList).getOrElse(List.empty)
                  )
                }
              case Failure(_) => List.empty
            }
          case subType@ConfigSourceSubTypes.kafka => kafkaSourceParser(
            Try(sourceConfig.toConfig.getObjectList(subType).toList).getOrElse(List.empty)
          )
          case tableSubType => tableSourceParser(tableSubType)(
            Try(sourceConfig.toConfig.getObjectList(tableSubType).toList).getOrElse(List.empty)
          )
        }.toMap
      case Failure(_) =>
        throw IllegalParameterException(s"Section '${ConfigSections.sources}' is not found in configuration file.")
    }

  /**
   * Parses virtual sources from configuration file
   * @return Map of (source_id, virtual_source_config)
   */
  override protected def getVirtualSourcesById: Map[String, VirtualFile] =
    Try(configObj.getObject(ConfigSections.virtualSources)) match {
      case Success(virtualSourceConfig) =>
        // check for invalid source subtypes:
        if (virtualSourceConfig.keySet().exists(!ConfigVirtualSourceSybTypes.contains(_)))
          throw IllegalParameterException("Config -> virtualSources: Invalid source subtypes are found.")

        ConfigVirtualSourceSybTypes.values.flatMap {
          virtualSourceSubType => virtualSourceParser(virtualSourceSubType)(
            Try(virtualSourceConfig.toConfig.getObjectList(virtualSourceSubType).toList).getOrElse(List.empty)
          )
        }.toMap
      case Failure(_) => Map.empty
    }

  /**
   * Parses load checks from configuration file
   * @return Map of (source_id, Seq[load_check])
   */
  override protected def getLoadChecks: Map[String, Seq[LoadCheck]] =
    Try(configObj.getObject(ConfigSections.loadChecks)) match {
      case Success(loadCheckConfig) =>
        // check for invalid load check subtypes:
        if (loadCheckConfig.keySet().exists(!ConfigLoadCheckSubTypes.contains(_)))
          throw IllegalParameterException("Config -> loadChecks: Invalid load check subtypes are found.")

        ConfigLoadCheckSubTypes.values.flatMap{
          loadCheckSubType => loadCheckParser(loadCheckSubType)(
            Try(loadCheckConfig.toConfig.getObjectList(loadCheckSubType).toList).getOrElse(List.empty)
          )
        }.groupBy(_._1).mapValues(_.map(_._2))
      case Failure(_) => Map.empty
    }

  /**
   * Parses metrics from configuration file
   * @return List of (source_id, metric)
   */
  override protected def getMetricsBySource: List[(String, Metric)] =
    Try(configObj.getObject(ConfigSections.metrics)) match {
      case Success(metricsConfig) =>
        // check for invalid load check subtypes:
        if (metricsConfig.keySet().exists(!ConfigMetricsSubTypes.contains(_)))
          throw IllegalParameterException("Config -> metrics: Invalid load check subtypes are found.")

        ConfigMetricsSubTypes.values.flatMap{ metricSubType => metricSubType match {
          case ConfigMetricsSubTypes.file =>
            Try(metricsConfig.toConfig.getObject(ConfigMetricsSubTypes.file)) match {
              case Success(fileMetricsConfig) =>
                // check for invalid file metric subtypes:
                if (fileMetricsConfig.keySet().exists(!ConfigFileMetrics.contains(_)))
                  throw IllegalParameterException ("Config -> metrics -> file: Invalid file metric subtypes are found.")

                // only one file metric exists for now: rowCount.
                fileMetricParser(
                  Try(fileMetricsConfig.toConfig.getObjectList(ConfigFileMetrics.rowCount).toList).getOrElse (List.empty)
                )
              case Failure(_) => List.empty
            }
          case ConfigMetricsSubTypes.column =>
            Try(metricsConfig.toConfig.getObject(ConfigMetricsSubTypes.column)) match {
              case Success(columnMetricsConfig) =>
                // check for invalid column metric subtypes:
                if (columnMetricsConfig.keySet().exists(!ConfigColumnMetrics.contains(_)))
                  throw IllegalParameterException("Config -> metrics -> column: Invalid file metric subtypes are found.")

                ConfigColumnMetrics.values.flatMap { columnMetric =>
                  columnMetricParser(columnMetric)(
                    Try(columnMetricsConfig.toConfig.getObjectList(columnMetric.toString).toList).getOrElse(List.empty)
                  )
                }
              case Failure(_) => List.empty
            }
          case ConfigMetricsSubTypes.composed => List.empty // composed metrics are parsed separately
        }
        }
      case Failure(_) =>
        throw IllegalParameterException(s"Section '${ConfigSections.metrics}' is not found in configuration file.")
    }

  /**
   * Parses composed metrics from configuration file
   * @return List of composed metrics
   */
  override protected def getComposedMetrics: List[ComposedMetric] =
    Try(configObj.getObject(ConfigSections.metrics)) match {
      case Success(metricsConfig) =>
        // check for invalid load check subtypes:
        if (metricsConfig.keySet().exists(!ConfigMetricsSubTypes.contains(_)))
          throw IllegalParameterException("Config -> metrics: Invalid load check subtypes are found.")

        // parse composed metrics
        composedMetricParser(
          Try(metricsConfig.toConfig.getObjectList(ConfigMetricsSubTypes.composed).toList).getOrElse(List.empty)
        )
      case Failure(_) =>
        throw IllegalParameterException(s"Section '${ConfigSections.metrics}' is not found in configuration file.")
    }

  /**
   * Parses checks connected with metrics from configuration file
   * @return List of (Check, MetricId)
   */
  override protected def getMetricByCheck: List[(Check, String)] =
    Try(configObj.getObject(ConfigSections.checks)) match {
      case Success(checksConfig) =>
        // check for invalid check subtypes:
        if (checksConfig.keySet().exists(!ConfigChecksSubTypes.contains(_)))
          throw IllegalParameterException("Config -> checks: Invalid check subtypes are found.")

        ConfigChecksSubTypes.values.flatMap { checkSubType =>
          checkSubType match {
            case ConfigChecksSubTypes.trend =>
              Try(checksConfig.toConfig.getObject(ConfigChecksSubTypes.trend)) match {
                case Success(trendChecksConfig) =>
                  // check for invalid trend check subtypes:
                  if (trendChecksConfig.keySet().exists(!ConfigTrendChecks.contains(_)))
                    throw IllegalParameterException("Config -> checks -> trend: Invalid trend check subtypes are found.")

                  ConfigTrendChecks.values.flatMap { trendCheck =>
                    trendCheckParser(trendCheck)(
                      Try(trendChecksConfig.toConfig.getObjectList(trendCheck).toList).getOrElse(List.empty)
                    )
                  }
                case Failure(_) => List.empty
              }
            case ConfigChecksSubTypes.snapshot =>
              Try(checksConfig.toConfig.getObject(ConfigChecksSubTypes.snapshot)) match {
                case Success(snapshotChecksConfig) =>
                  // check for invalid column metric subtypes:
                  if (snapshotChecksConfig.keySet().exists(!ConfigSnapshotChecks.contains(_)))
                    throw IllegalParameterException("Config -> checks -> snapshot: Invalid snapshot check subtypes are found.")

                  ConfigSnapshotChecks.values.flatMap { snapshotCheck =>
                    snapshotCheckParser(snapshotCheck)(
                      Try(snapshotChecksConfig.toConfig.getObjectList(snapshotCheck).toList).getOrElse(List.empty)
                    )
                  }
                case Failure(_) => List.empty
              }
            case ConfigChecksSubTypes.sql => List.empty // sql checks are parsed separately
          }
        }
      case Failure(_) => List.empty
    }

  /**
   * Parses SQL checks from configuration file
   * @return List of SQL checks
   */
  override protected def getSqlChecks: List[SQLCheck] =
    Try(configObj.getObject(ConfigSections.checks)) match {
      case Success(checksConfig) =>
        // check for invalid check subtypes:
        if (checksConfig.keySet().exists(!ConfigChecksSubTypes.contains(_)))
          throw IllegalParameterException("Config -> checks: Invalid check subtypes are found.")

        Try(checksConfig.toConfig.getObject(ConfigChecksSubTypes.sql)) match {
          case Success(sqlChecksConfig) =>
            // check for invalid column metric subtypes:
            if (sqlChecksConfig.keySet().exists(!ConfigSqlChecks.contains(_)))
              throw IllegalParameterException("Config -> checks -> sql: Invalid sql check subtypes are found.")

            ConfigSqlChecks.values.flatMap { sqlCheckSubType =>
              sqlCheckParser(sqlCheckSubType)(
                Try(sqlChecksConfig.toConfig.getObjectList(sqlCheckSubType).toList).getOrElse(List.empty)
              )
            }
          case Failure(_) => List.empty
        }
      case Failure(_) => List.empty
    }

  /**
   * Parses targets from configuration file
   * @return Map of (target_id, targetConfig)
   */
  override protected def getTargetsConfigMap: Map[String, List[TargetConfig]] =
    Try(configObj.getObject(ConfigSections.targets)) match {
      case Success(targetsConfig) =>
        // check for invalid check subtypes:
        if (targetsConfig.keySet().exists(!ConfigTargetsTypes.contains(_)))
          throw IllegalParameterException("Config -> targets: Invalid targets save types are found.")

        ConfigTargetsTypes.values.flatMap{ targetType =>
          Try(targetsConfig.toConfig.getObject(targetType)) match {
            case Success(targetsSubConfig) =>
              if (targetsSubConfig.keySet().exists(!ConfigTargetsSubTypes.contains(_)))
                throw IllegalParameterException(s"Config -> targets -> $targetType: Invalid targets subtypes are found.")

              ConfigTargetsSubTypes.values.flatMap{ targetSubType =>
                if (targetType == ConfigTargetsTypes.checkAlerts) {
                  Try(targetsSubConfig.toConfig.getObjectList(targetSubType)) match {
                    case Success(checkAlerts) => checkAlerts.flatMap{ checkAlert =>
                      targetParser(targetType, targetSubType)(Some(checkAlert))
                    }
                    case Failure(_) => List.empty
                  }
                } else {
                  targetParser(targetType, targetSubType)(Try(targetsSubConfig.toConfig.getObject(targetSubType)).toOption)
                }
              }
            case Failure(_) => List.empty
          }
        }.groupBy(_._1).mapValues(_.map(_._2))
      case Failure(_) => Map.empty
    }

  /**
   * Parses list of postprocessing operations from configuration file.
   * @return
   */
  override protected def getPostprocessorConfig: Seq[BasicPostprocessor] = Seq.empty

  /**
   * Returns a function to parse list of database connection configs
   * for specified database subtype.
   * @param subtype database subtype
   * @return function to parse database connection configs
   */
  private def databaseParser(subtype: String): List[ConfigObject] => Map[String, DatabaseConfig] =
    dbList => dbList.map { db =>
      // check for invalid database parameter keys:
      if (db.keySet().exists(!ConfigDbParameters.contains(_)))
        throw IllegalParameterException(s"Config -> databases -> $subtype: Invalid database parameter keys are found.")

      val dbConfig = db.toConfig
      val id = dbConfig.getString(ConfigDbParameters.id)
      val url = dbConfig.getString(ConfigDbParameters.url)
      subtype match {
        case ConfigDbSubTypes.oracle | ConfigDbSubTypes.postgresql =>
          // getting required credentials and optional schema
          val user = dbConfig.getString(ConfigDbParameters.user)
          val password = dbConfig.getString(ConfigDbParameters.password)
          val schema = Try(dbConfig.getString(ConfigDbParameters.schema)).toOption
          id -> DatabaseConfig(id, subtype, url, None, None, Some(user), Some(password), schema)
        case ConfigDbSubTypes.sqlite => id -> DatabaseConfig(id, subtype, url)
      }
    }.toMap

  /**
   * Returns a function to parse list of connections to message brokers from config
   * for specified broker type.
   * @param brokerType - type of the message broker (the only supported type for now is kafka)
   * @return
   */
  private def brokerParser(brokerType: String): List[ConfigObject] => Map[String, KafkaConfig] =
    brokersList => brokersList.map { broker =>
      if (broker.keySet().exists(!ConfigKafkaParameters.contains(_)))
        throw IllegalParameterException(s"Config -> messageBrokers -> $brokerType: Invalid kafka broker parameter keys are found.")

      val brokerConfig = broker.toConfig
      val id = brokerConfig.getString(ConfigKafkaParameters.id)
      val servers = brokerConfig.getStringList(ConfigKafkaParameters.servers).toSeq
      val jaasConfigFile = Try(brokerConfig.getString(ConfigKafkaParameters.jaasConfigFile)).toOption
      val parameters = Try(brokerConfig.getStringList(ConfigKafkaParameters.parameters).toSeq).toOption

      id -> KafkaConfig(id, servers, jaasConfigFile, parameters.getOrElse(Seq.empty[String]))
    }.toMap

  /**
   * Returns a function to parse list of table-like sources
   * from configuration file
   * @param subtype table-like source subtype
   * @return function to parse table-like sources
   */
  private def tableSourceParser(subtype: String): List[ConfigObject] => Map[String, SourceConfig] =
    sourceList => sourceList.map { source =>
      // check for invalid source parameter keys:
      if (source.keySet().exists(!ConfigSourceParameters.contains(_)))
        throw IllegalParameterException(s"Config -> sources -> $subtype: Invalid source parameter keys are found.")

      val sourceConfig = source.toConfig
      val id = sourceConfig.getString(ConfigSourceParameters.id)
      val keyFields = Try(
        sourceConfig.getStringList(ConfigSourceParameters.keyFields).toSeq.map(_.toLowerCase)
      ).getOrElse(Seq.empty)
      subtype match {
        case ConfigSourceSubTypes.table =>
          val database = sourceConfig.getString(ConfigSourceParameters.database)
          val table = sourceConfig.getString(ConfigSourceParameters.table)
          id -> TableConfig(id, database, table, None, None, keyFields)
        case ConfigSourceSubTypes.hive =>
          val query = sourceConfig.getString(ConfigSourceParameters.query)
          id -> HiveTableConfig(id, query, keyFields)
        //        case ConfigSourceSubTypes.hbase =>
        //          val table = sourceConfig.getString(ConfigSourceParameters.table)
        //          val columns = sourceConfig.getStringList(ConfigSourceParameters.columns)
        //          id -> HBaseSrcConfig(id, table, columns, keyFields)
      }
    }.toMap

  /**
   * Parses kafka sources from configuration file
   * @param kafkaSourceList list of configuration objects describing kafka sources
   * @return Map with kafka sources (sourceId, KafkaTopic)
   */
  private def kafkaSourceParser(kafkaSourceList: List[ConfigObject]): Map[String, KafkaSourceConfig] =
    kafkaSourceList.map { kafkaSource =>
      if (kafkaSource.keySet().exists(!ConfigSourceParameters.contains(_)))
        throw IllegalParameterException(s"Config -> sources -> kafka: Invalid kafka source parameter keys are found.")

      val kafkaSourceConfig = kafkaSource.toConfig
      val id = kafkaSourceConfig.getString(ConfigSourceParameters.id)
      val brokerId = kafkaSourceConfig.getString(ConfigSourceParameters.brokerId)
      val format = KafkaFormats.withNameOpt(kafkaSourceConfig.getString(ConfigSourceParameters.format)).get
      val topics = Try(kafkaSourceConfig.getStringList(ConfigSourceParameters.topics).toSeq).toOption
      val topicPattern = Try(kafkaSourceConfig.getString(ConfigSourceParameters.topicPattern)).toOption

      if ((topics.isEmpty && topicPattern.isEmpty) || (topics.nonEmpty && topicPattern.nonEmpty)) {
        throw IllegalParameterException(
          s"Config -> sources -> kafka -> $id: Either 'topics' or 'topicPattern' must be defined but not both."
        )
      }

      val startingOffsets = Try(kafkaSourceConfig.getString(ConfigSourceParameters.startingOffsets)).toOption
      val endingOffsets = Try(kafkaSourceConfig.getString(ConfigSourceParameters.endingOffsets)).toOption
      val options = Try(kafkaSourceConfig.getStringList(ConfigSourceParameters.options).toSeq).getOrElse(Seq.empty)

      val keyFields = Try(
        kafkaSourceConfig.getStringList(ConfigSourceParameters.keyFields).toSeq.map(_.toLowerCase)
      ).getOrElse(Seq.empty)

      id -> KafkaSourceConfig(
        id, brokerId, topics, topicPattern, format, startingOffsets, endingOffsets, options, keyFields
      )
    }.toMap

  /**
   * Returns a function to parse list of file sources
   * from configuration file
   * @param subtype file type to parse
   * @return function to parse file sources
   */
  private def fileSourceParser(subtype: String): List[ConfigObject] => Map[String, SourceConfig] =
    fileSourceList => fileSourceList.map { fileSource =>
      // check for invalid source parameter keys:
      if (fileSource.keySet().exists(!ConfigSourceParameters.contains(_)))
        throw IllegalParameterException(s"Config -> sources -> hdfs -> $subtype: Invalid source parameter keys are found.")

      val fileSourceConfig = fileSource.toConfig
      val id = fileSourceConfig.getString(ConfigSourceParameters.id)
      val path = fileSourceConfig.getString(ConfigSourceParameters.path)
      val keyFields = Try(
        fileSourceConfig.getStringList(ConfigSourceParameters.keyFields).toSeq.map(_.toLowerCase)
      ).getOrElse(Seq.empty)

      subtype match {
        case ConfigHdfsFileTypes.fixed =>
          val schemaType =
            if (fileSourceConfig.hasPath(ConfigSourceParameters.schema)) SchemaFormats.fixedFull
            else if (fileSourceConfig.hasPath(ConfigSourceParameters.shortSchema)) SchemaFormats.fixedShort
            else throw IllegalParameterException(s"No schema found for fixed-length file $id.")
          val schema = schemaParser(schemaType, fileSourceConfig)
          id -> HdfsFile(id, path, ConfigHdfsFileTypes.fixed, false, schema=Some(schema), keyFields=keyFields)
        case ConfigHdfsFileTypes.delimited =>
          val header = Try(fileSourceConfig.getBoolean(ConfigSourceParameters.header)).getOrElse(false)
          val delimiter = Try(fileSourceConfig.getString(ConfigSourceParameters.delimiter)).toOption
          val escape = Try(fileSourceConfig.getString(ConfigSourceParameters.escape)).toOption
          val quote = Try(fileSourceConfig.getString(ConfigSourceParameters.quote)).toOption
          val schema =
            if (header) None
            else if (fileSourceConfig.hasPath(ConfigSourceParameters.schema)) {
              Some(schemaParser(SchemaFormats.delimited, fileSourceConfig))
            }
            else throw IllegalParameterException(s"No schema found for delimited file $id.")
          id -> HdfsFile(
            id, path, ConfigHdfsFileTypes.delimited, header, delimiter, quote, escape, schema, keyFields
          )
        case ConfigHdfsFileTypes.avro =>
          val schema = Try(fileSourceConfig.getString(ConfigSourceParameters.schema)).toOption
          id -> HdfsFile(id, path, ConfigHdfsFileTypes.avro, false, schema=schema, keyFields=keyFields)
        case ConfigHdfsFileTypes.parquet => id -> HdfsFile(
          id, path, ConfigHdfsFileTypes.parquet, false, keyFields=keyFields
        )
        case ConfigHdfsFileTypes.orc => id -> HdfsFile(
          id, path, ConfigHdfsFileTypes.orc, false, keyFields=keyFields
        )
        case ConfigHdfsFileTypes.delta => id -> HdfsFile(
          id, path, ConfigHdfsFileTypes.delta, false, keyFields=keyFields
        )
      }
    }.toMap

  /**
   * Returns a function to parse list of virtual sources
   * from configuration file
   * @param subtype virtual source subtype
   * @return function to parse virtual sources
   */
  private def virtualSourceParser(subtype: String): List[ConfigObject] => Map[String, VirtualFile] =
    virtualSourceList => virtualSourceList.map { virtualSource =>
      // check for invalid virtual source parameter keys:
      if (virtualSource.keySet().exists(!ConfigVirtualSourceParameters.contains(_)))
        throw IllegalParameterException(s"Config -> virtualSources -> $subtype: Invalid virtual source parameter keys are found.")

      val virtualSourceConfig = virtualSource.toConfig
      val id = virtualSourceConfig.getString(ConfigVirtualSourceParameters.id)
      val parentSources = virtualSourceConfig.getStringList(ConfigVirtualSourceParameters.parentSources)
      val persist = Try(
        StorageLevel.fromString(virtualSourceConfig.getString(ConfigVirtualSourceParameters.persist))
      ).toOption
      val keyFields = Try(
        virtualSourceConfig.getStringList(ConfigSourceParameters.keyFields).toSeq.map(_.toLowerCase)
      ).getOrElse(Seq.empty)
      val save = Try {
        val saveConfig = virtualSourceConfig.getObject(ConfigVirtualSourceParameters.save).toConfig
        val fileFormat = saveConfig.getString(ConfigHdfsTargetParameters.fileFormat)
        val path = saveConfig.getString(ConfigHdfsTargetParameters.path)
        val date = Try(saveConfig.getString(ConfigHdfsTargetParameters.date)).toOption
        val delimiter = Try(saveConfig.getString(ConfigHdfsTargetParameters.delimiter)).toOption
        val quote = Try(saveConfig.getString(ConfigHdfsTargetParameters.quote)).toOption
        val escape = Try(saveConfig.getString(ConfigHdfsTargetParameters.escape)).toOption
        val quoteMode =
          if (Try(saveConfig.getBoolean(ConfigHdfsTargetParameters.quoted)).getOrElse(false)) Some("ALL")
          else Some("NONE")

        HdfsTargetConfig(id, fileFormat, path, delimiter, quote, escape, date, quoteMode)
      }.toOption

      subtype match {
        case ConfigVirtualSourceSybTypes.filterSql =>
          val sql = virtualSourceConfig.getString(ConfigVirtualSourceParameters.sql)
          id -> VirtualFileSelect(id, parentSources, sql, keyFields, save, persist)
        case ConfigVirtualSourceSybTypes.joinSql =>
          val sql = virtualSourceConfig.getString(ConfigVirtualSourceParameters.sql)
          id -> VirtualFileJoinSql(id, parentSources, sql, keyFields, save, persist)
        case ConfigVirtualSourceSybTypes.join =>
          val joinCols = virtualSourceConfig.getStringList(ConfigVirtualSourceParameters.joinColumns)
          val joinType = virtualSourceConfig.getString(ConfigVirtualSourceParameters.joinType)
          id -> VirtualFileJoin(id, parentSources, joinCols, joinType, keyFields, save, persist)
      }
    }.toMap

  /**
   * Returns a function to parse list of load checks
   * from configuration file
   * @param subtype load check subtype
   * @return function to parse load checks
   */
  private def loadCheckParser(subtype: String): List[ConfigObject] => Map[String, LoadCheck] =
    loadCheckList => loadCheckList.map { loadCheck =>
      // check for invalid virtual source parameter keys:
      if (loadCheck.keySet().exists(!ConfigLoadCheckParameters.contains(_)))
        throw IllegalParameterException(s"Config -> loadChecks -> $subtype: Invalid load check parameter keys are found.")

      val loadCheckConfig = loadCheck.toConfig
      val id = loadCheckConfig.getString(ConfigLoadCheckParameters.id)
      val source = loadCheckConfig.getString(ConfigLoadCheckParameters.source)

      subtype match {
        case ConfigLoadCheckSubTypes.exist =>
          val option = loadCheckConfig.getBoolean(ConfigLoadCheckParameters.option)
          source -> ExistLoadCheck(id, LoadCheckEnum.existLC.toString(), source, option)
        case ConfigLoadCheckSubTypes.encoding =>
          val option = loadCheckConfig.getString(ConfigLoadCheckParameters.option)
          source -> EncodingLoadCheck(id, LoadCheckEnum.encodingLC.toString(), source, option)
        case ConfigLoadCheckSubTypes.fileType =>
          val option = loadCheckConfig.getString(ConfigLoadCheckParameters.option)
          source -> FileTypeLoadCheck(id, LoadCheckEnum.fileTypeLC.toString(), source, option)
        case ConfigLoadCheckSubTypes.exactColNum =>
          val option = loadCheckConfig.getInt(ConfigLoadCheckParameters.option)
          source -> ColumnLoadCheck(id, LoadCheckEnum.columnNumLC.toString(), source, option)
        case ConfigLoadCheckSubTypes.minColNum =>
          val option = loadCheckConfig.getInt(ConfigLoadCheckParameters.option)
          source -> MinColumnLoadCheck(id, LoadCheckEnum.minColumnNumLC.toString(), source, option)
      }

    }.toMap

  /**
   * A function to parse file metrics
   * from configuration file
   * @return List of (id, file metric)
   */
  private def fileMetricParser(metricConfList: List[ConfigObject]): List[(String, Metric)] =
    metricConfList.map { metric =>
      // check for invalid metric parameter keys:
      if (metric.keySet().exists(!ConfigFileMetricParameters.contains(_)))
        throw IllegalParameterException(s"Config -> metrics -> file: Invalid file metric parameter keys are found.")

      val metricConf = metric.toConfig
      val id = metricConf.getString(ConfigFileMetricParameters.id)
      val name = camelToUnderscores(ConfigFileMetrics.rowCount).toUpperCase
      val description = Try(metricConf.getString(ConfigFileMetricParameters.description)).getOrElse("")
      val source = metricConf.getString(ConfigFileMetricParameters.source)
      source -> FileMetric(id, name, description, source, Map.empty)
    }

  /**
   * Returns a function to parse list of column metrics
   * from configuration file
   * @param subtype column metric subtype
   * @return function to parse column metrics
   */
  private def columnMetricParser(subtype: MetricConfig): List[ConfigObject] => List[(String, Metric)] =
    metricsList => metricsList.map { metric =>
      // check for invalid metric parameter keys:
      if (metric.keySet().exists(!ConfigColMetricParameters.contains(_)))
        throw IllegalParameterException(s"Config -> metrics -> column -> ${subtype.toString}: Invalid file metric parameter keys are found.")

      val metricConf = metric.toConfig
      val id = metricConf.getString(ConfigColMetricParameters.id)
      val name = camelToUnderscores(subtype.toString).toUpperCase
      val description = Try(metricConf.getString(ConfigColMetricParameters.description)).getOrElse("")
      val source = metricConf.getString(ConfigColMetricParameters.source)
      val columns = metricConf.getStringList(ConfigColMetricParameters.columns).toSeq.map(_.toLowerCase)
      val parameters: Map[String, Any] =
        if (subtype.parameters.isEmpty) Map.empty
        else
          Try(metricConf.getConfig("params")) match {
            case Success(paramConf) => subtype.parameters.map { parameter =>
              parameter.name -> paramGetter(parameter)(paramConf)
            }.filter(_._2 != None).toMap
            case Failure(_) =>
              if (subtype.parameters.forall(param => param.isOptional)) Map.empty
              else
                throw IllegalParameterException(s"Config -> metrics -> column -> ${subtype.toString} -> $id: Required parameters are not found.")
          }
      source -> ColumnMetric(id, name, description, source, columns, parameters, Seq.empty)
    }

  /**
   * A function to parse composed metrics
   * from configuration file
   * @return List of composed metrics
   */
  private def composedMetricParser(metricConfList: List[ConfigObject]): List[ComposedMetric] =
    metricConfList.map { metric =>
      // check for invalid metric parameter keys:
      if (metric.keySet().exists(!ConfigComposedMetricParameters.contains(_)))
        throw IllegalParameterException(s"Config -> metrics -> composed: Invalid composed metric parameter keys are found.")

      val metricConf = metric.toConfig
      val id = metricConf.getString(ConfigComposedMetricParameters.id)
      val description = Try(metricConf.getString(ConfigComposedMetricParameters.description)).getOrElse("")
      val formula = metricConf.getString(ConfigComposedMetricParameters.formula)
      ComposedMetric(id, "COMPOSED", description, formula, Map.empty)
    }

  /**
   * Returns a function to parse list of trend checks
   * from configuration file
   * @param subtype trend check subtype
   * @return function to parse trend checks
   */
  private def trendCheckParser(subtype: String): List[ConfigObject] => List[(Check, String)] =
    checkList => checkList.flatMap { check =>
      // check for invalid check parameter keys:
      if (check.keySet().exists(!ConfigCheckParameters.contains(_)))
        throw IllegalParameterException(s"Config -> checks -> trend: Invalid trend check parameter keys are found.")

      val checkConf = check.toConfig
      val id = checkConf.getString(ConfigCheckParameters.id)
      val description = Try(checkConf.getString(ConfigCheckParameters.description)).getOrElse("")
      val metric = checkConf.getString(ConfigCheckParameters.metric)
      val rule = checkConf.getString(ConfigCheckParameters.rule)
      val timeWindow = checkConf.getInt(ConfigCheckParameters.timeWindow)

      subtype match {
        case ConfigTrendChecks.averageBoundFullCheck =>
          val threshold = checkConf.getDouble(ConfigCheckParameters.threshold)
          List(
            AverageBoundFullCheck(id, description, Seq.empty, rule, threshold, timeWindow) -> metric
          )
        case ConfigTrendChecks.averageBoundLowerCheck =>
          val threshold = checkConf.getDouble(ConfigCheckParameters.threshold)
          List(
            AverageBoundLowerCheck(id, description, Seq.empty, rule, threshold, timeWindow) -> metric
          )
        case ConfigTrendChecks.averageBoundUpperCheck =>
          val threshold = checkConf.getDouble(ConfigCheckParameters.threshold)
          List(
            AverageBoundUpperCheck(id, description, Seq.empty, rule, threshold, timeWindow) -> metric
          )
        case ConfigTrendChecks.averageBoundRangeCheck =>
          val thresholdUpper = checkConf.getDouble(ConfigCheckParameters.thresholdUpper)
          val thresholdLower = checkConf.getDouble(ConfigCheckParameters.thresholdLower)
          List(
            AverageBoundRangeCheck(
              id, description, Seq.empty, rule, thresholdUpper, thresholdLower, timeWindow
            ) -> metric
          )
        case ConfigTrendChecks.topNRankCheck =>
          val threshold = checkConf.getDouble(ConfigCheckParameters.threshold)
          val targetNum = checkConf.getInt(ConfigCheckParameters.targetNumber)
          val topNCheck = TopNRankCheck(id, description, Seq.empty, rule, threshold, timeWindow)
          generateMetricSubId(metric, targetNum).map(topNCheck -> _)
      }
    }

  /**
   * Returns a function to parse list of snapshot checks
   * from configuration file
   * @param subtype snapshot check subtype
   * @return function to parse snapshot checks
   */
  private def snapshotCheckParser(subtype: String): List[ConfigObject] => List[(Check, String)] =
    checkList => checkList.flatMap { check =>
      // check for invalid check parameter keys:
      if (check.keySet().exists(!ConfigCheckParameters.contains(_)))
        throw IllegalParameterException(s"Config -> checks -> snapshot: Invalid snapshot check parameter keys are found.")

      val checkConf = check.toConfig
      val id = checkConf.getString(ConfigCheckParameters.id)
      val description = Try(checkConf.getString(ConfigCheckParameters.description)).getOrElse("")
      val metric = checkConf.getString(ConfigCheckParameters.metric)
      val threshold = Try(checkConf.getDouble(ConfigCheckParameters.threshold)).toOption
      val compareMetric = Try(checkConf.getString(ConfigCheckParameters.compareMetric)).toOption

      subtype match {
        case ConfigSnapshotChecks.equalTo => snapshotCheckGetter(EqualToThresholdCheck.apply, EqualToMetricCheck.apply)(
          id, description, metric, threshold, compareMetric
        )
        case ConfigSnapshotChecks.greaterThan => snapshotCheckGetter(GreaterThanThresholdCheck.apply, GreaterThanMetricCheck.apply)(
          id, description, metric, threshold, compareMetric
        )
        case ConfigSnapshotChecks.lessThan => snapshotCheckGetter(LessThanThresholdCheck.apply, LessThanMetricCheck.apply)(
          id, description, metric, threshold, compareMetric
        )
        case ConfigSnapshotChecks.differByLT => (threshold, compareMetric) match {
          case (Some(t), Some(cm)) => List(metric, cm).map(
            m => DifferByLTMetricCheck(id, description, Seq.empty, cm, t) -> m
          )
          case _ => throw MissingParameterInException(id)
        }

      }
    }

  /**
   * Returns a function to parse list of sql checks
   * from configuration file
   * @param subtype sql check subtype
   * @return function to parse sql checks
   */
  private def sqlCheckParser(subtype: String): List[ConfigObject] => List[SQLCheck] =
    sqlCheckList => sqlCheckList.map { check =>
      if (check.keySet().exists(!ConfigSqlCheckParameters.contains(_)))
        throw IllegalParameterException(s"Config -> checks -> sql: Invalid sql check parameter keys are found.")

      val checkConf = check.toConfig
      val id = checkConf.getString(ConfigSqlCheckParameters.id)
      val description = Try(checkConf.getString(ConfigSqlCheckParameters.description)).getOrElse("")
      val source = checkConf.getString(ConfigSqlCheckParameters.source)
      val query = checkConf.getString(ConfigSqlCheckParameters.query)
      val sourceConfig = this.dbConfigMap(this.sourcesConfigMap(source).asInstanceOf[TableConfig].dbId)

      val checkType = subtype match {
        case ConfigSqlChecks.countEqZero => "COUNT_EQ_ZERO"
        case ConfigSqlChecks.countNotEqZero => "COUNT_NOT_EQ_ZERO"
      }

      SQLCheck(id, description, checkType, source, sourceConfig, query)
    }

  /**
   * Returns a function to parse regular targets of given save type
   * from configuration file
   * @param targetType targets type
   * @param targetSubType target subtype
   * @return function to parse regular targets
   */
  private def targetParser(targetType: String,
                           targetSubType: String): Option[ConfigObject] => List[(String, TargetConfig)] = {
    case Some(targetObj) =>
      targetType match {
        case ConfigTargetsTypes.hdfs =>
          if (targetObj.keySet().exists(!ConfigHdfsTargetParameters.contains(_)))
            throw IllegalParameterException(s"Config -> targets -> hdfs: Invalid target parameter keys are found.")

          val targetConf = targetObj.toConfig
          val fileFormat = targetConf.getString(ConfigHdfsTargetParameters.fileFormat)
          val path = targetConf.getString(ConfigHdfsTargetParameters.path)
          val date = Try(targetConf.getString(ConfigHdfsTargetParameters.date)).toOption
          val delimiter = Try(targetConf.getString(ConfigHdfsTargetParameters.delimiter)).toOption
          val quote = Try(targetConf.getString(ConfigHdfsTargetParameters.quote)).toOption
          val escape = Try(targetConf.getString(ConfigHdfsTargetParameters.escape)).toOption
          val quoteMode =
            if (Try(targetConf.getBoolean(ConfigHdfsTargetParameters.quoted)).getOrElse(false)) Some("ALL")
            else Some("NONE")

          List(targetSubType -> HdfsTargetConfig(
            targetSubType, fileFormat, path, delimiter, quote, escape, date, quoteMode
          ))

        case ConfigTargetsTypes.hive =>
          if (targetObj.keySet().exists(!ConfigHiveTargetParameters.contains(_)))
            throw IllegalParameterException(s"Config -> targets -> hive: Invalid target parameter keys are found.")

          val targetConf = targetObj.toConfig
          val schema = targetConf.getString(ConfigHiveTargetParameters.schema)
          val table = targetConf.getString(ConfigHiveTargetParameters.table)
          val date = Try(targetConf.getString(ConfigHiveTargetParameters.date)).toOption
          val partitionColumn = Try(targetConf.getString(ConfigHiveTargetParameters.partitionColumn)).toOption
          List(targetSubType -> HiveTargetConfig(schema, table, date, partitionColumn))
        case ConfigTargetsTypes.errorCollection =>
          targetSubType match {
            case ConfigTargetsSubTypes.hdfs =>
              if (targetObj.keySet().exists(!ConfigHdfsTargetParameters.contains(_))) {
                throw IllegalParameterException(s"Config -> targets -> errorCollection -> hdfs: Invalid target parameter keys are found.")
              }

              val targetConf = targetObj.toConfig
              val metrics = Try(targetConf.getStringList(ConfigHdfsTargetParameters.metrics)).map(_.toSeq).getOrElse(Seq.empty)
              val dumpSize = Try(targetConf.getInt(ConfigHdfsTargetParameters.dumpSize)).getOrElse(100)
              val fileFormat = targetConf.getString(ConfigHdfsTargetParameters.fileFormat)
              val path = targetConf.getString(ConfigHdfsTargetParameters.path)
              val date = Try(targetConf.getString(ConfigHdfsTargetParameters.date)).toOption
              val delimiter = Try(targetConf.getString(ConfigHdfsTargetParameters.delimiter)).toOption
              val quote = Try(targetConf.getString(ConfigHdfsTargetParameters.quote)).toOption
              val escape = Try(targetConf.getString(ConfigHdfsTargetParameters.escape)).toOption
              val quoteMode =
                if (Try(targetConf.getBoolean(ConfigHdfsTargetParameters.quoted)).getOrElse(false)) Some("ALL")
                else Some("NONE")

              List(targetType -> ErrorCollectionHdfsTargetConfig(
                targetType, fileFormat, path, metrics, dumpSize, delimiter, quote, escape, date, quoteMode
              ))
            case ConfigTargetsSubTypes.kafka =>
              if (targetObj.keySet().exists(!ConfigErrorCollectionKafkaParameters.contains(_))) {
                throw IllegalParameterException(s"Config -> targets -> errorCollection -> kafka: Invalid target parameter keys are found.")
              }

              val targetConf = targetObj.toConfig
              val metrics = Try(targetConf.getStringList(ConfigErrorCollectionKafkaParameters.metrics)).map(_.toSeq).getOrElse(Seq.empty)
              val dumpSize = Try(targetConf.getInt(ConfigErrorCollectionKafkaParameters.dumpSize)).getOrElse(100)
              val topic = targetConf.getString(ConfigErrorCollectionKafkaParameters.topic)
              val brokerId = targetConf.getString(ConfigErrorCollectionKafkaParameters.brokerId)
              val date = Try(targetConf.getString(ConfigErrorCollectionKafkaParameters.date)).toOption
              val options = Try(targetConf.getStringList(ConfigErrorCollectionKafkaParameters.options).toSeq).getOrElse(Seq.empty)

              List(targetType -> ErrorCollectionKafkaTargetConfig(
                topic, brokerId, options, metrics, dumpSize, date
              ))
            case other => throw IllegalParameterException(s"Config -> targets -> errorCollection: Unsupported output channel: $other")
          }

        case ConfigTargetsTypes.summary =>
          targetSubType match {
            case ConfigTargetsSubTypes.email =>
              if (targetObj.keySet().exists(!ConfigSummaryEmailTargetParameters.contains(_)))
                throw IllegalParameterException(s"Config -> targets -> summary -> email: Invalid target parameter keys are found.")

              val targetConf = targetObj.toConfig
              val mailingList = targetConf.getStringList(ConfigSummaryEmailTargetParameters.mailingList).toSeq
              val metrics = Try(targetConf.getStringList(ConfigSummaryEmailTargetParameters.metrics)).map(_.toSeq).getOrElse(Seq.empty)
              val dumpSize = Try(targetConf.getInt(ConfigSummaryEmailTargetParameters.dumpSize)).getOrElse(100)
              val attachErrors = Try(targetConf.getBoolean(ConfigSummaryEmailTargetParameters.attachMetricErrors)).getOrElse(false)

              List(targetType -> SummaryEmailTargetConfig(mailingList, metrics, dumpSize, attachErrors))
            case ConfigTargetsSubTypes.mattermost =>
              if (targetObj.keySet().exists(!ConfigSummaryMMTargetParameters.contains(_)))
                throw IllegalParameterException(s"Config -> targets -> summary -> email: Invalid target parameter keys are found.")

              val targetConf = targetObj.toConfig
              val recipients = targetConf.getStringList(ConfigSummaryMMTargetParameters.recipients).toSeq
              val metrics = Try(targetConf.getStringList(ConfigSummaryMMTargetParameters.metrics)).map(_.toSeq).getOrElse(Seq.empty)
              val dumpSize = Try(targetConf.getInt(ConfigSummaryMMTargetParameters.dumpSize)).getOrElse(100)
              val attachErrors = Try(targetConf.getBoolean(ConfigSummaryMMTargetParameters.attachMetricErrors)).getOrElse(false)

              List(targetType -> SummaryMMTargetConfig(recipients, metrics, dumpSize, attachErrors))
            case ConfigTargetsSubTypes.kafka =>
              if (targetObj.keySet().exists(!ConfigSummaryKafkaTargetParameters.contains(_)))
                throw IllegalParameterException(s"Config -> targets -> summary -> kafka: Invalid target parameter keys are found.")

              val targetConf = targetObj.toConfig
              val topic = targetConf.getString(ConfigSummaryKafkaTargetParameters.topic)
              val brokerId = targetConf.getString(ConfigSummaryKafkaTargetParameters.brokerId)
              val options = Try(targetConf.getStringList(ConfigSummaryKafkaTargetParameters.options).toSeq).getOrElse(Seq.empty)

              List(targetType -> SummaryKafkaTargetConfig(topic, brokerId, options))
            case other => throw IllegalParameterException(s"Config -> targets -> summary: Unsupported output channel: $other")
          }
        case ConfigTargetsTypes.checkAlerts =>
          targetSubType match {
            case ConfigTargetsSubTypes.email =>
              if (targetObj.keySet().exists(!ConfigEmailAlertTargetParameters.contains(_)))
                throw IllegalParameterException(s"Config -> targets -> checkAlerts -> email: Invalid target parameter keys are found.")

              val targetConf = targetObj.toConfig
              val id = targetConf.getString(ConfigEmailAlertTargetParameters.id)
              val mailingList = targetConf.getStringList(ConfigEmailAlertTargetParameters.mailingList).toSeq
              val checks = Try(targetConf.getStringList(ConfigEmailAlertTargetParameters.checks).toSeq).getOrElse(Seq.empty)

              List(targetType -> CheckAlertEmailTargetConfig(id, checks, mailingList))
            case ConfigTargetsSubTypes.mattermost =>
              if (targetObj.keySet().exists(!ConfigMMAlertTargetParameters.contains(_)))
                throw IllegalParameterException(s"Config -> targets -> checkAlerts -> mattermost: Invalid target parameter keys are found.")

              val targetConf = targetObj.toConfig
              val id = targetConf.getString(ConfigMMAlertTargetParameters.id)
              val recipients = targetConf.getStringList(ConfigMMAlertTargetParameters.recipients).toSeq
              val checks = Try(targetConf.getStringList(ConfigMMAlertTargetParameters.checks).toSeq).getOrElse(Seq.empty)

              List(targetType -> CheckAlertMMTargetConfig(id, checks, recipients))
            case ConfigTargetsSubTypes.kafka =>
              if (targetObj.keySet().exists(!ConfigKafkaAlertTargetParameters.contains(_)))
                throw IllegalParameterException(s"Config -> targets -> checkAlerts -> kafka: Invalid target parameter keys are found.")

              val targetConf = targetObj.toConfig
              val id = targetConf.getString(ConfigKafkaAlertTargetParameters.id)
              val checks = Try(targetConf.getStringList(ConfigKafkaAlertTargetParameters.checks).toSeq).getOrElse(Seq.empty)
              val topic = targetConf.getString(ConfigKafkaAlertTargetParameters.topic)
              val brokerId = targetConf.getString(ConfigKafkaAlertTargetParameters.brokerId)
              val options = Try(targetConf.getStringList(ConfigKafkaAlertTargetParameters.options).toSeq).getOrElse(Seq.empty)

              List(targetType -> CheckAlertKafkaTargetConfig(id, checks, topic, brokerId, options))
            case other => throw IllegalParameterException(s"Config -> targets -> checkAlerts: Unsupported output channel: $other")
          }
        case ConfigTargetsTypes.results =>
          if (targetObj.keySet().exists(!ConfigKafkaResultTargetParameters.contains(_))) {
            throw IllegalParameterException(s"Config -> targets -> results -> kafka: Invalid target parameter keys are found.")
          }

          val targetConf = targetObj.toConfig
          val results = targetConf.getStringList(ConfigKafkaResultTargetParameters.results).toSeq
            .map(ResultTargets.withNameOpt(_).get)
          val topic = targetConf.getString(ConfigKafkaResultTargetParameters.topic)
          val brokerId = targetConf.getString(ConfigKafkaResultTargetParameters.brokerId)
          val options = Try(targetConf.getStringList(ConfigKafkaResultTargetParameters.options).toSeq).getOrElse(Seq.empty)

          List(targetType -> ResultsKafkaTargetConfig(results, topic, brokerId, options))
      }
    case None => List.empty
  }

  /**
   * Returns config getter for the metric parameter
   * @param param - metric parameter (name, type, isOptional)
   * @return - config getter for given parameter
   */
  private def paramGetter(param: MetricParameter): Config => Any = {
    conf => {
      val paramTry = param.typ match {
        case "string" => Try(conf.getString(param.name))
        case "stringList" => Try(conf.getStringList(param.name).toList)
        case "integer" => Try(conf.getInt(param.name))
        case "integerList" => Try(conf.getIntList(param.name).toList)
        case "double" => Try(conf.getDouble(param.name))
        case "doubleList" => Try(conf.getDoubleList(param.name).toList)
        case "boolean" => Try(conf.getBoolean(param.name))
        case "booleanList" => Try(conf.getBooleanList(param.name).toList)
      }
      // sage value extraction for optional parameters and throwable extract for required once:
      if (param.isOptional) paramTry.getOrElse(None) else paramTry.get
    }
  }

  /**
   * Chained function to get whether threshold comparable check or metric comparable check
   * given two corresponding check case classes. Check parameters are provided in a second parameter list.
   * @param thresholdCheck - case class for threshold comparable check.
   * @param compareMetricCheck - case class for metric comparable class.
   * @param id - check id
   * @param description - check description
   * @param metric - metric to check
   * @param threshold - optional threshold to comapre with
   * @param compareMetric - optional metric to compare with
   * @return - List of checks by metrics.
   */
  private def snapshotCheckGetter(
                                   thresholdCheck: (String, String, Seq[MetricResult], Double) => Check,
                                   compareMetricCheck: (String, String, Seq[MetricResult], String) => Check
                                 )(id: String,
                                   description: String,
                                   metric: String,
                                   threshold: Option[Double],
                                   compareMetric: Option[String]
                                 ): List[(Check, String)] = (threshold, compareMetric) match {
    case (Some(t), None) => List(thresholdCheck(id, description, Seq.empty, t) -> metric)
    case (None, Some(cm)) => List(metric, cm).map(
      m => compareMetricCheck(id, description, Seq.empty, cm) -> m
    )
    case _ => throw IllegalParameterException(id)
  }
}
