package ru.raiffeisen.checkita.apps

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.raiffeisen.checkita.checks.{CheckResult, LoadCheckResult}
import ru.raiffeisen.checkita.checks.load.{ExeEnum, LoadCheck}
import ru.raiffeisen.checkita.configs.ConfigReader
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.metrics.MetricProcessor.MetricErrors
import ru.raiffeisen.checkita.metrics.{ColumnMetric, ColumnMetricResult, ComposedMetricCalculator, ComposedMetricResult, FileMetric, FileMetricResult, MetricProcessor, MetricResult, TypedResult}
import ru.raiffeisen.checkita.sources.VirtualSourceProcessor.getActualSources
import ru.raiffeisen.checkita.sources.{HdfsFile, HiveTableConfig, KafkaSourceConfig, Source, TableConfig}
import ru.raiffeisen.checkita.targets.{CheckAlertEmailTargetConfig, CheckAlertKafkaTargetConfig, CheckAlertMMTargetConfig, ErrorCollectionHdfsTargetConfig, ErrorCollectionKafkaTargetConfig, HdfsTargetConfig, HiveTargetConfig, ResultsKafkaTargetConfig, SummaryEmailTargetConfig, SummaryKafkaTargetConfig, SummaryMMTargetConfig}
import ru.raiffeisen.checkita.utils.enums.ResultTargets
import ru.raiffeisen.checkita.utils.io.{HdfsReader, HdfsWriter, HiveReader}
import ru.raiffeisen.checkita.utils.io.dbmanager.DBManager
import ru.raiffeisen.checkita.utils.io.kafka.{KafkaManager, KafkaOutput}
import ru.raiffeisen.checkita.utils.io.mattermost.MMManager
import ru.raiffeisen.checkita.utils.mailing.NotificationManager
import ru.raiffeisen.checkita.utils.{DQMainClass, DQSettings, DQSparkContext, Logging, mapToJsonString, saveErrors, sendErrorsToKafka}

import java.sql.Connection
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object DQMasterBatch extends DQMainClass with DQSparkContext with Logging {

  override protected def body()(implicit fs: FileSystem,
                                sparkContext: SparkContext,
                                sparkSession: SparkSession,
                                resultsWriter: DBManager,
                                settings: DQSettings): Boolean = {

    /**
     * Configuration file parsing
     */
    log.info(s"[STAGE 1/${settings.stageNum}] Parsing configuration file...")
    log.info("Path: " + settings.jobConf)
    log.info("Default runtime variables are added into configuration file:")
    log.info(s"referenceDateTime=${settings.referenceDateString}, executionDateTime=${settings.executionDateString}")
    log.info(s"Following variables are provided via commandline on startup and added into configuration file:")
    log.info(settings.extraVars.keys.mkString("[", ", ", "] (values are not shown intentionally)"))

    implicit val configuration: ConfigReader = ConfigReader(settings.jobConf)

    log.info(s"Calculating metrics per JobID: ${configuration.jobId}")
    log.info(s"External database list (size ${configuration.dbConfigMap.size}):")
    configuration.dbConfigMap.par.foreach(x => log.debug(s"  ${x._1} -> ${x._2}"))
    log.info(s"Message Brokers list (size ${configuration.brokersConfigMap.size}):")
    configuration.brokersConfigMap.par.foreach(x => log.debug(s"  ${x._1} -> ${x._2}"))
    log.info(s"Source list (size ${configuration.sourcesConfigMap.size}):")
    configuration.sourcesConfigMap.par.foreach(x => log.debug(s"  ${x._1} -> ${x._2}"))
    log.info(s"Virtual source list (size ${configuration.virtualSourcesConfigMap.size}):")
    configuration.virtualSourcesConfigMap.par.foreach(x => log.debug(s"  ${x._1} -> ${x._2}"))
    log.info(s"Metrics list (size ${configuration.metricsBySourceList.size}):")
    configuration.metricsBySourceList.par.foreach(x => log.debug(s"  ${x._1} -> ${x._2}"))
    log.info(s"Checks list (size ${configuration.metricsByChecksList.size}):")
    configuration.metricsByChecksList.par.foreach(x => log.debug(s"  ${x._2} -> ${x._1}"))
    log.info(s"Targets list (size ${configuration.targetsConfigMap.size}):")
    configuration.targetsConfigMap.par.foreach(x => log.debug(s"  ${x._1} -> ${x._2}"))

    /**
     * Database connection management
     */
    log.info(s"[STAGE 2/${settings.stageNum}] Connecting to external databases...")
    val dbConnections: Map[String, Connection] =
      configuration.dbConfigMap.flatMap({ db =>
        log.info("Trying to connect to " + db._1)
        val res = Try(db._1 -> db._2.getConnection).toOption
        res match {
          case Some(_) => log.info("Connection successful")
          case None    => log.warn("Connection failed")
        }
        res
      })

    /**
     * Creating kafka managers
     */
    log.info(s"[STAGE 2/${settings.stageNum}] Connecting to kafka brokers...")
    val kafkaManagers: Map[String, KafkaManager] =
      configuration.brokersConfigMap.flatMap { broker =>
        val connection = KafkaManager(broker._2)
        connection.checkConnection match {
          case Success(_) =>
            log.info(s"Kafka broker ${broker._1}: Connection successful.")
            Some(broker._1 -> connection)
          case Failure(error) =>
            log.info(s"Kafka broker ${broker._1}: Connection failed with following error:")
            throw error
        }
      }

    /**
     * Source loading
     */
    log.info(s"[STAGE 3/${settings.stageNum}] Loading data...")
    val (sources: Seq[Source], lcResults: Seq[LoadCheckResult]) = configuration.sourcesConfigMap
      .map {
        case (source, conf) =>
          conf match {
            case hdfsFile: HdfsFile =>
              val loadChecks: Seq[LoadCheck] = configuration.loadChecksMap.getOrElse(source, Seq.empty[LoadCheck])

              val preLoadRes: Seq[LoadCheckResult] =
                loadChecks.filter(_.exeType == ExeEnum.pre).map(x => x.run(Some(hdfsFile))(fs, sparkSession, settings))

              val src: Seq[Source] = HdfsReader
                .load(hdfsFile).map(df => Source(source, df, conf.keyFields))

              val postLoadRes: Seq[LoadCheckResult] = loadChecks
                .filter(_.exeType == ExeEnum.post)
                .map(x => x.run(None, Try(src.head.df).toOption)(fs, sparkSession, settings))

              (src, preLoadRes ++ postLoadRes)
            case hiveTableConfig: HiveTableConfig =>
              val src: Seq[Source] = HiveReader
                .loadHiveTable(hiveTableConfig)(sparkSession)
                .map(df => Source(source, df, conf.keyFields))
              (src, Seq.empty[LoadCheckResult])
            //            case hbConf: HBaseSrcConfig =>
            //              (
            //                Seq(Source(source, settings.refDateString, HBaseLoader.loadToDF(hbConf), conf.keyFields)),
            //                Seq.empty[LoadCheckResult]
            //              )
            case tableConf: TableConfig =>
              val databaseConfig = configuration.dbConfigMap(tableConf.dbId)
              log.info(s"Loading table ${tableConf.table} from ${tableConf.dbId}")
              val df: DataFrame =
                (tableConf.password, tableConf.password) match {
                  case (Some(u), Some(p)) =>
                    databaseConfig.loadData(tableConf.table, Some(u), Some(p))
                  case _ =>
                    databaseConfig.loadData(tableConf.table)
                }
              (Seq(Source(source, df, conf.keyFields)), Seq.empty[LoadCheckResult])
            case kafkaConf: KafkaSourceConfig =>
              val manager = kafkaManagers(kafkaConf.brokerId)
              log.info(
                s"Loading kafka topic(s) ${kafkaConf.topicPattern.getOrElse(kafkaConf.topics.get.mkString(","))} " +
                  s"from brokerId ${kafkaConf.brokerId}..."
              )

              Try(manager.loadData(kafkaConf)) match {
                case Success(df) =>
                  (Seq(Source(source, df, kafkaConf.keyFields)), Seq.empty[LoadCheckResult])
                case Failure(_) =>
                  log.info("Failed to load from kafka topic.")
                  (Seq.empty[Source], Seq.empty[LoadCheckResult])
              }

            case x => throw IllegalParameterException(x.getType.toString)
          }
      }
      .foldLeft((Seq.empty[Source], Seq.empty[LoadCheckResult]))((x, y) => (x._1 ++ y._1, x._2 ++ y._2))

    resultsWriter.saveResultsToDB(lcResults.map(_.toDbFormat), "results_check_load")
    if (sources.length != configuration.sourcesConfigMap.size) {
      val failSrc: Seq[String] = configuration.sourcesConfigMap
        .filterNot(x => sources.map(_.id).toSet.contains(x._1)).map(x =>s"- ${x._1}: ${x._2.getType}").toSeq
      val additional = failSrc.mkString(s"Failed sources: ${failSrc.size}\n","\n","")

      log.error(additional)

      return false
    }

    val sourceMap: Map[String, Source] = sources.map(x => (x.id, x)).toMap
    val vsToSave: Map[String, HdfsTargetConfig] = configuration.virtualSourcesConfigMap
      .filter(_._2.saveTarget.isDefined).mapValues(_.saveTarget.get)
    // virtualSources will contains both direct sources and virtual sources:
    val virtualSources: Seq[Source]    = getActualSources(configuration.virtualSourcesConfigMap, sourceMap).values.toSeq

    log.info("Saving required sources...")
    virtualSources.foreach {
      case src if vsToSave.contains(src.id) =>
        HdfsWriter.saveDF(vsToSave(src.id), src.df)
        log.info(s"Virtual source ${src.id} was saved.")
      case src =>
        log.info(s"Virtual source ${src.id} will not be saved.")
      case _ =>
    }

    /**
     * Metrics calculation
     */
    log.info(s"[STAGE 4/${settings.stageNum}] Calculating metrics...")
    val allMetrics: Seq[(String,
      Map[Seq[String], Map[ColumnMetric, (Double, Option[String])]],
      Map[FileMetric, (Double, Option[String])], MetricErrors)] =
      virtualSources.map(source => {
        log.info(s"Calculating metrics for ${source.id}")

        //select all file metrics to do on this source
        val fileMetrics: Seq[FileMetric] =
          configuration.metricsBySourceMap.getOrElse(source.id, Nil).collect {
            case metric: FileMetric =>
              FileMetric(metric.id, metric.name, metric.description, metric.source, metric.paramMap)
          }
        log.info(s"Found file metrics: ${fileMetrics.size}")

        //select all columnar metrics to do on this source
        val colMetrics =
          configuration.metricsBySourceMap.getOrElse(source.id, Nil).collect {
            case metric: ColumnMetric =>
              ColumnMetric(metric.id,
                metric.name,
                metric.description,
                metric.source,
                metric.columns,
                metric.paramMap,
                metric.positions)
          }
        log.info(s"Found column metrics: ${colMetrics.size}")

        if (fileMetrics.isEmpty && colMetrics.isEmpty) {
          (
            source.id,
            Map.empty[Seq[String], Map[ColumnMetric, (Double, Option[String])]],
            Map.empty[FileMetric, (Double, Option[String])],
            Map.empty[String, (Seq[String], mutable.Seq[Seq[String]])]
          )
        } else {
          //compute all metrics
          val results: (Map[Seq[String], Map[ColumnMetric, (Double, Option[String])]],
            Map[FileMetric, (Double, Option[String])], MetricErrors) =
            MetricProcessor.processAllMetrics(source.df, colMetrics, fileMetrics, source.keyFields)

          source.df.unpersist()

          (source.id, results._1, results._2, results._3)
        }
      })

    // aggregated metric errors:
    val aggregatedMetricErrors = allMetrics.flatMap(_._4).toMap

    //    if (aggregatedMetricErrors.nonEmpty) saveErrors(aggregatedMetricErrors)


    /**
     * CREATE METRIC RESULTS (for checks)
     */
    val colMetricResultsList: Seq[ColumnMetricResult] =
      allMetrics.flatMap {
        case (_, resultsOnColumns, _, _) =>
          resultsOnColumns.flatMap {
            case (colIds, metricResultsMap) =>
              metricResultsMap.map { mr =>
                ColumnMetricResult(
                  configuration.jobId,
                  mr._1.id,
                  mr._1.name,
                  mr._1.description,
                  mr._1.source,
                  colIds,
                  mapToJsonString(mr._1.paramMap),
                  mr._2._1,
                  mr._2._2.getOrElse("")
                )
              }
          }.toList
      }

    val fileMetricResultsList: Seq[FileMetricResult] =
      allMetrics.flatMap {
        case (_, _, resultsOnfile, _) =>
          resultsOnfile.map {
            case (fileMetric, metricCalc) =>
              FileMetricResult(
                configuration.jobId,
                fileMetric.id,
                fileMetric.name,
                fileMetric.description,
                fileMetric.source,
                metricCalc._1,
                metricCalc._2.getOrElse("")
              )
          }
      }

    val primitiveMetricResults = fileMetricResultsList ++ colMetricResultsList

    /**
     * CALCULATING COMPOSED METRICS
     */
    log.info(s"[STAGE 5/${settings.stageNum}] Calculating composed metrics...")
    // todo: It's possible to calculate composed using $primitiveMetricResults as zero value to avoid extra merges
    val composedMetricResults: Seq[ComposedMetricResult] =
      configuration.composedMetrics
        .foldLeft[Seq[ComposedMetricResult]](Seq.empty[ComposedMetricResult])((accum, curr) => {
          log.info(s"Calculating ${curr.id} with formula ${curr.formula}")
          val composedMetricCalculator =
            new ComposedMetricCalculator(primitiveMetricResults ++ accum)
          val currRes: ComposedMetricResult =
            composedMetricCalculator.run(curr)
          accum ++ Seq(currRes)
        })

    val allMetricResults: Seq[MetricResult] = primitiveMetricResults ++ composedMetricResults

    /**
     * DEFINE and PERFORM CHECKS
     */
    log.info(s"[STAGE 6/${settings.stageNum}] Performing checks...")
    val buildChecks = configuration.metricsByCheckMap.map {
      case (check, metricList) =>
        val resList = metricList.flatMap { mId =>
          val ll = allMetricResults.filter(_.metricId == mId)
          if (ll.size == 1) Option(ll.head) else None
        }
        check.addMetricList(resList)
    }.toSeq

    val createdChecks = buildChecks.map(cmr => s"${cmr.id}, ${cmr.getMetrics} - ${cmr.getDescription}")
    log.info(s" * Checks created: ${createdChecks.size}")
    createdChecks.foreach(str => log.info(str))

    val checkResults: Seq[CheckResult] = buildChecks
      .flatMap { e =>
        Try {
          e.run
        } match {
          case Success(res) => Some(res)
          case Failure(e) =>
            log.error(e.toString)
            None
        }
      }

    log.info(s"Check Results:")
    checkResults.foreach(cr => log.info("  " + cr.message))

    /**
     * PERFORM SQL CHECKS
     */
    log.info(s"[STAGE 7/${settings.stageNum}] Performing SQL checks...")
    val sqlCheckResults: List[CheckResult] =
      configuration.sqlChecksList.map(check => {
        log.info("Calculating " + check.id + " " + check.description)

        //todo make more elegant fix
        val sourceConf = configuration.sourcesConfigMap(check.source)
        sourceConf match {
          case tableConf: TableConfig =>
            val databaseConfig = configuration.dbConfigMap(tableConf.dbId)
            check.executeCheck(dbConnections(databaseConfig.id))
          case _ => throw new IllegalArgumentException(s"Unsupported source type: ${sourceConf.getType}.")
        }
      })

    // Closing db connections
    dbConnections.values.foreach(_.close())

    val finalCheckResults: Seq[CheckResult] = checkResults ++ sqlCheckResults

    log.info(s"[STAGE 8/${settings.stageNum}] Processing results...")
    log.info(s"Saving results to the database...")
    log.info(s"With reference date: ${settings.referenceDateString}")
    log.info(s"With execution date: ${settings.executionDateString}")
    resultsWriter.saveResultsToDB(colMetricResultsList.map(_.toDbFormat), "results_metric_columnar")
    resultsWriter.saveResultsToDB(fileMetricResultsList.map(_.toDbFormat), "results_metric_file")
    resultsWriter.saveResultsToDB(composedMetricResults.map(_.toDbFormat), "results_metric_composed")
    resultsWriter.saveResultsToDB(finalCheckResults.map(_.toDbFormat), "results_check")

    val targetResultMap: Map[String, Seq[Product with Serializable with TypedResult]] = Map(
      settings.backComp.fileMetricTargetType -> fileMetricResultsList,
      settings.backComp.columnMetricTargetType -> colMetricResultsList,
      settings.backComp.composedMetricTargetType -> composedMetricResults,
      settings.backComp.checkTargetType -> finalCheckResults,
      settings.backComp.loadCheckTargetType -> lcResults
    )

    log.info("Processing targets...")

    // initiate mattermost manager if needed:
    val countMMNotifications = configuration.targetsConfigMap.values.foldLeft(0) { (a, t) =>
      a + t.count(c => c.isInstanceOf[SummaryMMTargetConfig] || c.isInstanceOf[CheckAlertMMTargetConfig])
    }

    implicit val mmManger: Option[MMManager] = if (countMMNotifications > 0 && settings.mmConfig.nonEmpty) {
      log.info("Connecting to Mattermost...")
      Some(new MMManager(settings.mmConfig.get))
    } else None

    configuration.targetsConfigMap.foreach(tar =>
      tar._1.toUpperCase match {
        case "SUMMARY" =>
          log.info(s"Sending summary report...")
          tar._2.foreach{
            case emailConf: SummaryEmailTargetConfig => NotificationManager.sendSummary(
              emailConf,
              finalCheckResults,
              lcResults,
              aggregatedMetricErrors
            )
            case mmConf: SummaryMMTargetConfig => NotificationManager.sendSummaryToMM(
              mmConf,
              finalCheckResults,
              lcResults,
              aggregatedMetricErrors
            )
            case kafkaConf: SummaryKafkaTargetConfig => NotificationManager.sendSummaryToKafka(
              kafkaConf,
              finalCheckResults,
              lcResults,
              kafkaManagers(kafkaConf.brokerId)
            )
          }
        case "CHECKALERTS" =>
          tar._2.foreach {
            case emailConf: CheckAlertEmailTargetConfig => NotificationManager.sendEmailCheckAlerts(
              emailConf, finalCheckResults, lcResults
            )
            case mmConf: CheckAlertMMTargetConfig => NotificationManager.sendMMCheckAlerts(
              mmConf, finalCheckResults, lcResults
            )
            case kafkaConfig: CheckAlertKafkaTargetConfig => NotificationManager.sendKafkaCheckAlerts(
              kafkaConfig, finalCheckResults, lcResults, kafkaManagers(kafkaConfig.brokerId)
            )
          }
        case "ERRORCOLLECTION" => tar._2.foreach {
          case hdfsConf: ErrorCollectionHdfsTargetConfig => saveErrors(
            hdfsConf, aggregatedMetricErrors, configuration.jobId
          )
          case kafkaConf: ErrorCollectionKafkaTargetConfig => sendErrorsToKafka(
            kafkaConf, aggregatedMetricErrors, configuration.jobId, kafkaManagers(kafkaConf.brokerId)
          )
        }
        case "RESULTS" => tar._2.foreach { conf =>
          val resultsConf = conf.asInstanceOf[ResultsKafkaTargetConfig]
          val messages = resultsConf.results.flatMap {
            case ResultTargets.columnMetrics => colMetricResultsList.map(_.toJsonString)
            case ResultTargets.fileMetrics => fileMetricResultsList.map(_.toJsonString)
            case ResultTargets.composedMetrics => composedMetricResults.map(_.toJsonString)
            case ResultTargets.loadChecks => lcResults.map(_.toJsonString(configuration.jobId))
            case ResultTargets.checks => finalCheckResults.map(_.toJsonString(configuration.jobId))
          }
          val explicitEntity = s"results@${configuration.jobId}"
          val data = KafkaOutput(messages, resultsConf.topic, resultsConf.options, Some(explicitEntity))
          val response = kafkaManagers(resultsConf.brokerId).writeData(data)

          if (response.isLeft) throw new RuntimeException(
            "Error while writing results to kafka with following error messages:\n" + response.left.get.mkString("\n")
          ) else log.info(s"Results have been successfully sent to Kafka broker with id = ${resultsConf.brokerId}")
        }

        case targetType =>
          log.info(s"Saving targets for $targetType...")
          tar._2.foreach {
            case hiveConf: HiveTargetConfig => HdfsWriter.saveHive(hiveConf, targetResultMap(tar._1))
            case hdfsCong: HdfsTargetConfig => HdfsWriter.saveHdfs(hdfsCong, targetResultMap(tar._1))
          }
      })

    true
  }
}
