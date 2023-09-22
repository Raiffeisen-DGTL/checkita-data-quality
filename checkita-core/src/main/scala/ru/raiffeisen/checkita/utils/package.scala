package ru.raiffeisen.checkita

import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.metrics.ColumnMetricResult
import ru.raiffeisen.checkita.metrics.MetricProcessor.{MetricErrors, ParamMap}
import ru.raiffeisen.checkita.sources.Source
import ru.raiffeisen.checkita.targets._
import ru.raiffeisen.checkita.utils.io.HdfsWriter
import ru.raiffeisen.checkita.utils.io.kafka.{KafkaManager, KafkaOutput}
import ru.raiffeisen.checkita.utils.mailing.{Mail, MailerConfiguration}

import java.security.MessageDigest
import java.sql.Timestamp
import java.text.DecimalFormat
import java.time.{ZoneId, ZonedDateTime}
import scala.collection.immutable.TreeMap
import scala.util.Try
import scala.util.matching.Regex

package object utils extends Logging {
  val doubleFractionFormat: Int = 13
  
  def parseTargetConfig(config: Config)(implicit settings: DQSettings): Option[HdfsTargetConfig] = {
    Try {
      val name: Option[String] = Try(config.getString("fileName")).toOption
      val format               = config.getString("fileFormat")
      val path                 = config.getString("path")

      val delimiter = settings.backComp.delimiterExtractor(config)
      val quote     = Try(config.getString("quote")).toOption
      val escape    = Try(config.getString("escape")).toOption

      val quoteMode = settings.backComp.quoteModeExtractor(config)

      HdfsTargetConfig(name.getOrElse(""),
        format,
        path,
        delimiter = delimiter,
        quote = quote,
        escape = escape,
        quoteMode = quoteMode)
    }.toOption
  }

  def saveErrors(conf: ErrorCollectionHdfsTargetConfig,
                 content: MetricErrors,
                 metricResults: Seq[ColumnMetricResult],
                 sources: Seq[Source],
                 jobId: String)(implicit fs: FileSystem,
                                sc: SparkContext,
                                sparkSes: SparkSession,
                                settings: DQSettings): Unit = {
    if (content.nonEmpty) {
      log.info(s"Saving metric errors...")
      val baseHeader: StructType =
        StructType(Seq("METRIC_ID", "SOURCE_ID", "SOURCE_KEY_FIELDS", "METRIC_COLUMNS", "ROW_DATA")
          .map(StructField(_, StringType)))

      val requestedMetrics = conf.metrics

      val data = content.flatMap { m =>
        val metIdWithCols = m._1.split("%~%", -1).toSeq
        val metId = metIdWithCols.head
        val metCols = metIdWithCols.tail.mkString("[", ", ", "]")
        val sourceId = metricResults.filter(_.metricId == metId).head.sourceId
        val keyFields = sources.filter(_.id == sourceId).head.keyFields.mkString("[\"", "\", \"", "\"]")

        val rowCols = m._2._1

        if (requestedMetrics.isEmpty || requestedMetrics.contains(metId))
          m._2._2.zipWithIndex.filter(_._2 < conf.dumpSize).map(_._1).map { rowData =>
            val rowJson = rowCols.zip(rowData).map {
              case (k, v) => "\"" + k + "\": \"" + v + "\""
            }.mkString("{", ", ", "}")
            Row.fromSeq(Seq(metId, sourceId, keyFields, metCols, rowJson))
          }
        else Seq.empty[Row]
      }.toSeq

      val baseRDD: RDD[Row] = sparkSes.sparkContext.parallelize(data)

      val baseDF = sparkSes.createDataFrame(baseRDD, baseHeader)

      val tarConf = HdfsTargetConfig(
        fileName = s"metricErrors_$jobId",
        fileFormat = conf.fileFormat,
        path = conf.path,
        delimiter = conf.delimiter,
        escape = conf.escape,
        quote = conf.quote,
        quoteMode = conf.quoteMode)

      HdfsWriter.saveDF(tarConf, baseDF)
    }
  }

  def sendErrorsToKafka(conf: ErrorCollectionKafkaTargetConfig,
                        content: MetricErrors,
                        jobId: String,
                        metricResults: Seq[ColumnMetricResult],
                        sources: Seq[Source],
                        writer: KafkaManager)(implicit settings: DQSettings): Unit = {
    if (content.nonEmpty) {
      log.info(s"Saving metric errors...")

      val requestedMetrics = conf.metrics

      val messages = content.flatMap { m =>
        val metIdWithCols = m._1.split("%~%", -1).toSeq
        val metId = metIdWithCols.head
        val metCols = metIdWithCols.tail.mkString("[\"", "\", \"", "\"]")
        val sourceId = metricResults.filter(_.metricId == metId).head.sourceId
        val keyFields = sources.filter(_.id == sourceId).head.keyFields.mkString("[\"", "\", \"", "\"]")
        
        val rowCols = m._2._1

        if (requestedMetrics.isEmpty || requestedMetrics.contains(metId))
          m._2._2.zipWithIndex.filter(_._2 < conf.dumpSize).map(_._1).map { rowData =>
            val rowJson = rowCols.zip(rowData).map {
              case (k, v) => "\"" + k + "\": \"" + v + "\""
            }.mkString("{", ", ", "}")
            s"""{
               |  "entityType": "metricErrors",
               |  "data": {
               |    "jobId": "$jobId",
               |    "referenceDate": "${conf.date.getOrElse(settings.referenceDateString)}",
               |    "metricId": "$metId",
               |    "sourceId": "$sourceId",
               |    "sourceKeyFields": $keyFields,
               |    "metricColumns": $metCols,
               |    "rowData": $rowJson
               |  }
               |}""".stripMargin
          }
        else Seq.empty[String]
      }.toSeq

      val explicitEntity = s"errorCollection@$jobId"
      val data = KafkaOutput(messages, conf.topic, conf.options, Some(explicitEntity))

      val response = writer.writeData(data)

      if (response.isLeft) throw new RuntimeException(
        "Error while writing metric errors to kafka with following error messages:\n" + response.left.get.mkString("\n")
      ) else log.info(s"Metric errors have been successfully sent to Kafka broker with id = ${conf.brokerId}")
    }
  }

  def sendCheckAlertMail(recievers: Seq[String],
                         text: Option[String],
                         subject: Option[String],
                         attachments: Seq[BinaryAttachment])(implicit mailer: MailerConfiguration): Unit = {

    val defaultText = "Some of requested checks failed. Please, check attached csv."
    
    val defaultSubject = "Data Quality failed check alert"
    
    Mail a Mail(
      from = (mailer.address, mailer.name),
      to = recievers,
      subject = subject.getOrElse(defaultSubject),
      message = text.getOrElse(defaultText),
      attachment = attachments
    )
  }

  def sendBashMail(numOfFailedChecks: Int,
                   failedCheckIds: String,
                   fullPath: String,
                   systemConfig: SystemTargetConfig)(implicit settings: DQSettings): Unit = {
    import sys.process.stringSeqToProcess
    val mailList: Seq[String] = systemConfig.mailList
    val mailListString        = mailList.mkString(" ")
    val targetName            = systemConfig.id

    if (settings.scriptPath.isDefined) {
      Seq(
        "/bin/bash",
        settings.scriptPath.get,
        targetName,
        mailListString,
        numOfFailedChecks.toString,
        failedCheckIds,
        fullPath
      ) !!
    } else throw new IllegalArgumentException("Mail script path is not defined")

  }

  /**
   * Generates explicit id for top N metric
   * Used in metric calculator result linking
   * @example generateMetricSubId("TOP_N", 3) => List("TOP_N_3","TOP_N_2", "TOP_N_1")
   * @param id Base id
   * @param n Requested N (amount of results)
   * @param aggr Generated id aggregator
   *
   * @return List of generated ids*/
  def generateMetricSubId(id: String, n: Int, aggr: List[String] = List.empty): List[String] = {
    if (n >= 1) {
      val newId: List[String] = List(id + "_" + n.toString)
      return generateMetricSubId(id, n - 1, aggr ++ newId)
    }
    aggr
  }

  /**
   * Generates tail for metric from parameter map
   * Used in metric calculator result linking
   * @example getParametrizedMetricTail(Map("targetValue"->1, "accuracy"->0.01d)) => ":0.01:1"
   * @param paramMap parameter map to process
   *
   * @return Generated result tail
   */
  def getParametrizedMetricTail(paramMap: ParamMap): String = {
    if (paramMap.nonEmpty) {
      // sorted by key to return the same result without affect of the map key order
      val sorted = TreeMap(paramMap.toArray: _*)
      val tail   = sorted.values.toList.mkString(":", ":", "")
      return tail
    }
    ""
  }

  /**
   * Maps map to JSON
   * Used to save the paramMap in local DB
   * @param map parameter map
   *
   * @return JSON representation of parameter map
   */
  def mapToJsonString(map: Map[String, Any]): String = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    if (map.isEmpty) "{}" else Serialization.write(map)
  }


  /**
   * Formats double. Used in result saving
   * @param double Target double
   *
   * @return String representation of formatted double
   */
  def formatDouble(double: Double): String = {
    val format: DecimalFormat = new DecimalFormat()
    // format can be changed
    format.setMaximumFractionDigits(utils.doubleFractionFormat)
    format.format(double)
  }

  /**
   * Tries to cast any value to String
   * Used in metric calculators
   * @param value value to cast
   *
   * @return Option of String. If it's null return None
   */
  def tryToString(value: Any): Option[String] = {
    value match {
      case null      => None
      case x: String => Some(x)
      case x         => Try(x.toString).toOption
    }
  }

  /**
   * Tries to cast any value to Joda DateTime object
   * for use in date-related metrics.
   * @param value - value to cast
   * @param dateFormat - date format used for casting
   * @return Option of Joda DateTime object.
   */
  def tryToDate(value: Any, dateFormat: String): Option[DateTime] = {
    value match {
      case null => None
      case x: Timestamp => Some(new DateTime(x.getTime))
      case x => Try {
        val fmt = DateTimeFormat forPattern dateFormat
        val str = tryToString(x)
        fmt parseDateTime str.get
      }.toOption
    }
  }

  private val binaryCasting: (Array[Byte]) => Option[Double] =
    (bytes: Array[Byte]) => {
      if (bytes == null) None
      else {
        Option(bytes.map(b => b.toChar).mkString.toDouble)
      }
    }

  /**
   * Tries to cast any value to Double
   * Used in metric calculators
   * @param value value to cast
   *
   * @return Option of Double. If it's null return None
   */
  def tryToDouble(value: Any): Option[Double] = {
    value match {
      case null           => None
      case x: Double      => Some(x)
      case x: Array[Byte] => binaryCasting(x)
      case x              => Try(x.toString.toDouble).toOption
    }
  }

  /**
   * Tries to cast any value to Long
   * Used in metric calculators
   * @param value
   * @return
   */
  def tryToLong(value: Any): Option[Long] = {
    value match {
      case null           => None
      case x: Long        => Some(x)
      case x: Int         => Some(x.toLong)
      case x: Array[Byte] => binaryCasting(x).map(_.toLong)
      case x              => Try(x.toString.toLong).toOption
    }
  }

  def camelToUnderscores(name: String): String =
    "[A-Z\\d]".r.replaceAllIn(name, { m =>
      if (m.end(0) == 1) {
        m.group(0).toLowerCase()
      } else {
        "_" + m.group(0).toLowerCase()
      }
    })

  def makeTableName(schema: Option[String], table: String): String = {
    val sep: String = "."
    schema match {
      case Some(x) => x + sep + table
      case None    => table
    }
  }

  def mapToDataType(typeString: String): DataType = typeString.toUpperCase match {
    case "STRING" => StringType
    case "BOOLEAN" => BooleanType
    case "DATE" => DateType
    case "TIMESTAMP" => TimestampType
    case "INTEGER" => IntegerType
    case "LONG" => LongType
    case "SHORT" => ShortType
    case "BYTE" => ByteType
    case "DOUBLE" => DoubleType
    case "FLOAT" => FloatType
    case d if d.startsWith("DECIMAL") =>
      val pattern: Regex = """\d+""".r
      val precision :: scale :: _ = pattern.findAllMatchIn(d).map(_.matched.toInt).toList
      DecimalType(precision, scale)
    case e =>
      log.warn("Failed to load schema")
      throw IllegalParameterException(e)
  }

  /**
   * Generate key for kafka message.
   * Key will contain entity type value, reference date value and run datetime value
   * separated by @ symbol.
   * @param msg Kafka message to generate key for (used to extract entity type value)
   * @param setting application settings object (used to get reference date and run datetime)
   * @return Kafka message key string
   */
  def kafkaKeyGenerator(msg: String, explicitEntity: Option[String] = None)
                       (implicit setting: DQSettings): String = {
    val md5hash = MessageDigest.getInstance("MD5").digest(msg.getBytes).map("%02x".format(_)).mkString
    if (explicitEntity.nonEmpty)
      s"${explicitEntity.get}@${setting.referenceDateString}@${setting.executionDateString}@$md5hash"
    else {
      val entityPattern = """"entityType": "(.+?)"""".r
      val jobIdPattern = """"jobId": "(.+?)"""".r
      val entity = Try {
        entityPattern.findFirstMatchIn(msg).get.group(1)
      }.getOrElse("unknownEntity")
      val jobId = Try {
        jobIdPattern.findFirstMatchIn(msg).get.group(1)
      }.getOrElse("unknownJobId")
      s"$entity@$jobId@${setting.referenceDateString}@${setting.executionDateString}@$md5hash"
    }
  }

  /**
   * Converts java ZonedDateTime to sql Timestamp at UTC timezone.
   * @param zdt date to convert
   * @return Timestamp at UTC zone
   */
  def toUtcTimestamp(zdt: ZonedDateTime): Timestamp = {
    val ldt = zdt.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime
    Timestamp.valueOf(ldt)
  }

  /**
   * Convert sequence of string options (in format of k=v) into a Map
   * @param optionsSeq Sequence of string options
   * @return Map of options
   */
  def optionSeqToMap(optionsSeq: Seq[String]): Map[String, String] =
    optionsSeq.map(_.split("=", 2)).collect{
      case Array(k, v) => k -> v
    }.toMap

}
