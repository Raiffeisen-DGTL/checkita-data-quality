package ru.raiffeisen.checkita.connections.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.json.XML
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.Enums.{CustomTime, EventTime, KafkaTopicFormat, ProcessingTime}
import ru.raiffeisen.checkita.config.jobconf.Connections.KafkaConnectionConfig
import ru.raiffeisen.checkita.config.jobconf.Sources.KafkaSourceConfig
import ru.raiffeisen.checkita.connections.{DQConnection, DQStreamingConnection}
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema
import ru.raiffeisen.checkita.utils.Common.paramsSeqToMap
import ru.raiffeisen.checkita.utils.ResultUtils._
import ru.raiffeisen.checkita.utils.SparkUtils.DurationOps

import java.util.Properties
import scala.util.Try

case class KafkaConnection(config: KafkaConnectionConfig) extends DQConnection with DQStreamingConnection {
  type SourceType = KafkaSourceConfig
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  private val kafkaParams: Map[String, String] = Map(
    "bootstrap.servers" -> config.servers.value.map(_.value).mkString(",")
  ) ++ paramsSeqToMap(sparkParams)
  
  private val xmlToJson: UserDefinedFunction = udf((xmlStr: String) => XML.toJSONObject(xmlStr).toString)
  
  /**
   * Gets proper subscribe option for given source configuration.
   * @param topics List of Kafka topics (can be empty)
   * @param topicPattern Topic pattern (optional)
   * @return Topic(s) subscribe option
   */
  private def getSubscribeOption(topics: Seq[String], topicPattern: Option[String]): (String, String) =
    (topics, topicPattern) match {
      case (ts@_ :: _, None) => if (ts.forall(_.contains("@"))) {
        "assign" -> ts.map(_.split("@")).collect {
          case Array(topic, partitions) => "\"" + topic + "\":" + partitions
        }.mkString("{", ",", "}")
      } else if (ts.forall(!_.contains("@"))) {
        "subscribe" -> ts.mkString(",")
      } else throw new IllegalArgumentException(
        s"Kafka source '$id' configuration error: mixed topic notation - " +
          "all topics must be defined either with partitions to read or without them (read all topic partitions)."
      )
      case (Nil, Some(tp)) => "subscribePattern" -> tp
      case _ => throw new IllegalArgumentException(
        s"Kafka source '$id' configuration error: " +
          "topics must be defined either explicitly as a sequence of topic names or as a topic pattern."
      )
    }

  /**
   * Decodes kafka message key and value columns given their format.
   * @param colName Name of the column to decode (either 'key' or 'value')
   * @param format Column format to parse
   * @param schemaId Schema ID used to parse column of JSON or XML format
   * @param schemas Map of all explicitly defined schemas (schemaId -> SourceSchema)
   * @return Decoded column
   */
  private def decodeColumn(colName: String, format: KafkaTopicFormat, schemaId: Option[String])
                          (implicit schemas: Map[String, SourceSchema]): Column = {

    val schemaGetter = (formatStr: String) => {
      val keySchemaId = schemaId.getOrElse(throw new IllegalArgumentException(
        s"Schema must be provided in order to parse kafka message '$colName' column of $formatStr format"
      ))
      schemas.getOrElse(keySchemaId,
        throw new NoSuchElementException(s"Schema with id = '$keySchemaId' not found.")
      )
    }

    format match {
      case KafkaTopicFormat.String => col(colName).cast(StringType).as(colName)
      case KafkaTopicFormat.Json =>
        val keySchema = schemaGetter("JSON")
        from_json(col(colName).cast(StringType), keySchema.schema).alias(colName)
      case KafkaTopicFormat.Xml =>
        val keySchema = schemaGetter("XML")
        from_json(xmlToJson(col(colName).cast(StringType)), keySchema.schema).alias(colName)
      case other => throw new IllegalArgumentException(
        s"Wrong kafka topic message format for column '$colName': ${other.toString}"
      ) // we do not support avro for now.
    }
  }

  /**
   * Checks connection.
   *
   * @return Nothing or error message in case if connection is not ready.
   */
  def checkConnection: Result[Unit] = Try {
    val props = new Properties()
    kafkaParams.foreach{ case (k, v) => props.put(k, v) }
    val consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
    consumer.listTopics()
    consumer.close()
  }.toResult(preMsg = s"Unable to establish Kafka connection '$id' due to following error: ")

  /**
   * Loads external data into dataframe given a source configuration
   *
   * @param sourceConfig Source configuration
   * @param settings     Implicit application settings object
   * @param spark        Implicit spark session object
   * @param schemas      Implicit Map of all explicitly defined schemas (schemaId -> SourceSchema)
   * @return Spark DataFrame
   */
  def loadDataFrame(sourceConfig: SourceType)
                   (implicit settings: AppSettings,
                    spark: SparkSession,
                    schemas: Map[String, SourceSchema]): DataFrame = {
    
    val allOptions = kafkaParams ++ 
      paramsSeqToMap(sourceConfig.options.map(_.value)) + 
      getSubscribeOption(sourceConfig.topics.map(_.value), sourceConfig.topicPattern.map(_.value)) +
      ("startingOffsets" -> sourceConfig.startingOffsets.map(_.value).getOrElse("earliest")) +
      ("endingOffsets" -> sourceConfig.endingOffsets.map(_.value).getOrElse("latest"))

    val rawDF = spark.read.format("kafka").options(allOptions).load()

    val keyColumn = decodeColumn("key", sourceConfig.keyFormat, sourceConfig.keySchema.map(_.value))
    val valueColumn = decodeColumn("value", sourceConfig.valueFormat, sourceConfig.valueSchema.map(_.value))

    rawDF.select(keyColumn, valueColumn)
  }

  /**
   * Loads stream into a dataframe given the stream configuration
   *
   * @param sourceConfig Source configuration
   * @param settings     Implicit application settings object
   * @param spark        Implicit spark session object
   * @param schemas      Implicit Map of all explicitly defined schemas (schemaId -> SourceSchema)
   * @return Spark Streaming DataFrame
   */
  def loadDataStream(sourceConfig: SourceType)
                    (implicit settings: AppSettings,
                     spark: SparkSession,
                     schemas: Map[String, SourceSchema]): DataFrame = {
    val allOptions = kafkaParams ++
      paramsSeqToMap(sourceConfig.options.map(_.value)) +
      getSubscribeOption(sourceConfig.topics.map(_.value), sourceConfig.topicPattern.map(_.value)) +
      ("startingOffsets" -> sourceConfig.startingOffsets.map(_.value).getOrElse("latest"))

    val windowTsCol = settings.streamConfig.windowTsCol
    val eventTsCol = settings.streamConfig.eventTsCol
    val windowDuration = settings.streamConfig.window.toSparkInterval

    val rawStream = spark.readStream.format("kafka").options(allOptions).load()

    val keyColumn = decodeColumn("key", sourceConfig.keyFormat, sourceConfig.keySchema.map(_.value))
    val valueColumn = decodeColumn("value", sourceConfig.valueFormat, sourceConfig.valueSchema.map(_.value))
    val eventColumn = sourceConfig.windowBy match {
      case ProcessingTime => current_timestamp().cast(LongType)
      case EventTime => col("timestamp")
      case CustomTime(colName) => col(colName).cast(LongType)
    }

    rawStream.select(keyColumn, valueColumn, col("timestamp"))
      .withColumn(eventTsCol, eventColumn)
      .withColumn(s"${windowTsCol}_pre", window(col(eventTsCol).cast(TimestampType), windowDuration))
      .select(
        col(eventTsCol),
        col(s"${windowTsCol}_pre").getField("start").cast(LongType).as(windowTsCol),
        col("key"),
        col("value")
      )
  }

  /**
   * Sends data to Kafka topic.
   * @param data Data to be send
   * @return Status of operation: either "Success" string or a list of errors.
   */
  def writeData(data: KafkaOutput): Result[String] = {

    val props = new Properties()
    val allOptions = kafkaParams ++ paramsSeqToMap(data.options) ++ Map(
      "batch.size" -> "65536",
      "linger.ms" -> "0",
      "acks" -> "all"
    ) // default options related to producer
    allOptions.foreach{ case (k, v) => props.put(k, v) }
    
    val producer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)
    val cb = new ProducerCallback
    
    data.messages.foreach { msg =>
      val record = new ProducerRecord[String, String](data.topic, msg._1, msg._2)
      producer.send(record, cb)
    }
    producer.flush()
    producer.close()
    val status = if (cb.getExceptions.isEmpty) Right("Success") else Left(cb.getExceptions)
    status.toResult(_.toVector)
  }
}
