package org.checkita.dqf.connections.kafka

import com.databricks.spark.xml.functions.from_xml
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.json4s.jackson.Serialization.write
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.Enums.KafkaTopicFormat
import org.checkita.dqf.config.jobconf.Connections.KafkaConnectionConfig
import org.checkita.dqf.config.jobconf.Sources.KafkaSourceConfig
import org.checkita.dqf.connections.{DQConnection, DQStreamingConnection}
import org.checkita.dqf.core.streaming.Checkpoints.{Checkpoint, KafkaCheckpoint}
import org.checkita.dqf.readers.SchemaReaders.SourceSchema
import org.checkita.dqf.utils.Common.{jsonFormats, paramsSeqToMap}
import org.checkita.dqf.utils.ResultUtils._
import org.checkita.dqf.utils.SparkUtils.DataFrameOps

import java.util.Properties
import scala.util.Try

case class KafkaConnection(config: KafkaConnectionConfig) extends DQConnection with DQStreamingConnection {
  type SourceType = KafkaSourceConfig
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  private val kafkaParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> config.servers.value.map(_.value).mkString(",")
  ) ++ paramsSeqToMap(sparkParams)

  /**
   * When kafka connection parameters are used to directly build Kafka Producer or Consumer,
   * then 'kafka.' prefix from their names must be removed. Such prefix is used insude Spark
   * to distinguish configurations related to Kafka.
   * @param params Spark Kafka connection parameters
   * @return Pure Kafka connection parameters
   */
  private def alterParams(params: Map[String, String]): Map[String, String] =
    params.map {
      case (k, v) => if (k.startsWith("kafka.")) k.drop("kafka.".length) -> v else k -> v
    }
  
  /**
   * Gets proper subscribe option for given source configuration.
   * @param srcId Source ID
   * @param topics List of Kafka topics (can be empty)
   * @param topicPattern Topic pattern (optional)
   * @return Topic(s) subscribe option
   */
  private def getSubscribeOption(srcId: String,
                                 topics: Seq[String],
                                 topicPattern: Option[String]): (String, String) =
    (topics, topicPattern) match {
      case (ts@_ :: _, None) => if (ts.forall(_.contains("@"))) {
        "assign" -> ts.map(_.split("@")).collect {
          case Array(topic, partitions) => "\"" + topic + "\":" + partitions
        }.mkString("{", ",", "}")
      } else if (ts.forall(!_.contains("@"))) {
        "subscribe" -> ts.mkString(",")
      } else throw new IllegalArgumentException(
        s"Kafka source '$srcId' configuration error: mixed topic notation - " +
          "all topics must be defined either with partitions to read or without them (read all topic partitions)."
      )
      case (Nil, Some(tp)) => "subscribePattern" -> tp
      case _ => throw new IllegalArgumentException(
        s"Kafka source '$srcId' configuration error: " +
          "topics must be defined either explicitly as a sequence of topic names or as a topic pattern."
      )
    }

  /**
   * Decodes kafka message key and value columns given their format.
   *
   * @param colName      Name of the column to decode (either 'key' or 'value')
   * @param format       Column format to parse
   * @param schemaId     Schema ID used to parse column of JSON or XML format
   * @param withSchemaId Boolean flag indication whether kafka message has Confluent wire format.
   * @param schemas      Map of all explicitly defined schemas (schemaId -> SourceSchema)
   * @return Decoded column
   */
  private def decodeColumn(colName: String,
                           format: KafkaTopicFormat,
                           schemaId: Option[String],
                           withSchemaId: Boolean = false)
                          (implicit schemas: Map[String, SourceSchema]): Column = {

    val schemaGetter = (formatStr: String) => {
      val colSchemaId = schemaId.getOrElse(throw new IllegalArgumentException(
        s"Schema must be provided in order to parse kafka message '$colName' column of $formatStr format"
      ))
      schemas.getOrElse(colSchemaId,
        throw new NoSuchElementException(s"Schema with id = '$colSchemaId' not found.")
      )
    }

    val binaryColumn: Column = if (withSchemaId) substring(col(colName), 6, Int.MaxValue) else col(colName)

    format match {
      case KafkaTopicFormat.Binary => binaryColumn
      case KafkaTopicFormat.String => binaryColumn.cast(StringType).as(colName)
      case KafkaTopicFormat.Json =>
        from_json(binaryColumn.cast(StringType), schemaGetter("JSON").schema).alias(colName)
      case KafkaTopicFormat.Xml =>
            from_xml(binaryColumn.cast(StringType), schemaGetter("XML").schema).alias(colName)
      case KafkaTopicFormat.Avro =>
        // intentionally use deprecated method to support compatibility with Spark 2.4.x versions.
        from_avro(binaryColumn, schemaGetter("AVRO").toAvroSchema).alias(colName)
      case other => throw new IllegalArgumentException(
        s"Wrong kafka topic message format for column '$colName': ${other.toString}"
      )
    }
  }

  /**
   * Checks connection.
   * @return Nothing or error message in case if connection is not ready.
   */
  def checkConnection: Result[Unit] = Try {
    val props = new Properties()
    alterParams(kafkaParams).foreach{ case (k, v) => props.put(k, v) }
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
      getSubscribeOption(
        sourceConfig.id.value, sourceConfig.topics.map(_.value).toList, sourceConfig.topicPattern.map(_.value)
      ) + ("startingOffsets" -> sourceConfig.startingOffsets.map(_.value).getOrElse("earliest")) +
      ("endingOffsets" -> sourceConfig.endingOffsets.map(_.value).getOrElse("latest"))

    val rawDF = spark.read.format("kafka").options(allOptions).load()

    val keyColumn = decodeColumn("key", sourceConfig.keyFormat, sourceConfig.keySchema.map(_.value))
    val valueColumn = decodeColumn(
      "value", sourceConfig.valueFormat, sourceConfig.valueSchema.map(_.value), sourceConfig.subtractSchemaId
    )

    rawDF.select(keyColumn, valueColumn)
  }

  /**
   * Loads stream into a dataframe given the stream configuration
   *
   * @param sourceConfig Source configuration
   * @param settings     Implicit application settings object
   * @param spark        Implicit spark session object
   * @param schemas      Implicit Map of all explicitly defined schemas (schemaId -> SourceSchema)
   * @param checkpoints Map of initial checkpoints read from checkpoint directory
   * @return Spark Streaming DataFrame
   */
  def loadDataStream(sourceConfig: SourceType)
                    (implicit settings: AppSettings,
                     spark: SparkSession,
                     schemas: Map[String, SourceSchema],
                     checkpoints: Map[String, Checkpoint]): DataFrame = {
    
    val startingOffsets = checkpoints.get(sourceConfig.id.value)
      .map(_.asInstanceOf[KafkaCheckpoint])
      .map{ chk =>
        val offsets = chk.currentOffsets
          .groupBy(t => t._1._1)
          .map {
            case (k, im) => k -> im.map(t => t._1._2.toString -> (t._2 + 1))
          }
        write(offsets)
      }
      .orElse(sourceConfig.startingOffsets.map(_.value))
      .getOrElse("latest")
    
    val allOptions = kafkaParams ++
      paramsSeqToMap(sourceConfig.options.map(_.value)) +
      getSubscribeOption(
        sourceConfig.id.value, sourceConfig.topics.map(_.value).toList, sourceConfig.topicPattern.map(_.value)
      ) + ("startingOffsets" -> startingOffsets)

    val rawStream = spark.readStream.format("kafka").options(allOptions).load()

    val keyColumn = decodeColumn("key", sourceConfig.keyFormat, sourceConfig.keySchema.map(_.value))
    val valueColumn = decodeColumn(
      "value", sourceConfig.valueFormat, sourceConfig.valueSchema.map(_.value), sourceConfig.subtractSchemaId
    )
    val offsetColumn = struct(col("topic"), col("partition"), col("offset")).as(settings.streamConfig.checkpointCol)

    rawStream.select(keyColumn, valueColumn, offsetColumn, col("timestamp")).prepareStream(sourceConfig.windowBy)
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

    // here we use direct Kafka Producer, therefore, Kafka parameters must be used without 'kafka.' prefix as in Spark
    alterParams(allOptions).foreach{ case (k, v) => props.put(k, v) }
    
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
