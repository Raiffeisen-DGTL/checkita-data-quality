package ru.raiffeisen.checkita.connections.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.Xml.toJson
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.Enums.KafkaTopicFormat
import ru.raiffeisen.checkita.config.jobconf.Connections.KafkaConnectionConfig
import ru.raiffeisen.checkita.config.jobconf.Sources.KafkaSourceConfig
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.utils.Common.paramsSeqToMap
import ru.raiffeisen.checkita.utils.ResultUtils._

import java.util.Properties
import scala.util.Try
import scala.xml.XML

case class KafkaConnection(config: KafkaConnectionConfig) extends DQConnection {
  type SourceType = KafkaSourceConfig
  val id: String = config.id.value
  protected val sparkParams: Seq[String] = config.parameters.map(_.value)
  private val kafkaParams: Map[String, String] = Map(
    "bootstrap.servers" -> config.servers.value.map(_.value).mkString(",")
  ) ++ paramsSeqToMap(sparkParams)
  
  private val xmlToJson: UserDefinedFunction = udf((xmlStr: String) => toJson(XML.loadString(xmlStr)))
  
  /**
   * Gets proper subscribe option for given source configuration.
   * @param conf Kafka source configuration
   * @return Topic(s) subscribe option
   */
  private def getSubscribeOption(conf: SourceType): (String, String) =
    (conf.topics.map(_.value), conf.topicPattern.map(_.value)) match {
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
   * @param settings Implicit application settings object
   * @param spark Implicit spark session object
   * @param sourceConfig Source configuration
   * @return Spark DataFrame
   */
  def loadDataframe(sourceConfig: SourceType)
                   (implicit settings: AppSettings,
                    spark: SparkSession): DataFrame = {
    import spark.implicits._
    
    val allOptions = kafkaParams ++ 
      paramsSeqToMap(sourceConfig.options.map(_.value)) + 
      getSubscribeOption(sourceConfig) +
      ("startingOffsets" -> sourceConfig.startingOffsets.value) +
      ("endingOffsets" -> sourceConfig.endingOffsets.value)

    val rawDF = spark.read.format("kafka").options(allOptions).load()
    val decodedDF = sourceConfig.format match {
      case KafkaTopicFormat.Json =>
        rawDF.select(col("value").cast(StringType).alias("data"))
      case KafkaTopicFormat.Xml =>
        rawDF.select(xmlToJson(col("value").cast(StringType)).alias("data"))
      case fmt => throw new IllegalArgumentException(
        s"Wrong kafka topic format for kafka source $id: ${fmt.toString}"
      ) // we do not support avro for now.
    }

    val schema = spark.read.json(decodedDF.select("data").as[String]).schema
    
    decodedDF.withColumn("jsonData", from_json(col("data"), schema)).select("jsonData.*")
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
