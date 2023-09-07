package ru.raiffeisen.checkita.utils.io.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types.StringType
import org.json.XML
import ru.raiffeisen.checkita.sources.{KafkaConfig, KafkaSourceConfig}
import ru.raiffeisen.checkita.utils.{DQSettings, kafkaKeyGenerator}
import ru.raiffeisen.checkita.utils.enums.KafkaFormats

import java.util.Properties
import scala.util.Try

case class KafkaManager(config: KafkaConfig) {

  private val xmlToJson: UserDefinedFunction = udf((value: String) => XML.toJSONObject(value).toString)

  private val kafkaParams: Map[String, String] = {
    if (config.jaasConfFile.nonEmpty) System.setProperty("java.security.auth.login.config", config.jaasConfFile.get)

    optionSeqToMap(config.parameters) + ("bootstrap.servers" -> config.servers.mkString(","))
  }

  /**
   * Loads data from kafka topic into Spark DataFrame.
   * @param topic - Kafka Source config containing parameters to create Kafka Consumer
   * @param spark - spark session object
   * @return Spark DataFrame with data from topic
   */
  def loadData(topic: KafkaSourceConfig)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val extraOptions = optionSeqToMap(topic.options)

    val allOptions = kafkaParams.map(t => s"kafka.${t._1}" -> t._2) ++ extraOptions + topic.subscribeOption + (
      "startingOffsets" -> topic.startingOffsets.getOrElse("earliest")
      ) + (
      "endingOffsets" -> topic.endingOffsets.getOrElse("latest")
      )

    val rawDF = spark.read.format("kafka").options(allOptions).load()

    val decodedDF = topic.format match {
      case KafkaFormats.json =>
        rawDF.select(col("value").cast(StringType).alias("data"))
      case KafkaFormats.xml =>
        rawDF.select(xmlToJson(col("value").cast(StringType)).alias("data"))
      case _ => throw new IllegalArgumentException(
        s"Wrong kafka topic format for source with id ${topic.id}: ${topic.format.toString}"
      )
    }

    val schema = spark.read.json(decodedDF.select("data").as[String]).schema
    val jsonDF = decodedDF
      .withColumn("jsonData", from_json(col("data"), schema))
      .select("jsonData.*")

    jsonDF.select(jsonDF.columns.map(c => col(c).as(c.toLowerCase)) : _*)
  }

  /**
   * Writes message to Kafka topic.
   * @param data - data to write (contains topic info with extra kafka parameters and messages to produce)
   * @return The result of messages write operations in form of either a "Success" string or a sequence of errors messages.
   */
  def writeData(data: KafkaOutput)(implicit settings: DQSettings): Either[Seq[String], String] = {

    val props = new Properties()
    val allOptions = kafkaParams ++ optionSeqToMap(data.options) ++ Map(
      "batch.size" -> "65536",
      "linger.ms" -> "0",
      "acks" -> "all"
    ) // default options related to producer

    allOptions.foreach(option => props.put(option._1, option._2)) // TODO: maybe is better to use putAll + asJava implicit conversion

    val producer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)
    val cb = new ProducerCallback

    if (settings.aggregatedKafkaOutput) {
      val aggMsg = data.messages.mkString("[\n", ",\n", "\n]")
      val record = new ProducerRecord[String, String](
        data.topic, kafkaKeyGenerator(aggMsg, data.explicitEntity), aggMsg
      )
      producer.send(record, cb)
    } else {
      data.messages.foreach { msg =>
        val record = new ProducerRecord[String, String](data.topic, kafkaKeyGenerator(msg), msg)
        producer.send(record, cb)
      }
    }
    producer.flush()
    producer.close()

    if (cb.getExceptions.isEmpty) Right("Success")
    else Left(cb.getExceptions)
  }

  /**
   * Checks the connection to Kafka by creating a simple consumer and by trying to fetch list of topics.
   * @return Try object containing error in case of failure or nothing in case of success.
   */
  def checkConnection: Try[Unit] = {
    val props = new Properties()
    kafkaParams.foreach(option => props.put(option._1, option._2))

    Try{
      val consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
      consumer.listTopics()
      consumer.close()
    }
  }

  private def optionSeqToMap(optionsSeq: Seq[String]): Map[String, String] =
    optionsSeq.map(_.split("=", 2)).collect{
      case Array(k, v) => k -> v
    }.toMap
}
