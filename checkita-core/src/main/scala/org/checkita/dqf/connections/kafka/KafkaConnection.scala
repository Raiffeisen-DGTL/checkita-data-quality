package org.checkita.dqf.connections.kafka

import com.databricks.spark.xml.functions.from_xml
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer, StringSerializer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.Enums.KafkaTopicFormat
import org.checkita.dqf.config.jobconf.Connections.KafkaConnectionConfig
import org.checkita.dqf.config.jobconf.Sources.KafkaSourceConfig
import org.checkita.dqf.connections.{DQConnection, DQStreamingConnection}
import org.checkita.dqf.core.streaming.Checkpoints.KafkaCheckpoint
import org.checkita.dqf.readers.SchemaReaders.SourceSchema
import org.checkita.dqf.utils.Common.{jsonFormats, paramsSeqToMap}
import org.checkita.dqf.utils.ResultUtils._
import org.checkita.dqf.utils.SparkUtils.DataFrameOps
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.util.Try

case class KafkaConnection(config: KafkaConnectionConfig) extends DQConnection with DQStreamingConnection {
  type SourceType = KafkaSourceConfig
  type CheckpointType = KafkaCheckpoint
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
   * Gets Kafka consumer provided with key and value deserializers.
   *
   * @param keyDe   Key deserializer
   * @param valueDe Value deserializer
   * @tparam A Type of message key
   * @tparam B Type of message value
   * @return Kafka consumer instance
   */
  private def getKafkaStringConsumer[A, B](keyDe: Deserializer[A], 
                                           valueDe: Deserializer[B]): KafkaConsumer[A, B] = {
    val props = new Properties()
    alterParams(kafkaParams).foreach{ case (k, v) => props.put(k, v) }
    new KafkaConsumer[A, B](props, keyDe, valueDe)
  }

  /**
   * Transformed used to convert kafka subscribe options to desired output provided
   * with transformed function for each of three possible subscribe scenarios.
   *
   * @param srcId        Source ID
   * @param topics       List of Kafka topics (can be empty)
   * @param topicPattern Topic pattern (optional)
   * @param f1           Transformer function for case when specific topic partitions are provided. 
   * @param f2           Transformer function for case when only topic names are provided.
   * @param f3           Transformer function for case when only topic pattern is provided.
   * @tparam T Type of the transformers output.
   * @return Result of transformer function applied to subscribe option.
   */
  private def subscribeOptionTransformer[T](srcId: String,
                                           topics: Seq[String],
                                           topicPattern: Option[String],
                                           f1: Seq[String] => T,
                                           f2: Seq[String] => T,
                                           f3: String => T): T =
    (topics.toList, topicPattern) match {
      case (ts@_ :: _, None) => 
        if (ts.forall(_.contains("@"))) f1(ts)
        else if (ts.forall(!_.contains("@"))) f2(ts)
        else throw new IllegalArgumentException(
        s"Kafka source '$srcId' configuration error: mixed topic notation - " +
          "all topics must be defined either with partitions to read or without them (read all topic partitions)."
      )
      case (Nil, Some(tp)) => f3(tp)
      case _ => throw new IllegalArgumentException(
        s"Kafka source '$srcId' configuration error: " +
          "topics must be defined either explicitly as a sequence of topic names or as a topic pattern."
      )
    }

  /**
   * Gets proper subscribe option for given source configuration.
   *
   * @param srcId        Source ID
   * @param topics       List of Kafka topics (can be empty)
   * @param topicPattern Topic pattern (optional)
   * @return Topic(s) subscribe option
   */
  private def getSubscribeOption(srcId: String,
                                 topics: Seq[String],
                                 topicPattern: Option[String]): (String, String) = {
    val f1: Seq[String] => (String, String) = 
      ts => "assign" -> ts.map(_.split("@")).collect {
        case Array(topic, partitions) => "\"" + topic + "\":" + partitions
      }.mkString("{", ",", "}")
    val f2: Seq[String] => (String, String) = ts => "subscribe" -> ts.mkString(",")
    val f3: String => (String, String) = tp => "subscribePattern" -> tp
    subscribeOptionTransformer(srcId, topics, topicPattern, f1, f2, f3)
  }

  /**
   * Gets list of subscribed topic partitions for given source configuration. 
   *
   * @param srcId        Source ID
   * @param topics       List of Kafka topics (can be empty)
   * @param topicPattern Topic pattern (optional)
   * @param consumer     Kafka consumer used to fetch topics and partitions data.
   * @tparam A Type of message key (needed only for proper function signature)
   * @tparam B Type of message value (needed only for proper function signature)
   * @return List of subscribed topic partitions.
   */
  private def getSubscribedTopicPartitions[A, B](srcId: String,
                                                 topics: Seq[String],
                                                 topicPattern: Option[String],
                                                 consumer: KafkaConsumer[A, B]): Seq[TopicPartition] = {
    val allTopics = consumer.listTopics()

    val f1: Seq[String] => Seq[PartitionInfo] =
      ts => ts.map(_.split("@")).collect {
        case Array(t, p) => (t, p.replaceAll("""[\[\]]""", "").split(""",\s*""").map(_.toInt).toSeq)
      }.flatMap{
        case (topic, partitions) => allTopics.get(topic) match {
          case null => List.empty[PartitionInfo]
          case someInfo => someInfo
            .asScala
            .filter(info => partitions.contains(info.partition()))
        }
      }

    val f2: Seq[String] => Seq[PartitionInfo] =
      ts => ts.flatMap{
        topic => allTopics.get(topic) match {
          case null => List.empty[PartitionInfo]
          case someInfo => someInfo.asScala
        }
      }

    val f3: String => Seq[PartitionInfo] =
      tp => allTopics.asScala.filterKeys(_.matches(tp)).values.flatMap(_.asScala).toSeq


    subscribeOptionTransformer(srcId, topics, topicPattern, f1, f2, f3)
      .map(p => new TopicPartition(p.topic(), p.partition()))
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
   * Gets latest offsets for provided topic partitions.
   *
   * @param topicPartitions Topic partitions to search offsets for.
   * @param consumer        Kafka consumer used to fetch offset data.
   * @tparam A Type of message key (needed only for proper function signature)
   * @tparam B Type of message value (needed only for proper function signature)
   * @return Map of latest offsets per topic partition.
   */
  private def getLatestOffsets[A, B](topicPartitions: Seq[TopicPartition], 
                                     consumer: KafkaConsumer[A, B]): Map[(String, Int), Long] =
    consumer.endOffsets(topicPartitions.asJava).asScala.map {
      case (t, o) => (t.topic(), t.partition()) -> o.toLong
    }.toMap

  /**
   * Gets earliest offsets for provided topic partitions.
   *
   * @param topicPartitions Topic partitions to search offsets for.
   * @param consumer        Kafka consumer used to fetch offset data.
   * @tparam A Type of message key (needed only for proper function signature)
   * @tparam B Type of message value (needed only for proper function signature)
   * @return Map of latest offsets per topic partition.
   */
  private def getEarliestOffsets[A, B](topicPartitions: Seq[TopicPartition],
                                       consumer: KafkaConsumer[A, B]): Map[(String, Int), Long] =
    consumer.beginningOffsets(topicPartitions.asJava).asScala.map {
      case (t, o) => (t.topic(), t.partition()) -> o.toLong
    }.toMap

  /**
   * Tries to parse starting offsets string.
   *
   * @param srcId           Source ID for which the starting offsets are parsed.
   * @param startingOffsets Starting offsets string.
   * @return Either a parsed map of starting offsets or a list of parsing errors.
   */
  private def parseStartingOffsets(srcId: String,
                                   startingOffsets: String): Result[Map[(String, Int), Long]] = Try {
    for {
      (topic, parts) <- parse(startingOffsets).extract[Map[String, Map[String, Long]]]
      (part, offset) <- parts
    } yield (topic, part.toInt) -> offset
  }.toResult(
    s"Unable to parse startingOffsets string for stream '$srcId' due to following error:"
  )
  
  /**
   * Validates starting offset by making sure that offsets are presented for all 
   * subscribed topics and partitions. Additional complexity is involved to collect 
   * all missing topic partitions (if any).
   * 
   * @param offsets Starting offsets.
   * @param topicPartitions List of subscribed topic partitions.
   * @return Either the same offsets or a list of missing topic partitions.
   */
  private def validateStartingOffsets(offsets: Map[(String, Int), Long], 
                                      topicPartitions: Seq[TopicPartition]): Result[Map[(String, Int), Long]] = {
    val partitionsSet = topicPartitions.map(tp => (tp.topic(), tp.partition())).toSet
    offsets.map{
      case (topicPartition, offset) => Either.cond(
        partitionsSet.contains(topicPartition),
        topicPartition -> offset, 
        s"No offset provided for topic partition '${topicPartition._1}:${topicPartition._2}'"
      ).toResult(Vector(_))
    }.foldLeft(liftToResult(Map.empty[(String, Int), Long]))((r1, r2) => r1.combine(r2)(_ + _))
      .mapValue(_.toMap)
      .mapLeft(errs => "Starting offsets must specify all topic partitions." +: errs)
  }

  /**
   * Checks connection.
   * @return Nothing or error message in case if connection is not ready.
   */
  def checkConnection: Result[Unit] = Try {
    val stringDe = new StringDeserializer
    val consumer = getKafkaStringConsumer(stringDe, stringDe)
    consumer.listTopics()
    consumer.close()
  }.toResult(preMsg = s"Unable to establish Kafka connection '$id' due to following error: ")
  
  /**
   * Creates initial checkpoint for provided Kafka source configuration.
   *
   * @param sourceConfig Kafka source configuration
   * @return Kafka checkpoint
   */
  def initCheckpoint(sourceConfig: SourceType): CheckpointType = {
    val stringDe = new StringDeserializer
    val consumer = getKafkaStringConsumer(stringDe, stringDe)
    val topicPartitions: Seq[TopicPartition] = getSubscribedTopicPartitions(
      sourceConfig.id.value,
      sourceConfig.topics.map(_.value),
      sourceConfig.topicPattern.map(_.value),
      consumer
    )
    
    assert(
      topicPartitions.nonEmpty,
      s"Topics and partitions for stream '${sourceConfig.id.value}' not found. Cannot subscribe to nothing!"
    )
    
    val offsets: Map[(String, Int), Long] = sourceConfig.startingOffsets.map(_.value) match {
      case Some("latest") => getLatestOffsets(topicPartitions, consumer)
      case Some("earliest") => getEarliestOffsets(topicPartitions, consumer)
      case Some(startingOffsets) => parseStartingOffsets(sourceConfig.id.value, startingOffsets)
        .flatMap(offsets => validateStartingOffsets(offsets, topicPartitions)) match {
          case Right(offsets) => offsets
          case Left(e) => throw new RuntimeException(
            s"Unable to initialize Kafka checkpoint due to following error:\n" + e.mkString("\n")
          )
        }
      case None => getLatestOffsets(topicPartitions, consumer) // default is "latest"
    }
    consumer.close()
    KafkaCheckpoint(sourceConfig.id.value, offsets)
  }

  /**
   * Validates checkpoint structure and makes updates in case if
   * checkpoint structure needs to be changed.
   *
   * @param checkpoint   Checkpoint to validate and fix (if needed).
   * @param sourceConfig Source configuration
   * @return Either original checkpoint if it is valid or a fixed checkpoint.
   */
  def validateOrFixCheckpoint(checkpoint: CheckpointType, sourceConfig: SourceType): CheckpointType = {
    val stringDe = new StringDeserializer
    val consumer = getKafkaStringConsumer(stringDe, stringDe)
    val topicPartitions: Seq[TopicPartition] = getSubscribedTopicPartitions(
      sourceConfig.id.value,
      sourceConfig.topics.map(_.value),
      sourceConfig.topicPattern.map(_.value),
      consumer
    )

    assert(
      topicPartitions.nonEmpty,
      s"Topics and partitions for stream '${sourceConfig.id.value}' not found. Cannot subscribe to nothing!"
    )
    
    val validated = validateStartingOffsets(checkpoint.currentOffsets, topicPartitions) match {
      case Right(_) => checkpoint
      case Left(_) =>
        // for newly added topic partitions we will start with "earliest".
        // This is default Spark Streaming behaviour: see startingOffsets description in 
        // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations
        checkpoint.merge(KafkaCheckpoint(
          sourceConfig.id.value,
          getEarliestOffsets(topicPartitions, consumer)
        )).asInstanceOf[KafkaCheckpoint] 
    }
    
    consumer.close()
    validated
  }
  
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
   * @param checkpoint   Checkpoint to start stream from.
   * @param settings     Implicit application settings object
   * @param spark        Implicit spark session object
   * @param schemas      Implicit Map of all explicitly defined schemas (schemaId -> SourceSchema)
   * @return Spark Streaming DataFrame
   */
  def loadDataStream(sourceConfig: SourceType, checkpoint: CheckpointType)
                    (implicit settings: AppSettings,
                     spark: SparkSession,
                     schemas: Map[String, SourceSchema]): DataFrame = {
    
    val startingOffsets = write(
      checkpoint.currentOffsets
        .groupBy(t => t._1._1)
        .map {
          case (k, im) => k -> im.map(t => t._1._2.toString -> (t._2 + 1))
        }
    )
    
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
   *
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
