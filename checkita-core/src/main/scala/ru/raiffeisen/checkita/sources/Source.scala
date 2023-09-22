package ru.raiffeisen.checkita.sources

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import ru.raiffeisen.checkita.configs.GenStructColumn
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.sources.SourceTypes.SourceType
import ru.raiffeisen.checkita.targets.HdfsTargetConfig
import ru.raiffeisen.checkita.utils.DQSettings
import ru.raiffeisen.checkita.utils.enums.KafkaFormats.KafkaFormat

import scala.reflect.{ClassTag, classTag}

object SourceTypes extends Enumeration {
  type SourceType = Value
  val hdfs: SourceType = Value("HDFS")
  val table: SourceType = Value("TABLE")
  val output: SourceType = Value("OUTPUT")
  val virtual: SourceType = Value("VIRTUAL")
  val hive: SourceType = Value("HIVE")
  val hbase: SourceType = Value("HBASE")
  val kafka: SourceType = Value("KAFKA")
  val custom: SourceType = Value("CUSTOM")
}

abstract class SourceConfig {
  def getType: SourceType
  def keyFields: Seq[String]
}

case class HBaseSrcConfig(
                           id: String,
                           table: String,
                           hbaseColumns: Seq[String],
                           keyFields: Seq[String] = Seq.empty,
                           save: Boolean = false
                         ) extends SourceConfig {
  override def getType: SourceType = SourceTypes.hbase
  def getClassTag: ClassTag[(String, String)] = classTag[(String, String)]
}

abstract class VirtualFile(id: String,
                           keyFields: Seq[String],
                           save: Option[HdfsTargetConfig] = None)
  extends SourceConfig {
  override def getType: SourceType = SourceTypes.virtual
  def saveTarget: Option[HdfsTargetConfig] = save
  def parentSourceIds: Seq[String]
}

case class VirtualFileSelect(id: String,
                             parentSourceIds: Seq[String],
                             sqlQuery: String,
                             keyFields: Seq[String],
                             save: Option[HdfsTargetConfig] = None,
                             persist: Option[StorageLevel])
  extends VirtualFile(id, keyFields, save)

case class VirtualFileJoinSql(id: String,
                              parentSourceIds: Seq[String],
                              sqlJoin: String,
                              keyFields: Seq[String],
                              save: Option[HdfsTargetConfig] = None,
                              persist: Option[StorageLevel])
  extends VirtualFile(id, keyFields, save)

case class VirtualFileJoin(id: String,
                           parentSourceIds: Seq[String],
                           joiningColumns: Seq[String],
                           joinType: String,
                           keyFields: Seq[String],
                           save: Option[HdfsTargetConfig] = None,
                           persist: Option[StorageLevel])
  extends VirtualFile(id, keyFields, save)

case class HdfsFile(id: String,
                    path: String,
                    fileType: String,
                    header: Boolean,
                    delimiter: Option[String] = None,
                    quote: Option[String] = None,
                    escape: Option[String] = None,
                    schema: Option[Any] = None,
                    keyFields: Seq[String] = Seq.empty)
  extends SourceConfig {

  def this(tar: HdfsTargetConfig)(implicit settings: DQSettings) = {
    this(
      tar.fileName,
      tar.path + "/" + tar.fileName + s".${tar.fileFormat}",
      tar.fileFormat,
      true,
      tar.delimiter,
      tar.quote,
      tar.escape
    )
  }
  override def getType: SourceType = SourceTypes.hdfs
}

case class OutputFile(
                       id: String,
                       path: String,
                       fileType: String,
                       separator: Option[String],
                       header: Boolean,
                       dependencies: List[String] = List.empty[String],
                       schema: Option[List[GenStructColumn]] = None,
                       keyFields: Seq[String] = Seq.empty
                     ) extends SourceConfig {
  override def getType: SourceType = SourceTypes.output
}

case class TableConfig(
                        id: String,
                        dbId: String,
                        table: Option[String],
                        query: Option[String],
                        username: Option[String],
                        password: Option[String],
                        keyFields: Seq[String] = Seq.empty
                      ) extends SourceConfig {
  override def getType: SourceType = SourceTypes.table
}

case class HiveTableConfig(
                            id: String,
                            query: String,
                            keyFields: Seq[String] = Seq.empty
                          ) extends SourceConfig {
  override def getType: SourceType = SourceTypes.hive
}

case class KafkaSourceConfig(
                              id: String,
                              brokerId: String,
                              topics: Option[Seq[String]],
                              topicPattern: Option[String],
                              format: KafkaFormat,
                              startingOffsets: Option[String] = None,
                              endingOffsets: Option[String] = None,
                              options: Seq[String] = Seq.empty[String],
                              keyFields: Seq[String] = Seq.empty[String]
                            ) extends SourceConfig {
  override def getType: SourceType = SourceTypes.kafka

  val subscribeOption: (String, String) = (topics, topicPattern) match {
    case (Some(topics), None) => if (topics.forall(_.contains("@"))) {
      "assign" -> topics.map(_.split("@")).collect {
        case Array(topic, partitions) => "\"" + topic + "\":" + partitions
      }.mkString("{", ",", "}")
    } else if (topics.forall(!_.contains("@"))) {
      "subscribe" -> topics.mkString(",")
    } else throw IllegalParameterException(
      s"Kafka source (id = $id) configuration error: mixed topic notation - " +
        "all topics must be defined either with partitions to read or without them (read all topic partitions)."
    )
    case (None, Some(pattern)) => "subscribePattern" -> pattern
    case _ => throw IllegalParameterException(
      s"Kafka source (id = $id) configuration error: either topics or topicPattern must be defined but not both."
    )
  }

}

case class CustomSourceConfig(
                               id: String,
                               format: String,
                               path: Option[String] = None,
                               options: Seq[String] = Seq.empty[String],
                               keyFields: Seq[String] = Seq.empty[String]
                             ) extends SourceConfig {
  override def getType: SourceType = SourceTypes.custom
}

case class Source(
                   id: String,
                   df: DataFrame,
                   keyFields: Seq[String] = Seq.empty
                 )
