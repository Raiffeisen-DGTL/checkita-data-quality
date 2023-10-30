package ru.raiffeisen.checkita.config

import org.apache.spark.sql.Column
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.storage.StorageLevel
import pureconfig.generic.{FieldCoproductHint, ProductHint}
import pureconfig.{CamelCase, ConfigConvert, ConfigFieldMapping}
import ru.raiffeisen.checkita.config.Enums._
import ru.raiffeisen.checkita.config.RefinedTypes.{DateFormat, ID}
import ru.raiffeisen.checkita.config.jobconf.Outputs.FileOutputConfig
import ru.raiffeisen.checkita.config.jobconf.Schemas.SchemaConfig
import ru.raiffeisen.checkita.config.jobconf.Sources.{FileSourceConfig, VirtualSourceConfig}
import ru.raiffeisen.checkita.utils.SparkUtils.{toDataType, toStorageLvlString}

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.TimeZone

/**
 * Implicit pureconfig hints and converters for specific types used in Job Configuration.
 */
object Implicits {
  
  /** SparkSqlParser is used to validate IDs */
  private val idParser = new SparkSqlParser(new SQLConf())
  
  implicit def hint[A]: ProductHint[A] = ProductHint[A](
    ConfigFieldMapping(CamelCase, CamelCase),
    allowUnknownKeys = false
  )

  implicit val idConverter: ConfigConvert[ID] =
    ConfigConvert[String].xmap[ID](
      idString => {
        val _ = idParser.parseTableIdentifier(idString)
        ID(idString) // return ID if idString can be parsed as Spark table identifier
      },
      id => id.value
    )
    
  implicit val dateFormatConverter: ConfigConvert[DateFormat] =
    ConfigConvert[String].xmap[DateFormat](
      pattern => DateFormat(pattern, DateTimeFormatter.ofPattern(pattern)),
      dateFormat => dateFormat.pattern
    )

  implicit val timeZoneConverter: ConfigConvert[ZoneId] =
    ConfigConvert[String].xmap[ZoneId](
      s => TimeZone.getTimeZone(s).toZoneId,
      tz => TimeZone.getTimeZone(tz).getDisplayName
    )
    
  implicit val dqStorageTypeConverter: ConfigConvert[DQStorageType] =
    ConfigConvert[String].xmap[DQStorageType](DQStorageType.withNameInsensitive, _.toString.toLowerCase)
    
  implicit val dqDataTypeReader: ConfigConvert[DataType] =
    ConfigConvert[String].xmap[DataType](
      toDataType,
      t => t.toString.toLowerCase.replace("type", "")
    )

  private val dropRight: String => String => String =
    dropText => s => s.dropRight(dropText.length).zipWithIndex.map(t => if (t._2 == 0) t._1.toLower else t._1).mkString

  implicit val schemaHint: FieldCoproductHint[SchemaConfig] =
    new FieldCoproductHint[SchemaConfig]("kind") {
      override def fieldValue(name: String): String = dropRight("SchemaConfig")(name)
    }

  implicit val hdfsSourceHint: FieldCoproductHint[FileSourceConfig] =
    new FieldCoproductHint[FileSourceConfig]("kind") {
      override def fieldValue(name: String): String = dropRight("FileSourceConfig")(name)
    }

  implicit val kafkaTopicFormatConverter: ConfigConvert[KafkaTopicFormat] =
    ConfigConvert[String].xmap[KafkaTopicFormat](KafkaTopicFormat.withNameInsensitive, _.toString.toLowerCase)

  implicit val fileEncodingConverter: ConfigConvert[FileEncoding] =
    ConfigConvert[String].xmap[FileEncoding](FileEncoding.withNameInsensitive, _.entryName)

  implicit val fileTypeConverter: ConfigConvert[FileType] =
    ConfigConvert[String].xmap[FileType](FileType.withNameInsensitive, _.toString.toLowerCase)

  implicit val storageLevelConverter: ConfigConvert[StorageLevel] =
    ConfigConvert[String].xmap[StorageLevel](
      s => StorageLevel.fromString(s.toUpperCase),
      toStorageLvlString
    )

  implicit val virtualSourceHint: FieldCoproductHint[VirtualSourceConfig] =
    new FieldCoproductHint[VirtualSourceConfig]("kind") {
      override def fieldValue(name: String): String = dropRight("VirtualSourceConfig")(name)
    }
    
  implicit val fileOutputHint: FieldCoproductHint[FileOutputConfig] =
    new FieldCoproductHint[FileOutputConfig]("kind") {
      override def fieldValue(name: String): String = dropRight("FileOutputConfig")(name)
    }

  implicit val jointTypeConverter: ConfigConvert[SparkJoinType] =
    ConfigConvert[String].xmap[SparkJoinType](
      SparkJoinType.withNameInsensitive,
      _.toString.zipWithIndex.map(t => if (t._2 == 0) t._1.toLower else t._1).mkString
    )
  
  implicit val sparkExprConverter: ConfigConvert[Column] = ConfigConvert[String].xmap[Column](expr, _.toString)
  
  implicit val compareRuleConverter: ConfigConvert[CompareRule] =
    ConfigConvert[String].xmap[CompareRule](CompareRule.withNameInsensitive, _.toString.toLowerCase)

  implicit val precisionCompareRuleConverter: ConfigConvert[PrecisionCompareRule] =
    ConfigConvert[String].xmap[PrecisionCompareRule](PrecisionCompareRule.withNameInsensitive, _.toString.toLowerCase)

  implicit val trendCheckRuleConverter: ConfigConvert[TrendCheckRule] =
    ConfigConvert[String].xmap[TrendCheckRule](TrendCheckRule.withNameInsensitive, _.toString.toLowerCase)

  implicit val resultTargetTypeConverter: ConfigConvert[ResultTargetType] =
    ConfigConvert[String].xmap[ResultTargetType](
      ResultTargetType.withNameInsensitive,
      _.toString.zipWithIndex.map(t => if (t._2 == 0) t._1.toLower else t._1).mkString
    )
}
