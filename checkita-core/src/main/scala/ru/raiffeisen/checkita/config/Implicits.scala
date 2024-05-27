package ru.raiffeisen.checkita.config

import org.apache.commons.validator.routines.EmailValidator
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.DataType
import org.apache.spark.storage.StorageLevel
import pureconfig.generic.{FieldCoproductHint, ProductHint}
import pureconfig.{CamelCase, ConfigConvert, ConfigFieldMapping}
import ru.raiffeisen.checkita.config.Enums._
import ru.raiffeisen.checkita.config.Parsers.idParser
import ru.raiffeisen.checkita.config.RefinedTypes.{DateFormat, Email, ID}
import ru.raiffeisen.checkita.config.jobconf.Outputs.FileOutputConfig
import ru.raiffeisen.checkita.config.jobconf.Schemas.SchemaConfig
import ru.raiffeisen.checkita.config.jobconf.Sources.{FileSourceConfig, VirtualSourceConfig}
import ru.raiffeisen.checkita.utils.SparkUtils.{DurationOps, toDataType, toStorageLvlString}

import java.time.ZoneId
import java.util.TimeZone
import scala.concurrent.duration.Duration

/**
 * Implicit pureconfig hints and converters for specific types used in Job Configuration.
 */
object Implicits {
  
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

  implicit val emailConverter: ConfigConvert[Email] = ConfigConvert[String].xmap[Email](
    emailString =>
      if (EmailValidator.getInstance().isValid(emailString)) Email(emailString)
      else throw new IllegalArgumentException(s"Email '$emailString' is not valid."),
    email => email.value
  )

  implicit val dateFormatConverter: ConfigConvert[DateFormat] =
    ConfigConvert[String].xmap[DateFormat](DateFormat, _.pattern)

  implicit val timeZoneConverter: ConfigConvert[ZoneId] =
    ConfigConvert[String].xmap[ZoneId](
      s => TimeZone.getTimeZone(s).toZoneId,
      tz => TimeZone.getTimeZone(tz).getDisplayName
    )

  implicit val metricEnginAPIConverter: ConfigConvert[MetricEngineAPI] =
    ConfigConvert[String].xmap[MetricEngineAPI](MetricEngineAPI.withNameInsensitive, _.toString.toLowerCase)

  implicit val dqStorageTypeConverter: ConfigConvert[DQStorageType] =
    ConfigConvert[String].xmap[DQStorageType](DQStorageType.withNameInsensitive, _.toString.toLowerCase)
    
  implicit val dqDataTypeConverter: ConfigConvert[DataType] =
    ConfigConvert[String].xmap[DataType](
      toDataType,
      t => t.toString.toLowerCase.replace("type", "")
    )

  implicit val durationTypeConverter: ConfigConvert[Duration] =
    ConfigConvert[String].xmap[Duration](Duration(_), _.toShortString)

  implicit val kafkaWindowingConverter: ConfigConvert[StreamWindowing] =
    ConfigConvert[String].xmap[StreamWindowing](StreamWindowing(_), _.windowBy)

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
