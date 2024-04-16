package ru.raiffeisen.checkita.config

import enumeratum.{Enum, EnumEntry}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.TimestampType
import ru.raiffeisen.checkita.config.Parsers.idParser

import scala.collection.immutable

object Enums {

  /**
   * Allowed systems for use as data quality history storage
   */
  sealed trait DQStorageType extends EnumEntry
  object DQStorageType extends Enum[DQStorageType] {
    case object Postgres extends DQStorageType
    case object Oracle extends DQStorageType
    case object MySql extends DQStorageType
    case object MSSql extends DQStorageType
    case object H2 extends DQStorageType
    case object SQLite extends DQStorageType
    case object Hive extends DQStorageType
    case object File extends DQStorageType
    override val values: immutable.IndexedSeq[DQStorageType] = findValues
  }
  
  /**
   * Allowed column definitions in schemas:
   */
  sealed trait KafkaTopicFormat extends EnumEntry
  object KafkaTopicFormat extends Enum[KafkaTopicFormat] {
    case object Binary extends KafkaTopicFormat
    case object String extends KafkaTopicFormat
    case object Xml extends KafkaTopicFormat
    case object Json extends KafkaTopicFormat
    case object Avro extends KafkaTopicFormat

    override val values: immutable.IndexedSeq[KafkaTopicFormat] = findValues
  }

  /**
   * Supported stream windowing types, that control how windows are build:
   *   - based on processing time: current timestamp when rows is processed;
   *   - based on event time: message creation timestamp.
   *     Following Kafka message structure, it always must be column with name 'timestamp'.
   *     Column should have LongType or TimestampType or other type with contents that can be converted
   *     to a LongType containing unix epoch (in seconds).
   *   - custom time: arbitrary timestamp column (or expression that evaluates to column) from message defined by user.
   * @note When event time or custom time columns are selected for windowing then it is up to user to ensure
   *       timestamp correctness in the selected column.
   * @note this is not an enumeration since custom time type requires user-defined column name.
   */
  sealed trait StreamWindowing { val windowBy: String }
  case object ProcessingTime extends StreamWindowing { val windowBy: String = "processingTime" }
  case object EventTime extends StreamWindowing { val windowBy: String = "eventTime" }
  case class CustomTime(windowByExpr: Column) extends StreamWindowing {
    val windowBy: String = s"custom(${windowByExpr.toString()}"
  }
  object StreamWindowing {
    private val customPattern = """^custom\((.+)\)$""".r
    def apply(s: String): StreamWindowing = s match {
      case "processingTime" => ProcessingTime
      case "eventTime" => EventTime
      case custom if customPattern.pattern.matcher(custom).matches() =>
        val customPattern(exprStr) = custom
        CustomTime(expr(exprStr))
      case other => throw new IllegalArgumentException(s"Wrong stream windowing type: $other")
    }
  }


  /**
   * Supported file types for FileTypeLoadCheck
   */
  sealed trait FileType extends EnumEntry
  object FileType extends Enum[FileType] {
    case object Delimited extends FileType
    case object Fixed extends FileType
    case object Avro extends FileType
    case object Orc extends FileType
    case object Parquet extends FileType

    override val values: immutable.IndexedSeq[FileType] = findValues
  }

  /**
   * Supported file encodings
   *
   * @param entryName Encoding name as per specification
   */
  sealed abstract class FileEncoding(override val entryName: String) extends EnumEntry
  object FileEncoding extends Enum[FileEncoding] {
    case object Ascii extends FileEncoding("US-ASCII")
    case object Iso extends FileEncoding("ISO-8859-1")
    case object Utf8 extends FileEncoding("UTF-8")
    case object Utf16 extends FileEncoding("UTF-16")
    case object Utf16be extends FileEncoding("UTF-16BE")
    case object Utf16le extends FileEncoding("UTF-16LE")

    override def values: immutable.IndexedSeq[FileEncoding] = findValues
  }
  
  sealed trait SparkJoinType extends EnumEntry
  object SparkJoinType extends Enum[SparkJoinType] {
    case object Inner extends SparkJoinType
    case object Outer extends SparkJoinType
    case object Cross extends SparkJoinType
    case object Full extends SparkJoinType
    case object Right extends SparkJoinType
    case object Left extends SparkJoinType
    case object Semi extends SparkJoinType
    case object Anti extends SparkJoinType
    case object FullOuter extends SparkJoinType
    case object RightOuter extends SparkJoinType
    case object LeftOuter extends SparkJoinType
    case object LeftSemi extends SparkJoinType
    case object LeftAnti extends SparkJoinType
    
    override def values: immutable.IndexedSeq[SparkJoinType] = findValues
  }
  
  
  /**
   * Compare rules for metric parameters
   */
  sealed trait CompareRule extends EnumEntry
  object CompareRule extends Enum[CompareRule] {
    case object Eq extends CompareRule
    case object Lt extends CompareRule
    case object Lte extends CompareRule
    case object Gt extends CompareRule
    case object Gte extends CompareRule

    override def values: immutable.IndexedSeq[CompareRule] = findValues
  }

  /**
   * Precision compare rules for formatted number metric parameters
   */
  sealed trait PrecisionCompareRule extends EnumEntry
  object PrecisionCompareRule extends Enum[PrecisionCompareRule] {
    case object Inbound extends PrecisionCompareRule
    case object Outbound extends PrecisionCompareRule

    override def values: immutable.IndexedSeq[PrecisionCompareRule] = findValues
  }

  /**
   * Trend check window definition rules
   */
  sealed trait TrendCheckRule extends EnumEntry
  object TrendCheckRule extends Enum[TrendCheckRule] {
    case object Record extends TrendCheckRule
    case object Datetime extends TrendCheckRule
    
    override def values: immutable.IndexedSeq[TrendCheckRule] = findValues
  }

  /**
   * Supported result target types
   */
  sealed trait ResultTargetType extends EnumEntry
  object ResultTargetType extends Enum[ResultTargetType] {
    case object RegularMetrics extends ResultTargetType
    case object ComposedMetrics extends ResultTargetType
    case object LoadChecks extends ResultTargetType
    case object Checks extends ResultTargetType
    case object JobState extends ResultTargetType

    override def values: immutable.IndexedSeq[ResultTargetType] = findValues
  }

  sealed trait TemplateFormat extends EnumEntry
  object TemplateFormat extends Enum[TemplateFormat] {
    case object Markdown extends TemplateFormat
    case object Html extends TemplateFormat
    override val values: immutable.IndexedSeq[TemplateFormat] = findValues
  }
}
