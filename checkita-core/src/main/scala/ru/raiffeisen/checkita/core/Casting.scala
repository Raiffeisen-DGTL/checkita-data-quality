package ru.raiffeisen.checkita.core

import org.apache.spark.sql.Row

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.util.Try

/**
 * Helpers used to convert values of type Any to desirable type.
 *
 * The indent of these helpers is to manage values obtained from Spark Row to desired type for use
 * in metric calculators.
 *
 * As the Spark Row can stores elements of various type then we need to guess (pattern match) it to provide
 * an appropriate conversion method.
 *
 * For that purpose we will follow Spark SQL Type to Java types mapping:
 *
 *   - BooleanType -> java.lang.Boolean
 *   - ByteType -> java.lang.Byte
 *   - ShortType -> java.lang.Short
 *   - IntegerType -> java.lang.Integer
 *   - LongType -> java.lang.Long
 *   - FloatType -> java.lang.Float
 *   - DoubleType -> java.lang.Double
 *   - StringType -> String
 *   - DecimalType -> java.math.BigDecimal
 *   - DateType -> java.sql.Date if spark.sql.datetime.java8API.enabled is false
 *   - DateType -> java.time.LocalDate if spark.sql.datetime.java8API.enabled is true
 *   - TimestampType -> java.sql.Timestamp if spark.sql.datetime.java8API.enabled is false
 *   - TimestampType -> java.time.Instant if spark.sql.datetime.java8API.enabled is true
 *   - BinaryType -> byte array
 *   - ArrayType -> scala.collection.Seq (use getList for java.util.List)
 *   - MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
 *   - StructType -> org.apache.spark.sql.Row
 */
object Casting {

  /**
   * Tries to convert array of bytes to double.
   * The approach on casting depends on size of array:
   *   - in case of empty array return None
   *   - in case of single byte return this byte converted to double
   *   - in case of two bytes retrieve short number and convert it to double
   *   - in case of four bytes retrieve integer number and convert it to double
   *   - in case of eight bytes retrieve double itself
   *   - for other lengths try to convert array to string and string to double.
   *   - None if none of the above was successful.
   * @param b Byte array to convert to double
   * @return Some double of conversion was successful or None
   */
  def getDoubleFromBytes(b: Array[Byte]): Option[Double] = b.length match {
      case 0 => None
      case 1 => Some(b.head.toDouble)
      case 2 => Some(ByteBuffer.wrap(b).asShortBuffer().get().toDouble)
      case 4 => Some(ByteBuffer.wrap(b).getInt.toDouble)
      case 8 => Some(ByteBuffer.wrap(b).getDouble)
      case _ => Try(b.map(_.toChar).mkString.toDouble).toOption
    }

  /**
   * Tries to convert array of bytes to long.
   * The approach will differ on size of array:
   *   - in case of empty array return None
   *   - in case of single byte return this byte converted to long
   *   - in case of two bytes retrieve short number and convert it to long
   *   - in case of four bytes retrieve integer number and convert it to long
   *   - in case of eight bytes retrieve long itself
   *   - for other lengths try to convert array to string and string to long.
   *   - None if none of the above was successful.
   * @param b Byte array to convert to long
   * @return Some long of conversion was successful or None
   */
  def getLongFromBytes(b: Array[Byte]): Option[Long] = b.length match {
      case 0 => None
      case 1 => Some(b.head.toLong)
      case 2 => Some(ByteBuffer.wrap(b).asShortBuffer().get().toLong)
      case 4 => Some(ByteBuffer.wrap(b).getInt.toLong)
      case 8 => Some(ByteBuffer.wrap(b).getLong())
      case _ => Try(b.map(_.toChar).mkString.toDouble.toLong).toOption
    }

  /**
   * Converts string to LocalDateTime object provided with format string.
   * @param str String to convert to LocalDateTime
   * @param dateFormat Format string
   * @return Some LocalDateTime instance if conversion was successful or None
   */
  def stringToLocalDateTime(str: String, dateFormat: String): Option[LocalDateTime] = Try{
    val fmt = DateTimeFormatter.ofPattern(dateFormat)
    fmt.parse(str)
  }.flatMap { accessor =>
    val tryDateTime = Try(LocalDateTime.from(accessor))
    val tryDate = Try(LocalDate.from(accessor).atStartOfDay())
    val tryTime = Try(LocalTime.from(accessor).atDate(LocalDate.now))
    tryDateTime orElse tryDate orElse tryTime
  }.toOption

  /**
   * Converts value of primitive to string.
   * @param value value to convert to string
   * @param dtAsLong Boolean flag indicating whether date and time related types should be converted
   *                 to Epoch before converting to string.
   * @return String representation of a value
   */
  def primitiveValToString(value: Any, dtAsLong: Boolean = false): String = value match {
    case bool: java.lang.Boolean => bool.toString
    case byte: java.lang.Byte => byte.toString
    case short: java.lang.Short => short.toString
    case int: java.lang.Integer => int.toString
    case long: java.lang.Long => long.toString
    case float: java.lang.Float => float.toString
    case double: java.lang.Double => double.toString
    case string: String => string
    case char: Char => char.toString
    case decimal: java.math.BigDecimal => decimal.toPlainString
    case date: java.sql.Date => if (dtAsLong) date.toLocalDate.toEpochDay.toString else date.toString
    case localDate: java.time.LocalDate => if (dtAsLong) localDate.toEpochDay.toString else localDate.toString
    case timestamp: java.sql.Timestamp =>
      if (dtAsLong) timestamp.toInstant.toEpochMilli.toString else timestamp.toString
    case instant: java.time.Instant => if (dtAsLong) instant.toEpochMilli.toString else instant.toString
  }

  /**
   * Converts primitive value to Double.
   * @param value Value to convert to double
   * @return Some double value if conversion was successful or None
   *
   * @note Date and time related types are converted to Epoch and then to Double
   */
  def primitiveValToDouble(value: Any): Option[Double] = value match {
    case byte: java.lang.Byte => Some(byte.toDouble)
    case short: java.lang.Short => Some(short.toDouble)
    case int: java.lang.Integer => Some(int.toDouble)
    case long: java.lang.Long => Some(long.toDouble)
    case float: java.lang.Float => Some(float.toDouble)
    case double: java.lang.Double => Some(double)
    case string: String => Try(string.toDouble).toOption
    case char: Char => Try(char.toString.toDouble).toOption
    case decimal: java.math.BigDecimal => Some(decimal.doubleValue())
    case date: java.sql.Date => Some(date.toLocalDate.toEpochDay.toDouble)
    case localDate: java.time.LocalDate => Some(localDate.toEpochDay.toDouble)
    case timestamp: java.sql.Timestamp => Some(timestamp.toInstant.toEpochMilli.toDouble)
    case instant: java.time.Instant => Some(instant.toEpochMilli.toDouble)
    case _ => None
  }

  /**
   * Converts primitive value to Long
   * @param value Value to convert to long
   * @return Some long value if conversion was successful or None
   *
   * @note Date and time related types are converted to Epoch long
   */
  def primitiveValToLong(value: Any): Option[Long] = value match {
    case byte: java.lang.Byte => Some(byte.toLong)
    case short: java.lang.Short => Some(short.toLong)
    case int: java.lang.Integer => Some(int.toLong)
    case long: java.lang.Long => Some(long.toLong)
    case float: java.lang.Float => Some(float.toLong)
    case double: java.lang.Double => Some(double.toLong)
    case string: String => Try(string.toLong).toOption
    case char: Char => Try(char.toString.toLong).toOption
    case decimal: java.math.BigDecimal => Some(decimal.longValue())
    case date: java.sql.Date => Some(date.toLocalDate.toEpochDay)
    case localDate: java.time.LocalDate => Some(localDate.toEpochDay)
    case timestamp: java.sql.Timestamp => Some(timestamp.toInstant.toEpochMilli)
    case instant: java.time.Instant => Some(instant.toEpochMilli)
  }

  /**
   * Recursive function to convert sequence of values (possibly may contain nested traversable structures) to string.
   * String representation is just a concatenation of all primitive values converted to string.
   * @param seq Sequence to convert to string
   * @param acc String accumulator used to store already converted elements.
   * @return String representation of a sequence
   *
   * @note This kind of conversion is used in distinctValues and duplicateValues metric calculators
   *       which use Set to store all unique column tuples.
   *       Therefore, these tuples needs to be serialized as a single string to be properly put to Set.
   *       Alternatively, we could've proceed with serialization to byte array, but benchmarking showed
   *       that string serialization works better (mostly because of lower GC workload).
   *
   * @note Maps and Sets do not guarantee the order of traversing elements, therefore, concatenation of
   *       string representation if their elements could yield different result for the collection with the
   *       same elements. On the other hand, Scala guarantees that set or map with the same elements will
   *       yield the same hashcode. Thus, we chose that approach to represent maps and sets as string.
   *       This is sufficient for the purpose of finding unique column values.
   */
  @tailrec
  def seqToString(seq: Seq[_], acc: String = ""): String =
    if (seq.isEmpty) acc else {
      seq.head match {
        case null => seqToString(seq.tail, acc + s"None@${None.hashCode}")
        case b: Array[Byte] => seqToString(seq.tail, acc + new String(b, StandardCharsets.UTF_8))
        case sq: Seq[_] => seqToString(sq ++ seq.tail, acc)
        case row: Row => seqToString(row.toSeq ++ seq.tail, acc)
        case map: Map[_, _] => seqToString(seq.tail, acc + map.hashCode.toString) // map doesn't guarantee the order of traversing elements
        case set: Set[_] => seqToString(seq.tail, acc + set.hashCode.toString) // set doesn't guarantee the order of traversing elements
        case primitiveValue => seqToString(seq.tail, acc + primitiveValToString(primitiveValue, dtAsLong = true))
      }
    }


  /**
   * Tries to cast primitive value to String.
   * Used in metric calculators.
   *
   * @param value value to cast
   * @return Optional of String value (None if casting wasn't successful)
   *
   * @note Metric calculators are not intended to work complex data types.
   *       Therefore, only primitive types are converted to string as well as byte arrays.
   *       Attempt to convert complex data type such as Map or StructType will return None.
   */
  def tryToString(value: Any): Option[String] = value match {
    case null => None
    case b: Array[Byte] => Some(new String(b, StandardCharsets.UTF_8))
    case otherValue => Try(primitiveValToString(otherValue)).toOption
  }

  /**
   * Tries to cast primitive value to Double.
   * Used in metric calculators.
   *
   * @param value value to cast
   * @return Optional Double value (None if casting wasn't successful)
   *
   * @note Metric calculators are not intended to work complex data types.
   *       Therefore, only primitive types are converted to double as well as byte arrays.
   *       Attempt to convert complex data type such as Map or StructType will return None.
   */
  def tryToDouble(value: Any): Option[Double] = value match {
    case null => None
    case b: Array[Byte] => getDoubleFromBytes(b)
    case otherValue => primitiveValToDouble(otherValue)
  }

  /**
   * Tries to cast any value to Long.
   * Used in metric calculators.
   *
   * @param value value to cast
   * @return Optional Long value (None if casting wasn't successful)
   *
   * @note Metric calculators are not intended to work complex data types.
   *       Therefore, only primitive types are converted to long as well as byte arrays.
   *       Attempt to convert complex data type such as Map or StructType will return None.
   */
  def tryToLong(value: Any): Option[Long] = value match {
    case null => None
    case b: Array[Byte] => getLongFromBytes(b)
    case otherValue => primitiveValToLong(otherValue)
  }

  /**
   * Tries to cast primitive value to LocalDateTime object
   * for use in date-related metrics calculators.
   *
   * @param value      - value to cast
   * @param dateFormat - date format used for casting
   * @return Optional LocalDateTime object (None if casting wasn't successful)
   *
   * @note Metric calculators are not intended to work complex data types.
   *       Therefore, only primitive types can be converted to LocalDateTime as well as byte arrays.
   *       Attempt to convert complex data type such as Map or StructType will return None.
   */
  def tryToDate(value: Any, dateFormat: String): Option[LocalDateTime] =
    value match {
      case null => None
      case date: java.sql.Date => Some(date.toLocalDate.atStartOfDay())
      case localDate: java.time.LocalDate => Some(localDate.atStartOfDay())
      case timestamp: java.sql.Timestamp => Some(timestamp.toLocalDateTime)
      case instant: java.time.Instant => Some(LocalDateTime.ofInstant(instant, ZoneOffset.UTC))
      case b: Array[Byte] => stringToLocalDateTime(new String(b, StandardCharsets.UTF_8), dateFormat)
      case otherValue =>
        Try(primitiveValToString(otherValue)).toOption.flatMap(stringToLocalDateTime(_, dateFormat))
    }
}
