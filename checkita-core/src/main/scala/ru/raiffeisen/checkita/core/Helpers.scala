package ru.raiffeisen.checkita.core

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import scala.util.Try

object Helpers {

  /**
   * Tries to cast any value to String.
   * Used in metric calculators.
   *
   * @param value value to cast
   * @return Optional of String value (None if casting wasn't successful)
   */
  def tryToString(value: Any): Option[String] = value match {
    case null => None
    case x: String => Some(x)
    case x => Try(x.toString).toOption
  }

  /**
   * Tries to cast any value to LocalDateTime object
   * for use in date-related metrics calculators.
   *
   * @param value      - value to cast
   * @param dateFormat - date format used for casting
   * @return Optional LocalDateTime object (None if casting wasn't successful)
   */
  def tryToDate(value: Any, dateFormat: String): Option[LocalDateTime] = {
    value match {
      case null => None
      case x: Timestamp => Some(x.toLocalDateTime)
      case x => Try {
        val fmt = DateTimeFormatter.ofPattern(dateFormat)
        val str = tryToString(x).get
        fmt.parse(str)
      }.flatMap { accessor =>
        val tryDateTime = Try(LocalDateTime.from(accessor))
        val tryDate = Try(LocalDate.from(accessor).atStartOfDay())
        val tryTime = Try(LocalTime.from(accessor).atDate(LocalDate.now))
        tryDateTime orElse tryDate orElse tryTime
      }.toOption
    }
  }

  /**
   * Tries to cast array of bytes to double.
   */
  private val binaryCasting: Array[Byte] => Option[Double] = (bytes: Array[Byte]) =>
    if (bytes == null) None else Option(bytes.map(b => b.toChar).mkString.toDouble)

  /**
   * Tries to cast any value to Double.
   * Used in metric calculators.
   *
   * @param value value to cast
   * @return Optional Double value (None if casting wasn't successful)
   */
  def tryToDouble(value: Any): Option[Double] = value match {
    case null => None
    case x: Double => Some(x)
    case x: Array[Byte] => binaryCasting(x)
    case x => Try(x.toString.toDouble).toOption
  }

  /**
   * Tries to cast any value to Long.
   * Used in metric calculators.
   *
   * @param value value to cast
   * @return Optional Long value (None if casting wasn't successful)
   */
  def tryToLong(value: Any): Option[Long] = value match {
    case null => None
    case x: Long => Some(x)
    case x: Int => Some(x.toLong)
    case x: Array[Byte] => binaryCasting(x).map(_.toLong)
    case x => Try(x.toString.toLong).toOption
  }
}
