package org.checkita.utils

import org.checkita.config.RefinedTypes.DateFormat

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId, ZoneOffset, ZonedDateTime}
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.util.Try

/**
 * Wrapper over datetime string with predefined format and time zone.
 * Used to yield various datetime objects and operate with them.
 * @param dateFormat Datetime format
 * @param timeZone Time zone
 * @param zonedDT Zoned date time wrapped into EnrichedDT instance.
 */
class EnrichedDT(val dateFormat: DateFormat,
                 val timeZone: ZoneId,
                 protected val zonedDT: ZonedDateTime) {
  
  /** Formatter with required time zone */
  private val formatter = dateFormat.formatter.withZone(timeZone)
  
  /**
   * Zoned DateTime getter
   * @return Current ZonedDateTime
   */
  def getDateTime: ZonedDateTime = zonedDT

  /**
   * Render ZonedDateTime to a string with predefined format
   * @return DateTime string
   */
  def render: String = zonedDT.format(formatter)
    
  /**
   * Transforms ZonedDateTime to timestamp at UTC time zone
   * @return Timestamp at UTC tz
   */
  def getUtcTS: Timestamp = Timestamp.valueOf(
    zonedDT.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime
  )

  /**
   * Creates new EnrichedDT instance by offsetting current one by offset duration.
   * Maximum offset precision is seconds.
   * @param offset Offset duration (backwards)
   * @return New EnrichedDT instance with offset
   */
  def withOffset(offset: Duration): EnrichedDT = new EnrichedDT(
    this.dateFormat,
    this.timeZone,
    this.zonedDT.minusSeconds(offset.toSeconds)
  )

  /**
   * Offsets current datetime back for a given offset and transforms new value to timestamp at UTC time zone.
   * @param offset Offset from current datetime (duration)
   * @return Timestamp at UTC tz with offset
   */
  def getUtcTsWithOffset(offset: Duration): Timestamp = Timestamp.valueOf(
    zonedDT.minusSeconds(offset.toSeconds).withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime
  )

  /**
   * Returns a new EnrichedDT instance with the same date format and time zone, but with time
   * being set to current time.
   * @return New instance of EnrichedDT for current time.
   */
  def resetToCurrentTime: EnrichedDT = new EnrichedDT(this.dateFormat, this.timeZone, ZonedDateTime.now(timeZone))
  
  def setTo(ts: Timestamp): EnrichedDT = EnrichedDT(this.dateFormat, this.timeZone, ts)
  def setTo(epoch: Long): EnrichedDT = EnrichedDT(this.dateFormat, this.timeZone, epoch)
  def setTo(instant: Instant): EnrichedDT = EnrichedDT(this.dateFormat, this.timeZone, instant)
}

object EnrichedDT {
  
  private val utcZone = ZoneId.of("UTC")

  /**
   * Builds EnrichedDT instance from optional date string: parses date string into ZonedDateTime.
   * 
   * The problem with date string parsing is that if format contains only date part
   * then it is only possible to retrieve LocalDate. An attempt retrieve LocalDateTime
   * for such format will throw an exception. The same is true when format contains only time part.
   * In this case it is only possible to retrieve LocalTime.
   * Thus, parsing of date string has chained all these attempts with following priority:
   *   - try to get LocalDateTime
   *   - try to get LocalDate
   *   - try to get LocalTime
   * First one to succeed will be returned. If none of these attempts succeeds then exception will be thrown.  
   * 
   * If no date string provided then builds EnrichedDT instance with datetime set to current datetime.
   */
  def apply(dateFormat: DateFormat, timeZone: ZoneId, dateString: Option[String] = None): EnrichedDT = {
    val formatter = dateFormat.formatter
    val zonedDT: ZonedDateTime = if (dateString.isEmpty) ZonedDateTime.now(timeZone) else {
      val accessor = formatter.parse(dateString.get)
      val tryDateTime = Try(LocalDateTime.from(accessor).atZone(timeZone))
      val tryDate = Try(LocalDate.from(accessor).atStartOfDay(timeZone))
      val tryTime = Try(LocalTime.from(accessor).atDate(LocalDate.now).atZone(timeZone))
      val dt = tryDateTime orElse tryDate orElse tryTime
      dt.getOrElse(throw new IllegalArgumentException(
        s"Cannot parse datetime string '$dateString' with formatter of pattern '${dateFormat.pattern}'."
      ))
    }
    new EnrichedDT(dateFormat, timeZone, zonedDT)
  }

  /**
   * Builds EnrichedDT instance from instant.
   *
   * @param dateFormat Date format
   * @param timeZone   Time zone
   * @param instant    Instant object
   * @return EnrichedDT instance.
   */
  def apply(dateFormat: DateFormat, timeZone: ZoneId, instant: Instant): EnrichedDT = {
    val zdt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault())
      .atZone(utcZone).withZoneSameInstant(timeZone)
    new EnrichedDT(dateFormat, timeZone, zdt)
  }


  /**
   * Builds EnrichedDT instance from UTC timestamp.
   *
   * @param dateFormat Date format
   * @param timeZone   Time zone
   * @param ts         UTC timestamp
   * @return EnrichedDT instance
   */
  def apply(dateFormat: DateFormat, timeZone: ZoneId, ts: Timestamp): EnrichedDT = 
    EnrichedDT(dateFormat, timeZone, ts.toInstant)

  /**
   * Builds EnrichedDT instance from Unix epoch (in seconds)
   *
   * @param epoch      Unix epoch
   * @param dateFormat Date format
   * @param timeZone   Time zone
   * @return EnrichedDT instance
   */
  def apply(dateFormat: DateFormat, timeZone: ZoneId, epoch: Long): EnrichedDT = 
    EnrichedDT(dateFormat, timeZone, Instant.ofEpochSecond(epoch))

  /**
   * Auxiliary constructor from date string with implicit date format and time zone.
   */
  def apply(dateString: Option[String])
           (implicit dateFormat: DateFormat, timeZone: ZoneId): EnrichedDT = 
    EnrichedDT(dateFormat, timeZone, dateString)


  /**
   * Auxiliary constructor from instant with implicit date format and time zone.
   */
  def apply(instant: Instant)
           (implicit dateFormat: DateFormat, timeZone: ZoneId): EnrichedDT =
    EnrichedDT(dateFormat, timeZone, instant)

  /**
   * Auxiliary constructor from timestamp with implicit date format and time zone.
   */
  def apply(ts: Timestamp)
           (implicit dateFormat: DateFormat, timeZone: ZoneId): EnrichedDT =
    EnrichedDT(dateFormat, timeZone, ts)

  /**
   * Auxiliary constructor from epoch seconds with implicit date format and time zone.
   */
  def apply(epoch: Long)
           (implicit dateFormat: DateFormat, timeZone: ZoneId): EnrichedDT =
    EnrichedDT(dateFormat, timeZone, epoch)
}
