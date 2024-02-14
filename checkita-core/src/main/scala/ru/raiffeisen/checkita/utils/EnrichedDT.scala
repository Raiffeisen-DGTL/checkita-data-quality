package ru.raiffeisen.checkita.utils

import ru.raiffeisen.checkita.config.RefinedTypes.DateFormat

import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.util.Try

/**
 * Wrapper over datetime string with predefined format and time zone.
 * Used to yield various datetime objects and operate with them.
 * @param dateFormat Datetime format
 * @param timeZone Time zone
 * @param dateString Datetime string
 */
case class EnrichedDT(dateFormat: DateFormat, timeZone: ZoneId, dateString: Option[String] = None) {

  /** Formatter with required time zone */
  private val formatter = dateFormat.formatter.withZone(timeZone)

  /**
   * Parse date string into ZonedDateTime.
   * The problem with date string parsing is that if format contains only date part
   * then it is only possible to retrieve LocalDate. An attempt retrieve LocalDateTime
   * for such format will throw an exception. The same is true when format contains only time part.
   * In this case it is only possible to retrieve LocalTime.
   * Thus, parsing of date string has chained all these attempts with following priority:
   *   - try to get LocalDateTime
   *   - try to get LocalDate
   *   - try to get LocalTime
   * First one to succeed will be returned. If none of these attempts succeeds then exception will be thrown.  
   */
  private val zonedDT: ZonedDateTime = if (dateString.isEmpty) ZonedDateTime.now(timeZone) else {
    val accessor = formatter.parse(dateString.get)
    val tryDateTime = Try(LocalDateTime.from(accessor).atZone(timeZone))
    val tryDate = Try(LocalDate.from(accessor).atStartOfDay(timeZone))
    val tryTime = Try(LocalTime.from(accessor).atDate(LocalDate.now).atZone(timeZone))
    val dt = tryDateTime orElse tryDate orElse tryTime
    dt.getOrElse(throw new IllegalArgumentException(
      s"Cannot parse datetime string '$dateString' with formatter of pattern '${dateFormat.pattern}'."
    ))
  }
  
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
  def withOffset(offset: Duration): EnrichedDT = EnrichedDT(
    this.dateFormat,
    this.timeZone,
    Some(this.zonedDT.minusSeconds(offset.toSeconds).format(formatter))
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
  def resetToCurrentTime: EnrichedDT = EnrichedDT(this.dateFormat, this.timeZone)
}

object EnrichedDT {
  /**
   * Builds EnrichedDT instance from Unix epoch (in seconds)
   * @param epoch Unix epoch
   * @param dateFormat Date format
   * @param timeZone Time zone
   * @return EnrichedDT instance
   */
  def fromEpoch(epoch: Long,
                dateFormat: DateFormat = DateFormat.fromString("yyyy-MM-dd HH:mm:ss"),
                timeZone: ZoneId = ZoneId.systemDefault()
               ): EnrichedDT = {
    val zoneOffset = timeZone.getRules.getOffset(Instant.now())
    val localDt = LocalDateTime.ofEpochSecond(epoch, 0, zoneOffset)
    val localDtString = localDt.format(dateFormat.formatter)
    EnrichedDT(dateFormat, timeZone, Some(localDtString))
  }
}
