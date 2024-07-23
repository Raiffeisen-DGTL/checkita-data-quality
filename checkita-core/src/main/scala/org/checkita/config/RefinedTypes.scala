package org.checkita.config

import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.{MinSize, NonEmpty, Size}
import eu.timepit.refined.generic.Equal
import eu.timepit.refined.numeric.{Interval, Positive}
import eu.timepit.refined.string._

import java.time.format.DateTimeFormatter

object RefinedTypes {

  type URI = String Refined Uri
  type URL = String Refined Url
  type Port = Int Refined Interval.Closed[W.`0`.T, W.`9999`.T]
  type MMRecipient = String Refined MatchesRegex[W.`"""^(@|#)[a-zA-Z][a-zA-Z0-9.\\-_]+$"""`.T]
  type SparkParam = String Refined MatchesRegex[W.`"""^\\S+?\\=[\\S\\s]+$"""`.T]
  type PositiveInt = Int Refined Positive
  type FixedShortColumn = String Refined MatchesRegex[W.`"""^[^\\n\\r\\t:]+:\\d+$"""`.T]
  type PercentileDouble = Double Refined Interval.Closed[W.`0.0`.T, W.`1.0`.T]
  type AccuracyDouble = Double Refined Interval.OpenClosed[W.`0.0`.T, W.`1.0`.T]
  type RegexPattern = String Refined Regex

  /**
   * Refinement for string holding secret key for sensitive data encryption.
   * This string should contain at least 32 characters.
   */
  type EncryptionKey = String Refined MatchesRegex[W.`"""^.{32}.*$"""`.T]
  
  /**
   * Various refinements for string sequences:
   */
  type NonEmptyStringSeq = Seq[String] Refined NonEmpty
  type SingleElemStringSeq = Seq[String] Refined Size[Equal[W.`1`.T]]
  type DoubleElemStringSeq = Seq[String] Refined Size[Equal[W.`2`.T]]
  type MultiElemStringSeq = Seq[String] Refined MinSize[W.`2`.T]
  type NonEmptyURISeq = Seq[URI] Refined NonEmpty
  
  /**
   * Refinements for double sequences:
   */
  type NonEmptyDoubleSeq = Seq[Double] Refined NonEmpty

  /**
   * All IDs are parsers via SparkSqlParser so they are valid SparkSQL identifiers
   * This is required to eliminate any issues with interoperability with Spark.
   * For example, source IDs can be used to create virtual source: 
   * in this case parent source are registered as temporary views in Spark.
   * @param value Validated ID value
   */
  case class ID(value: String)

  /**
   * Email class is used to wrap email addresses and provide email validation
   * during configuration parsing.
   * @param value Validated email address
   */
  case class Email(value: String)

  /**
   * DateFormat class is used to store both date-time pattern and corresponding formatter.
   * Such construction allows to verify if pattern is convertable to formatter at the point
   * of configuration reading and also supports backwards conversion (when writing configuration).
   * The issue with backwards conversion is due to DateTimeFormatter does not store string representation
   * of original datetime pattern.
   * @note DataTimeFormatter is not serializable, therefore, it is marked as @transient 
   *       to avoid serialization errors in Spark Tasks.
   * @param pattern - datetime pattern string
   */
  case class DateFormat(pattern: String) {
    @transient val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
  }
//  object DateFormat {
//    def fromString(s: String): DateFormat = DateFormat(s, DateTimeFormatter.ofPattern(s))
//  }
}
