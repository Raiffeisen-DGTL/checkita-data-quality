package ru.raiffeisen.checkita.config

import eu.timepit.refined.W
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.{MinSize, NonEmpty, Size}
import eu.timepit.refined.generic.Equal
import eu.timepit.refined.numeric.{Interval, Positive}
import eu.timepit.refined.string._

import java.time.format.DateTimeFormatter

object RefinedTypes {
//  type ID = String Refined MatchesRegex[W.`"""^[a-zA-Z0-9_]+$"""`.T]
  type URI = String Refined Uri
  type URL = String Refined Url
  type Port = Int Refined Interval.Closed[W.`0`.T, W.`9999`.T]
  type Email = String Refined MatchesRegex[W.`"""^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"""`.T]
  type MMRecipient = String Refined MatchesRegex[W.`"""^(@|#)[a-zA-Z][a-zA-Z0-9.\\-_]+$"""`.T]
  type SparkParam = String Refined MatchesRegex[W.`"""^\\S+?\\=[\\S\\s]+$"""`.T]
  type PositiveInt = Int Refined Positive
  type FixedShortColumn = String Refined MatchesRegex[W.`"""^[^\\n\\r\\t:]+:\\d+$"""`.T]
  type AccuracyDouble = Double Refined Interval.Closed[W.`0.0`.T, W.`1.0`.T]
  type RegexPattern = String Refined Regex
  
  /**
   * Various refinements for string sequences:
   */
  type NonEmptyStringSeq = Seq[String] Refined NonEmpty
  type SingleElemStringSeq = Seq[String] Refined Size[Equal[W.`1`.T]]
  type DoubleElemStringSeq = Seq[String] Refined Size[Equal[W.`2`.T]]
  type MultiElemStringSeq = Seq[String] Refined MinSize[W.`2`.T]

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
   * DateFormat class is used to store both date-time pattern and corresponding formatter.
   * Such construction allows to verify if pattern is convertable to formatter at the point
   * of configuration reading and also supports backwards conversion (when writing configuration).
   * The issue with backwards conversion is due to DateTimeFormatter does not store string representation
   * of original datetime pattern.
   * @note DataTimeFormatter is not serializable, therefore, it is marked as @transient 
   *       to avoid serialization errors in Spark Tasks.
   * @param pattern - datetime pattern string
   * @param formatter - datetime formatter for pattern
   */
  case class DateFormat(pattern: String, @transient formatter: DateTimeFormatter)
  object DateFormat {
    def fromString(s: String): DateFormat = DateFormat(s, DateTimeFormatter.ofPattern(s))
  }
}
