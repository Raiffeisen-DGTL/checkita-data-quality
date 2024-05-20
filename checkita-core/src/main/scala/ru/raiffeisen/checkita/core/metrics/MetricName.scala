package ru.raiffeisen.checkita.core.metrics

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

/**
 * Enumeration holding all metric calculator names
 * @param entryName metric calculator name
 */
sealed abstract class MetricName(override val entryName: String) extends EnumEntry with Serializable
object MetricName extends Enum[MetricName] {
  case object Composed extends MetricName("COMPOSED")
  case object RowCount extends MetricName("ROW_COUNT")
  case object NullValues extends MetricName("NULL_VALUES")
  case object EmptyValues extends MetricName("EMPTY_VALUES")
  case object DuplicateValues extends MetricName("DUPLICATE_VALUES")
  case object Completeness extends MetricName("COMPLETENESS")
  case object SequenceCompleteness extends MetricName("SEQUENCE_COMPLETENESS")
  case object ApproximateSequenceCompleteness extends MetricName("APPROXIMATE_SEQUENCE_COMPLETENESS")
  case object DistinctValues extends MetricName("DISTINCT_VALUES")
  case object ApproximateDistinctValues extends MetricName("APPROXIMATE_DISTINCT_VALUES")
  case object RegexMatch extends MetricName("REGEX_MATCH")
  case object RegexMismatch extends MetricName("REGEX_MISMATCH")
  case object MinString extends MetricName("MIN_STRING")
  case object MaxString extends MetricName("MAX_STRING")
  case object AvgString extends MetricName("AVG_STRING")
  case object FormattedDate extends MetricName("FORMATTED_DATE")
  case object StringLength extends MetricName("STRING_LENGTH")
  case object StringInDomain extends MetricName("STRING_IN_DOMAIN")
  case object StringOutDomain extends MetricName("STRING_OUT_DOMAIN")
  case object StringValues extends MetricName("STRING_VALUES")
  case object MinNumber extends MetricName("MIN_NUMBER")
  case object MaxNumber extends MetricName("MAX_NUMBER")
  case object SumNumber extends MetricName("SUM_NUMBER")
  case object StdNumber extends MetricName("STD_NUMBER")
  case object AvgNumber extends MetricName("AVG_NUMBER")
  case object FormattedNumber extends MetricName("FORMATTED_NUMBER")
  case object CastedNumber extends MetricName("CASTED_NUMBER")
  case object NumberInDomain extends MetricName("NUMBER_IN_DOMAIN")
  case object NumberOutDomain extends MetricName("NUMBER_OUT_DOMAIN")
  case object NumberValues extends MetricName("NUMBER_VALUES")
  case object NumberLessThan extends MetricName("NUMBER_LESS_THAN")
  case object NumberGreaterThan extends MetricName("NUMBER_GREATER_THAN")
  case object NumberBetween extends MetricName("NUMBER_BETWEEN")
  case object NumberNotBetween extends MetricName("NUMBER_NOT_BETWEEN")
  case object MedianValue extends MetricName("MEDIAN_VALUE")
  case object FirstQuantile extends MetricName("FIRST_QUANTILE")
  case object ThirdQuantile extends MetricName("THIRD_QUANTILE")
  case object GetQuantile extends MetricName("GET_QUANTILE")
  case object GetPercentile extends MetricName("GET_PERCENTILE")
  case object CoMoment extends MetricName("CO_MOMENT")
  case object Covariance extends MetricName("COVARIANCE")
  case object CovarianceBessel extends MetricName("COVARIANCE_BESSEL")
  case object ColumnEq extends MetricName("COLUMN_EQ")
  case object DayDistance extends MetricName("DAY_DISTANCE")
  case object LevenshteinDistance extends MetricName("LEVENSHTEIN_DISTANCE")
  case object TopN extends MetricName("TOP_N")
  
  override def values: immutable.IndexedSeq[MetricName] = findValues
}
