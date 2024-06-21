package ru.raiffeisen.checkita.config.jobconf

import eu.timepit.refined.auto._
import eu.timepit.refined.types.string.NonEmptyString
import ru.raiffeisen.checkita.config.Enums.{CompareRule, PrecisionCompareRule}
import ru.raiffeisen.checkita.config.RefinedTypes._

import java.time.format.DateTimeFormatter

object MetricParams {

  /**
   * Base class for metric parameters configurations
   */
  sealed abstract class Params

  /**
   * Parameters for approximateDistinctValues metric
   * @param accuracyError Calculation accuracy error in interval [0,1]
   *                      Default: 0.01
   */
  final case class ApproxDistinctValuesParams(accuracyError: AccuracyDouble = 0.01) extends Params

  /**
   * Parameters for completeness metric
   * @param includeEmptyStrings boolean flag indicating whether empty strings should be taken into account.
   *                            Default: false - only null values are considered.
   */
  final case class CompletenessParams(includeEmptyStrings: Boolean = false) extends Params

  /**
   * Parameters for sequence completeness metric
   * @param increment sequence increment. Default: 1
   */
  final case class SequenceCompletenessParams(increment: PositiveInt = 1) extends Params

  /**
   * Parameters for sequence completeness metric
   * @param increment sequence increment. Default: 1
   * @param accuracyError Calculation accuracy error in interval [0,1]
   *                      Default: 0.01
   */
  final case class ApproxSequenceCompletenessParams(
                                                     increment: PositiveInt = 1,
                                                     accuracyError: AccuracyDouble = 0.01
                                                   ) extends Params
    
  /**
   * Parameters for string length metric
   * @param length Required string length
   * @param compareRule Compare rule to use for metric calculation
   */
  final case class StringLengthParams(length: PositiveInt, compareRule: CompareRule) extends Params

  /**
   * Parameters for string domain metric
   * @param domain Sequence of string values that forms a domain
   */
  final case class StringDomainParams(domain: NonEmptyStringSeq) extends Params

  /**
   * Parameters for number domain metric
   * @param domain Sequence of string values that forms a domain
   */
  final case class NumberDomainParams(domain: NonEmptyDoubleSeq) extends Params
  
  /**
   * Parameters for string values metric
   * @param compareValue Required string value
   */
  final case class StringValuesParams(compareValue: NonEmptyString) extends Params

  /**
   * Parameters for number values metric
   * @param compareValue Required number value
   */
  final case class NumberValuesParams(compareValue: Double) extends Params
  
  
  /**
   * Parameters for regex match and mismatch metrics
   * @param regex Regex pattern to check for match/mismatch
   */
  final case class RegexParams(regex: RegexPattern) extends Params

  /**
   * Parameters for formatted date metric
   * @param dateFormat Required datetime format pattern. Default: "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
   */
  final case class FormattedDateParams(
                                        dateFormat: DateFormat = DateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                                      ) extends Params

  /**
   * Parameters for formatted number metric
   * @param precision Required number precision
   * @param scale Required number scale
   * @param compareRule Precision compare rule (whether number should fit to required precision and scale or not)
   */
  final case class FormattedNumberParams(
                                          precision: PositiveInt,
                                          scale: PositiveInt,
                                          compareRule: PrecisionCompareRule = PrecisionCompareRule.Inbound
                                        ) extends Params

  /**
   * Parameters for numberlessThan and numberGreaterThan metrics
   * @param compareValue Value to compare with
   * @param includeBound Boolean flag indicating whether to include (less/greater than or equal to)
   *                     or not (strict less/greater than) compareValue into allowed interval.
   *                     Default: false
   */
  final case class NumberCompareParams(compareValue: Double, includeBound: Boolean = false) extends Params

  /**
   * Parameters for numberBetween and numberNotBetween metrics
   * @param lowerCompareValue Lower bound value to compare with
   * @param upperCompareValue Upper bound value to compare with
   * @param includeBound Boolean flag indicating whether to include (less/greater than or equal to)
   *                     or not (strict less/greater than) bounds into allowed interval.
   *                     Default: false
   */
  final case class NumberIntervalParams(
                                        lowerCompareValue: Double,
                                        upperCompareValue: Double,
                                        includeBound: Boolean = false
                                      ) extends Params

  /**
   * Parameters for TDigest metrics except getQuantile and geqPercentile
   * @param accuracyError Calculation accuracy error in interval [0,1]
   *                      Default: 0.005
   */
  final case class TDigestParams(accuracyError: AccuracyDouble = 0.005) extends Params

  /**
   * Parameters for TDigest getQuantile metric
   * @param accuracyError Calculation accuracy error in interval [0,1]
   *                      Default: 0.005
   * @param target Required quantile to be calculated. Number in interval [0, 1]
   */
  final case class TDigestGeqQuantileParams(
                                             accuracyError: AccuracyDouble = 0.005,
                                             target: PercentileDouble
                                           ) extends Params

  /**
   * Parameters for TDigest getPercentile metric
   * @param accuracyError Calculation accuracy error in interval [0,1]
   *                      Default: 0.005
   * @param target Arbitrary number from set of column values for which the percentile is calculated.
   */
  final case class TDigestGeqPercentileParams(
                                               accuracyError: AccuracyDouble = 0.005,
                                               target: Double
                                             ) extends Params

  /**
   * Parameters for dayDistance metric
   * @param threshold Maximum allowed day distance between dates
   * @param dateFormat Date format of checked columns. Default: "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
   */
  final case class DayDistanceParams(
                                      threshold: PositiveInt,
                                      dateFormat: DateFormat = DateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                                    ) extends Params

  /**
   * Parameters for levenshteinDistance metric
   * @param threshold Maximum allowed levenshtein distance.
   * @param normalize Boolean flag indicating whether levenshtein distance should be normalized.
   *                  If true, then threshold must be in interval [0, 1]. Default: false
   */
  final case class LevenshteinDistanceParams(
                                              threshold: Double,
                                              normalize: Boolean = false
                                            ) extends Params

  /**
   * Parameters for topN metric
   * @param targetNumber Required number of top values for search. Default: 10
   * @param maxCapacity Maximum capacity of container for storing top values. Default: 100
   */
  final case class TopNParams(
                               targetNumber: PositiveInt = 10,
                               maxCapacity: PositiveInt = 100
                             ) extends Params
  
}
