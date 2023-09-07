package ru.raiffeisen.checkita.metrics

import ru.raiffeisen.checkita.metrics.column.AlgebirdMetrics._
import ru.raiffeisen.checkita.metrics.column.BasicNumericMetrics._
import ru.raiffeisen.checkita.metrics.column.BasicStringMetrics._
import ru.raiffeisen.checkita.metrics.column.MultiColumnMetrics._

import scala.language.implicitConversions

object MetricMapper extends Enumeration {

  val distinctValues: MetricDefinition = MetricDefinition("DISTINCT_VALUES", classOf[UniqueValuesMetricCalculator])
  val approxDistinctValues: MetricDefinition = MetricDefinition("APPROXIMATE_DISTINCT_VALUES", classOf[HyperLogLogMetricCalculator])
  val nullValues: MetricDefinition = MetricDefinition("NULL_VALUES", classOf[NullValuesMetricCalculator])
  val completeness: MetricDefinition = MetricDefinition("COMPLETENESS", classOf[CompletenessMetricCalculator])
  val sequenceCompleteness: MetricDefinition = MetricDefinition("SEQUENCE_COMPLETENESS", classOf[SequenceCompletenessMetricCalculator])
  val approxSequenceCompleteness: MetricDefinition = MetricDefinition("APPROXIMATE_SEQUENCE_COMPLETENESS", classOf[HLLSequenceCompletenessMetricCalculator])
  val emptyValues: MetricDefinition = MetricDefinition("EMPTY_VALUES", classOf[EmptyStringValuesMetricCalculator])
  val minNumeric: MetricDefinition = MetricDefinition("MIN_NUMBER", classOf[MinNumericValueMetricCalculator])
  val maxNumeric: MetricDefinition = MetricDefinition("MAX_NUMBER", classOf[MaxNumericValueMetricCalculator])
  val sumOfValues: MetricDefinition = MetricDefinition("SUM_NUMBER", classOf[SumNumericValueMetricCalculator])
  val meanNumeric: MetricDefinition = MetricDefinition("AVG_NUMBER",classOf[StdAvgNumericValueCalculator])
  val stdDeviation: MetricDefinition = MetricDefinition("STD_NUMBER", classOf[StdAvgNumericValueCalculator])
  val minLength: MetricDefinition = MetricDefinition("MIN_STRING", classOf[MinStringValueMetricCalculator])
  val maxLength: MetricDefinition = MetricDefinition("MAX_STRING", classOf[MaxStringValueMetricCalculator])
  val meanLength: MetricDefinition = MetricDefinition("AVG_STRING", classOf[AvgStringValueMetricCalculator])
  val stringLength: MetricDefinition = MetricDefinition("STRING_LENGTH", classOf[StringLengthValuesMetricCalculator])
  val formattedDate: MetricDefinition = MetricDefinition("FORMATTED_DATE", classOf[DateFormattedValuesMetricCalculator])
  val formattedNumeric: MetricDefinition = MetricDefinition("FORMATTED_NUMBER", classOf[NumberFormattedValuesMetricCalculator])
  val castableNumeric: MetricDefinition = MetricDefinition("CASTED_NUMBER", classOf[NumberCastValuesMetricCalculator])
  val inDomainNumeric: MetricDefinition = MetricDefinition("NUMBER_IN_DOMAIN", classOf[NumberInDomainValuesMetricCalculator])
  val outOfDomainNumeric: MetricDefinition = MetricDefinition("NUMBER_OUT_DOMAIN", classOf[NumberOutDomainValuesMetricCalculator])
  val lessThanNumeric: MetricDefinition = MetricDefinition("NUMBER_LESS_THAN", classOf[NumberLessThanMetricCalculator])
  val greaterThanNumeric: MetricDefinition = MetricDefinition("NUMBER_GREATER_THAN", classOf[NumberGreaterThanMetricCalculator])
  val betweenNumeric: MetricDefinition = MetricDefinition("NUMBER_BETWEEN", classOf[NumberBetweenMetricCalculator])
  val notBetweenNumeric: MetricDefinition = MetricDefinition("NUMBER_NOT_BETWEEN", classOf[NumberNotBetweenMetricCalculator])
  val inDomainString: MetricDefinition = MetricDefinition("STRING_IN_DOMAIN", classOf[StringInDomainValuesMetricCalculator])
  val outOfDomainString: MetricDefinition = MetricDefinition("STRING_OUT_DOMAIN", classOf[StringOutDomainValuesMetricCalculator])
  val stringAppearance: MetricDefinition = MetricDefinition("STRING_VALUES", classOf[StringValuesMetricCalculator])
  val regexMatch: MetricDefinition = MetricDefinition("REGEX_MATCH", classOf[RegexMatchMetricCalculator])
  val regexMismatch: MetricDefinition = MetricDefinition("REGEX_MISMATCH", classOf[RegexMismatchMetricCalculator])
  val numberAppearance: MetricDefinition = MetricDefinition("NUMBER_VALUES", classOf[NumberValuesMetricCalculator])
  val medianValue: MetricDefinition = MetricDefinition("MEDIAN_VALUE", classOf[TDigestMetricCalculator])
  val firstQuantileValue: MetricDefinition = MetricDefinition("FIRST_QUANTILE", classOf[TDigestMetricCalculator])
  val thirdQuantileValue: MetricDefinition = MetricDefinition("THIRD_QUANTILE", classOf[TDigestMetricCalculator])
  val specificQuantile: MetricDefinition = MetricDefinition("GET_QUANTILE", classOf[TDigestMetricCalculator])
  val specificPercentile: MetricDefinition = MetricDefinition("GET_PERCENTILE", classOf[TDigestMetricCalculator])
  val topNRating: MetricDefinition = MetricDefinition("TOP_N", classOf[TopKMetricCalculator])
  val columnCompare: MetricDefinition = MetricDefinition("COLUMN_EQ", classOf[EqualStringColumnsMetricCalculator])
  val dayDistanceCompare: MetricDefinition = MetricDefinition("DAY_DISTANCE", classOf[DayDistanceMetric])
  val levenshteinDistanceCompare: MetricDefinition =  MetricDefinition("LEVENSHTEIN_DISTANCE", classOf[LevenshteinDistanceMetric])
  val comomentValue: MetricDefinition = MetricDefinition("CO_MOMENT", classOf[CovarianceMetricCalculator])
  val covarianceBiasedValue: MetricDefinition = MetricDefinition("COVARIANCE", classOf[CovarianceMetricCalculator])
  val covarianceValue: MetricDefinition = MetricDefinition("COVARIANCE_BESSEL", classOf[CovarianceMetricCalculator])

  def getMetricClass(name: String): Class[_ <: MetricCalculator with Product with Serializable] =
    convert(super.withName(name)).calculator

  protected case class MetricDefinition(
                                         name: String,
                                         calculator: Class[_ <: MetricCalculator with Product with Serializable])
    extends super.Val() {
    override def toString(): String = this.name
  }
  implicit def convert(value: Value): MetricDefinition =
    value.asInstanceOf[MetricDefinition]
}

