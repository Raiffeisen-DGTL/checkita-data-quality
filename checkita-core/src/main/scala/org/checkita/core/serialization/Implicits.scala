package org.checkita.core.serialization

import org.checkita.config.Enums.{CompareRule, PrecisionCompareRule, TrendCheckRule}
import org.checkita.config.RefinedTypes.{DateFormat, Email, ID}
import org.checkita.config.jobconf.MetricParams._
import org.checkita.config.jobconf.Metrics._
import org.checkita.core.CalculatorStatus
import org.checkita.core.metrics.ErrorCollection.{AccumulatedErrors, MetricStatus}
import org.checkita.core.metrics.rdd.RDDMetricCalculator
import org.checkita.core.metrics.rdd.regular.AlgebirdRDDMetrics._
import org.checkita.core.metrics.rdd.regular.BasicNumericRDDMetrics._
import org.checkita.core.metrics.rdd.regular.BasicStringRDDMetrics._
import org.checkita.core.metrics.rdd.regular.FileRDDMetrics.RowCountRDDMetricCalculator
import org.checkita.core.metrics.rdd.regular.MultiColumnRDDMetrics._
import org.checkita.core.metrics.{MetricName, RegularMetric}
import org.checkita.core.streaming.Checkpoints._
import org.checkita.core.streaming.ProcessorBuffer
import org.checkita.utils.Common.{camelToSnakeCase, getPrependVars}

object Implicits extends SerDeTransformations 
  with SerializersBasic 
  with SerializersTuples 
  with SerializersCollections 
  with SerializersSpecific {
  
  implicit def checkpointSerDe(implicit kSerDe: SerDe[Int]): SerDe[Checkpoint] = {
    val kindGetter: Checkpoint => Int = (c: Checkpoint) =>
      c.getClass.getSimpleName.dropRight("Checkpoint".length).toLowerCase match {
        case "kafka" => 1
      }

    val serDeGetter: Int => SerDe[Checkpoint] = kindId => {
      val serDe = kindId match {
        case 1 => getProductSerDe(KafkaCheckpoint.unapply, KafkaCheckpoint.tupled)
      }
      serDe.asInstanceOf[SerDe[Checkpoint]]
    }

    kinded(kSerDe, kindGetter, serDeGetter)
  }
  
  implicit def rddCalculatorSerDe(implicit kSerDe: SerDe[String]): SerDe[RDDMetricCalculator] = {
    val kindGetter: RDDMetricCalculator => String = 
      r => camelToSnakeCase(r.getClass.getSimpleName.dropRight("RDDMetricCalculator".length))

    val serDeGetter: String => SerDe[RDDMetricCalculator] = calcName => {
      val serDe = calcName match {
        case "row_count" =>
          getProductSerDe(RowCountRDDMetricCalculator.unapply, RowCountRDDMetricCalculator.tupled)
        case "distinct_values" =>
          getProductSerDe(DistinctValuesRDDMetricCalculator.unapply, DistinctValuesRDDMetricCalculator.tupled)
        case "duplicate_values" =>
          getProductSerDe(DuplicateValuesRDDMetricCalculator.unapply, DuplicateValuesRDDMetricCalculator.tupled)
        case "regex_match" =>
          getProductSerDe(RegexMatchRDDMetricCalculator.unapply, RegexMatchRDDMetricCalculator.tupled)
        case "regex_mismatch" =>
          getProductSerDe(RegexMismatchRDDMetricCalculator.unapply, RegexMismatchRDDMetricCalculator.tupled)
        case "null_values" =>
          getProductSerDe(NullValuesRDDMetricCalculator.unapply, NullValuesRDDMetricCalculator.tupled)
        case "completeness" =>
          getProductSerDe(CompletenessRDDMetricCalculator.unapply, CompletenessRDDMetricCalculator.tupled)
        case "emptiness" =>
          getProductSerDe(EmptinessRDDMetricCalculator.unapply, EmptinessRDDMetricCalculator.tupled)
        case "empty_values" =>
          getProductSerDe(EmptyValuesRDDMetricCalculator.unapply, EmptyValuesRDDMetricCalculator.tupled)
        case "min_string" =>
          getProductSerDe(MinStringRDDMetricCalculator.unapply, MinStringRDDMetricCalculator.tupled)
        case "max_string" =>
          getProductSerDe(MaxStringRDDMetricCalculator.unapply, MaxStringRDDMetricCalculator.tupled)
        case "avg_string" =>
          getProductSerDe(AvgStringRDDMetricCalculator.unapply, AvgStringRDDMetricCalculator.tupled)
        case "formatted_date" =>
          getProductSerDe(FormattedDateRDDMetricCalculator.unapply, FormattedDateRDDMetricCalculator.tupled)
        case "string_length" =>
          getProductSerDe(StringLengthRDDMetricCalculator.unapply, StringLengthRDDMetricCalculator.tupled)
        case "string_in_domain" =>
          getProductSerDe(StringInDomainRDDMetricCalculator.unapply, StringInDomainRDDMetricCalculator.tupled)
        case "string_out_domain" =>
          getProductSerDe(StringOutDomainRDDMetricCalculator.unapply, StringOutDomainRDDMetricCalculator.tupled)
        case "string_values" =>
          getProductSerDe(StringValuesRDDMetricCalculator.unapply, StringValuesRDDMetricCalculator.tupled)
        case "t_digest" =>
          getProductSerDe(TDigestRDDMetricCalculator.unapply, TDigestRDDMetricCalculator.tupled)
        case "min_number" =>
          getProductSerDe(MinNumberRDDMetricCalculator.unapply, MinNumberRDDMetricCalculator.tupled)
        case "max_number" =>
          getProductSerDe(MaxNumberRDDMetricCalculator.unapply, MaxNumberRDDMetricCalculator.tupled)
        case "sum_number" =>
          getProductSerDe(SumNumberRDDMetricCalculator.unapply, SumNumberRDDMetricCalculator.tupled)
        case "std_avg_number" =>
          getProductSerDe(StdAvgNumberRDDMetricCalculator.unapply, StdAvgNumberRDDMetricCalculator.tupled)
        case "formatted_number" =>
          getProductSerDe(FormattedNumberRDDMetricCalculator.unapply, FormattedNumberRDDMetricCalculator.tupled)
        case "casted_number" =>
          getProductSerDe(CastedNumberRDDMetricCalculator.unapply, CastedNumberRDDMetricCalculator.tupled)
        case "number_in_domain" =>
          getProductSerDe(NumberInDomainRDDMetricCalculator.unapply, NumberInDomainRDDMetricCalculator.tupled)
        case "number_out_domain" =>
          getProductSerDe(NumberOutDomainRDDMetricCalculator.unapply, NumberOutDomainRDDMetricCalculator.tupled)
        case "number_values" =>
          getProductSerDe(NumberValuesRDDMetricCalculator.unapply, NumberValuesRDDMetricCalculator.tupled)
        case "number_less_than" =>
          getProductSerDe(NumberLessThanRDDMetricCalculator.unapply, NumberLessThanRDDMetricCalculator.tupled)
        case "number_greater_than" =>
          getProductSerDe(NumberGreaterThanRDDMetricCalculator.unapply, NumberGreaterThanRDDMetricCalculator.tupled)
        case "number_between" =>
          getProductSerDe(NumberBetweenRDDMetricCalculator.unapply, NumberBetweenRDDMetricCalculator.tupled)
        case "number_not_between" =>
          getProductSerDe(NumberNotBetweenRDDMetricCalculator.unapply, NumberNotBetweenRDDMetricCalculator.tupled)
        case "sequence_completeness" =>
          getProductSerDe(SequenceCompletenessRDDMetricCalculator.unapply, SequenceCompletenessRDDMetricCalculator.tupled)
        case "hyper_log_log" =>
          getProductSerDe(HyperLogLogRDDMetricCalculator.unapply, HyperLogLogRDDMetricCalculator.tupled)
        case "h_l_l_sequence_completeness" =>
          getProductSerDe(HLLSequenceCompletenessRDDMetricCalculator.unapply, HLLSequenceCompletenessRDDMetricCalculator.tupled)
        case "top_k" =>
          getProductSerDe(TopKRDDMetricCalculator.unapply, TopKRDDMetricCalculator.tupled)
        case "covariance" =>
          getProductSerDe(CovarianceRDDMetricCalculator.unapply, CovarianceRDDMetricCalculator.tupled)
        case "column_eq" =>
          getProductSerDe(ColumnEqRDDMetricCalculator.unapply, ColumnEqRDDMetricCalculator.tupled)
        case "day_distance" =>
          getProductSerDe(DayDistanceRDDMetricCalculator.unapply, DayDistanceRDDMetricCalculator.tupled)
        case "levenshtein_distance" =>
          getProductSerDe(LevenshteinDistanceRDDMetricCalculator.unapply, LevenshteinDistanceRDDMetricCalculator.tupled)
      }
      serDe.asInstanceOf[SerDe[RDDMetricCalculator]]
    }
    kinded(kSerDe, kindGetter, serDeGetter)
  }
  
  // SerDe's for enumerations:
  implicit val calculatorStatusSerDe: SerDe[CalculatorStatus] = getEnumSerDe(CalculatorStatus)
  implicit val metricNameSerDe: SerDe[MetricName] = getEnumSerDe(MetricName)
  implicit val compareRuleSerDe: SerDe[CompareRule] = getEnumSerDe(CompareRule)
  implicit val precisionCompareSerDe: SerDe[PrecisionCompareRule] = getEnumSerDe(PrecisionCompareRule)
  implicit val trendCheckRuleSerDer: SerDe[TrendCheckRule] = getEnumSerDe(TrendCheckRule) 
  
  // SerDe's for error collection classes:
  implicit val metricStatusSerDe: SerDe[MetricStatus] =
    getProductSerDe(MetricStatus.unapply, MetricStatus.tupled)
  implicit val accumulatedErrorsSerDe: SerDe[AccumulatedErrors] =
    getProductSerDe(AccumulatedErrors.unapply, AccumulatedErrors.tupled)
  
  // SerDe's for specific classes used within configuration classes:
  implicit val idSerDe: SerDe[ID] = getProductSerDe(ID.unapply, ID)
  implicit val emailSerde: SerDe[Email] = getProductSerDe(Email.unapply, Email)
  implicit val dateFormatSerDe: SerDe[DateFormat] = getProductSerDe(
    dt => Some(dt.pattern), fmt => DateFormat(fmt)
  )

  // SerDe's for metric parameters:
  implicit val approxDistinctParamSerDe: SerDe[ApproxDistinctValuesParams] = getProductSerDe(
    p => Some(p.accuracyError), a => ApproxDistinctValuesParams(a)
  )
  implicit val completenessParamSerDe: SerDe[CompletenessParams] = getProductSerDe(
    p => Some(p.includeEmptyStrings), ies => CompletenessParams(ies)
  )
  implicit val sequenceCompletenessParamSerDe: SerDe[SequenceCompletenessParams] = getProductSerDe(
    p => Some(p.increment), inc => SequenceCompletenessParams(inc)
  )
  implicit val approxSequenceCompletenessParamSerDe: SerDe[ApproxSequenceCompletenessParams] = getProductSerDe(
    ApproxSequenceCompletenessParams.unapply, ApproxSequenceCompletenessParams.tupled
  )
  implicit val stringLengthParamSerDe: SerDe[StringLengthParams] = getProductSerDe(
    StringLengthParams.unapply, StringLengthParams.tupled
  )
  implicit val stringDomainParamSerDe: SerDe[StringDomainParams] = getProductSerDe(
    p => Some(p.domain), d => StringDomainParams(d)
  )
  implicit val numberDomainParamSerDe: SerDe[NumberDomainParams] = getProductSerDe(
    p => Some(p.domain), d => NumberDomainParams(d)
  )
  implicit val stringValuesParamSerDe: SerDe[StringValuesParams] = getProductSerDe(
    p => Some(p.compareValue), cv => StringValuesParams(cv)
  )
  implicit val numberValuesParamSerDe: SerDe[NumberValuesParams] = getProductSerDe(
    p => Some(p.compareValue), cv => NumberValuesParams(cv)
  )
  implicit val RegexParamSerDe: SerDe[RegexParams] = getProductSerDe(
    p => Some(p.regex), rgx => RegexParams(rgx)
  )
  implicit val formattedDateParamSerDe: SerDe[FormattedDateParams] = getProductSerDe(
    p => Some(p.dateFormat), df => FormattedDateParams(df)
  )
  implicit val formattedNumberParamSerDe: SerDe[FormattedNumberParams] = getProductSerDe(
    FormattedNumberParams.unapply, FormattedNumberParams.tupled
  )
  implicit val numberCompareParamSerDe: SerDe[NumberCompareParams] = getProductSerDe(
    NumberCompareParams.unapply, NumberCompareParams.tupled
  )
  implicit val numberIntervalParamSerDe: SerDe[NumberIntervalParams] = getProductSerDe(
    NumberIntervalParams.unapply, NumberIntervalParams.tupled
  )
  implicit val tDigestParamSerDe: SerDe[TDigestParams] = getProductSerDe(
    p => Some(p.accuracyError), a => TDigestParams(a)
  )
  implicit val tDigestGeqQuantileParamSerDe: SerDe[TDigestGeqQuantileParams] = getProductSerDe(
    TDigestGeqQuantileParams.unapply, TDigestGeqQuantileParams.tupled
  )
  implicit val tDigestGeqPercentileParamSerDe: SerDe[TDigestGeqPercentileParams] = getProductSerDe(
    TDigestGeqPercentileParams.unapply, TDigestGeqPercentileParams.tupled
  )
  implicit val dayDistanceParamSerDe: SerDe[DayDistanceParams] = getProductSerDe(
    DayDistanceParams.unapply, DayDistanceParams.tupled
  )
  implicit val levenshteinDistanceParamSerDe: SerDe[LevenshteinDistanceParams] = getProductSerDe(
    LevenshteinDistanceParams.unapply, LevenshteinDistanceParams.tupled
  )
  implicit val topNParamSerDe: SerDe[TopNParams] = getProductSerDe(
    TopNParams.unapply, TopNParams.tupled
  )
  
  implicit def regularMetricSerDe(implicit mSerDe: SerDe[MetricName]): SerDe[RegularMetric] = {
    val getter: MetricName => SerDe[RegularMetric] = mn => {
      val serDe = mn match {
        case MetricName.RowCount =>
          getProductSerDe(RowCountMetricConfig.unapply, RowCountMetricConfig.tupled)
        case MetricName.DistinctValues =>
          getProductSerDe(DistinctValuesMetricConfig.unapply, DistinctValuesMetricConfig.tupled)
        case MetricName.ApproximateDistinctValues =>
          getProductSerDe(ApproxDistinctValuesMetricConfig.unapply, ApproxDistinctValuesMetricConfig.tupled)
        case MetricName.NullValues =>
          getProductSerDe(NullValuesMetricConfig.unapply, NullValuesMetricConfig.tupled)
        case MetricName.EmptyValues =>
          getProductSerDe(EmptyValuesMetricConfig.unapply, EmptyValuesMetricConfig.tupled)
        case MetricName.DuplicateValues =>
          getProductSerDe(DuplicateValuesMetricConfig.unapply, DuplicateValuesMetricConfig.tupled)
        case MetricName.Completeness =>
          getProductSerDe(CompletenessMetricConfig.unapply, CompletenessMetricConfig.tupled)
        case MetricName.Emptiness =>
          getProductSerDe(EmptinessMetricConfig.unapply, EmptinessMetricConfig.tupled)
        case MetricName.SequenceCompleteness =>
          getProductSerDe(SequenceCompletenessMetricConfig.unapply, SequenceCompletenessMetricConfig.tupled)
        case MetricName.ApproximateSequenceCompleteness =>
          getProductSerDe(ApproxSequenceCompletenessMetricConfig.unapply, ApproxSequenceCompletenessMetricConfig.tupled)
        case MetricName.MinString =>
          getProductSerDe(MinStringMetricConfig.unapply, MinStringMetricConfig.tupled)
        case MetricName.MaxString =>
          getProductSerDe(MaxStringMetricConfig.unapply, MaxStringMetricConfig.tupled)
        case MetricName.AvgString =>
          getProductSerDe(AvgStringMetricConfig.unapply, AvgStringMetricConfig.tupled)
        case MetricName.StringLength =>
          getProductSerDe(StringLengthMetricConfig.unapply, StringLengthMetricConfig.tupled)
        case MetricName.StringInDomain =>
          getProductSerDe(StringInDomainMetricConfig.unapply, StringInDomainMetricConfig.tupled)
        case MetricName.StringOutDomain =>
          getProductSerDe(StringOutDomainMetricConfig.unapply, StringOutDomainMetricConfig.tupled)
        case MetricName.StringValues =>
          getProductSerDe(StringValuesMetricConfig.unapply, StringValuesMetricConfig.tupled)
        case MetricName.RegexMatch =>
          getProductSerDe(RegexMatchMetricConfig.unapply, RegexMatchMetricConfig.tupled)
        case MetricName.RegexMismatch =>
          getProductSerDe(RegexMismatchMetricConfig.unapply, RegexMismatchMetricConfig.tupled)
        case MetricName.FormattedDate =>
          getProductSerDe(FormattedDateMetricConfig.unapply, FormattedDateMetricConfig.tupled)
        case MetricName.FormattedNumber =>
          getProductSerDe(FormattedNumberMetricConfig.unapply, FormattedNumberMetricConfig.tupled)
        case MetricName.MinNumber =>
          getProductSerDe(MinNumberMetricConfig.unapply, MinNumberMetricConfig.tupled)
        case MetricName.MaxNumber =>
          getProductSerDe(MaxNumberMetricConfig.unapply, MaxNumberMetricConfig.tupled)
        case MetricName.SumNumber =>
          getProductSerDe(SumNumberMetricConfig.unapply, SumNumberMetricConfig.tupled)
        case MetricName.AvgNumber =>
          getProductSerDe(AvgNumberMetricConfig.unapply, AvgNumberMetricConfig.tupled)
        case MetricName.StdNumber =>
          getProductSerDe(StdNumberMetricConfig.unapply, StdNumberMetricConfig.tupled)
        case MetricName.CastedNumber =>
          getProductSerDe(CastedNumberMetricConfig.unapply, CastedNumberMetricConfig.tupled)
        case MetricName.NumberInDomain =>
          getProductSerDe(NumberInDomainMetricConfig.unapply, NumberInDomainMetricConfig.tupled)
        case MetricName.NumberOutDomain =>
          getProductSerDe(NumberOutDomainMetricConfig.unapply, NumberOutDomainMetricConfig.tupled)
        case MetricName.NumberLessThan =>
          getProductSerDe(NumberLessThanMetricConfig.unapply, NumberLessThanMetricConfig.tupled)
        case MetricName.NumberGreaterThan =>
          getProductSerDe(NumberGreaterThanMetricConfig.unapply, NumberGreaterThanMetricConfig.tupled)
        case MetricName.NumberBetween =>
          getProductSerDe(NumberBetweenMetricConfig.unapply, NumberBetweenMetricConfig.tupled)
        case MetricName.NumberNotBetween =>
          getProductSerDe(NumberNotBetweenMetricConfig.unapply, NumberNotBetweenMetricConfig.tupled)
        case MetricName.NumberValues =>
          getProductSerDe(NumberValuesMetricConfig.unapply, NumberValuesMetricConfig.tupled)
        case MetricName.MedianValue =>
          getProductSerDe(MedianValueMetricConfig.unapply, MedianValueMetricConfig.tupled)
        case MetricName.FirstQuantile =>
          getProductSerDe(FirstQuantileMetricConfig.unapply, FirstQuantileMetricConfig.tupled)
        case MetricName.ThirdQuantile =>
          getProductSerDe(ThirdQuantileMetricConfig.unapply, ThirdQuantileMetricConfig.tupled)
        case MetricName.GetPercentile =>
          getProductSerDe(GetPercentileMetricConfig.unapply, GetPercentileMetricConfig.tupled)
        case MetricName.GetQuantile =>
          getProductSerDe(GetQuantileMetricConfig.unapply, GetQuantileMetricConfig.tupled)
        case MetricName.ColumnEq =>
          getProductSerDe(ColumnEqMetricConfig.unapply, ColumnEqMetricConfig.tupled)
        case MetricName.DayDistance =>
          getProductSerDe(DayDistanceMetricConfig.unapply, DayDistanceMetricConfig.tupled)
        case MetricName.LevenshteinDistance =>
          getProductSerDe(LevenshteinDistanceMetricConfig.unapply, LevenshteinDistanceMetricConfig.tupled)
        case MetricName.CoMoment =>
          getProductSerDe(CoMomentMetricConfig.unapply, CoMomentMetricConfig.tupled)
        case MetricName.Covariance =>
          getProductSerDe(CovarianceMetricConfig.unapply, CovarianceMetricConfig.tupled)
        case MetricName.CovarianceBessel =>
          getProductSerDe(CovarianceBesselMetricConfig.unapply, CovarianceBesselMetricConfig.tupled)
        case MetricName.TopN =>
          getProductSerDe(TopNMetricConfig.unapply, TopNMetricConfig.tupled)
        case MetricName.Composed =>
          getProductSerDe(ComposedMetricConfig.unapply, ComposedMetricConfig.tupled)
        case MetricName.TrendAvg =>
          getProductSerDe(AvgTrendMetricConfig.unapply, AvgTrendMetricConfig.tupled)
        case MetricName.TrendStd =>
          getProductSerDe(StdTrendMetricConfig.unapply, StdTrendMetricConfig.tupled)
        case MetricName.TrendMin =>
          getProductSerDe(MinTrendMetricConfig.unapply, MinTrendMetricConfig.tupled)
        case MetricName.TrendMax =>
          getProductSerDe(MaxTrendMetricConfig.unapply, MaxTrendMetricConfig.tupled)
        case MetricName.TrendSum => 
          getProductSerDe(SumTrendMetricConfig.unapply, SumTrendMetricConfig.tupled)
        case MetricName.TrendMedian =>
          getProductSerDe(MedianTrendMetricConfig.unapply, MedianTrendMetricConfig.tupled)
        case MetricName.TrendFirstQ =>
          getProductSerDe(FirstQuartileTrendMetricConfig.unapply, FirstQuartileTrendMetricConfig.tupled)
        case MetricName.TrendThirdQ =>
          getProductSerDe(ThirdQuartileTrendMetricConfig.unapply, ThirdQuartileTrendMetricConfig.tupled)
        case MetricName.TrendQuantile =>
          getProductSerDe(QuantileTrendMetricConfig.unapply, QuantileTrendMetricConfig.tupled)
      }
      serDe.asInstanceOf[SerDe[RegularMetric]]
    }
    kinded(mSerDe, (r: RegularMetric) => r.metricName, getter)
  }
  
  /**
   * Implicit SerDe for Streaming Processor Buffer.
   * 
   * This is top-level SerDe which will be used to serialize/deserialize
   * streaming application processor buffer state during checkpoint operations.
   */
  implicit val processorBufferSerDe: SerDe[ProcessorBuffer] =
    getProductSerDe(ProcessorBuffer.unapply, ProcessorBuffer.fromTuple)
}
