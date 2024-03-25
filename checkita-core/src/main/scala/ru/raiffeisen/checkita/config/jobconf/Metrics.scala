package ru.raiffeisen.checkita.config.jobconf

import eu.timepit.refined.types.string.NonEmptyString
import org.json4s.jackson.Serialization.write
import ru.raiffeisen.checkita.config.RefinedTypes._
import ru.raiffeisen.checkita.config.jobconf.MetricParams._
import ru.raiffeisen.checkita.core.dfmetrics.BasicStringMetrics._
import ru.raiffeisen.checkita.core.dfmetrics.{DFMetricCalculator, DFRegularMetric}
import ru.raiffeisen.checkita.core.metrics.regular.AlgebirdMetrics._
import ru.raiffeisen.checkita.core.metrics.regular.BasicNumericMetrics._
import ru.raiffeisen.checkita.core.metrics.regular.BasicStringMetrics._
import ru.raiffeisen.checkita.core.metrics.regular.FileMetrics._
import ru.raiffeisen.checkita.core.metrics.regular.MultiColumnMetrics._
import ru.raiffeisen.checkita.core.metrics._
import ru.raiffeisen.checkita.utils.Common.{getFieldsMap, jsonFormats}


object Metrics {

  /**
   * Base class for all metrics configurations. All metrics are described as DQ entities.
   */
  sealed abstract class MetricConfig extends JobConfigEntity {
    val metricId: String = id.value // actual ID value after validation.
  }

  /**
   * Base class for all regular metric configurations (except row count metric). All regular metrics must have a
   * reference to source ID over which the metric is being calculated. In addition, column metrics must have non-empty
   * sequence of columns over which the metric is being calculated.
   * Metric error collection logic might be reversed provided with corresponding boolean flag set to `true`.
   */
  sealed abstract class RegularMetricConfig extends MetricConfig with RegularMetric {
    val source: NonEmptyString

    // actual source ID after validation
    val metricSource: String = source.value

    // additional validation will be imposed on the allowed number
    // of columns depending on metric type.
    val metricColumns: Seq[String]

    val paramString: Option[String]
  }

  /** Base class for column metrics that works with any number of columns.
    */
  sealed abstract class AnyColumnRegularMetricConfig extends RegularMetricConfig {
    val columns: NonEmptyStringSeq
    val metricColumns: Seq[String] = columns.value
  }

  /** Base class for column metrics that works only with single column.
    */
  sealed abstract class SingleColumnRegularMetricConfig extends RegularMetricConfig {
    val columns: SingleElemStringSeq
    val metricColumns: Seq[String] = columns.value
  }

  /** Base class for column metrics that works only with two columns.
    */
  sealed abstract class DoubleColumnRegularMetricConfig extends RegularMetricConfig {
    val columns: DoubleElemStringSeq
    val metricColumns: Seq[String] = columns.value
  }

  /** Base class for column metrics that works with at least two columns.
    */
  sealed abstract class MultiColumnRegularMetricConfig extends RegularMetricConfig {
    val columns: MultiElemStringSeq
    val metricColumns: Seq[String] = columns.value
  }

  /** Composed metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param formula     Formula to calculate composed metric
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class ComposedMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      formula: NonEmptyString,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends MetricConfig
      with ComposedMetric {
    // actual metric formula after validation:
    val metricFormula: String = formula.value
  }

  /** Row count metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class RowCountMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends RegularMetricConfig {
    val metricColumns: Seq[String]             = Seq.empty[String]
    val metricName: MetricName                 = MetricName.RowCount
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new RowCountMetricCalculator()
  }

  /** Duplicate values column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class DuplicateValuesMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends AnyColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.DuplicateValues
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new DuplicateValuesMetricCalculator()
  }

  /** Distinct values column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class DistinctValuesMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends AnyColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.DistinctValues
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new DistinctValuesMetricCalculator()
  }

  /** Approximate distinct values column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class ApproxDistinctValuesMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      params: ApproxDistinctValuesParams = ApproxDistinctValuesParams(),
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName      = MetricName.ApproximateDistinctValues
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: MetricCalculator =
      new HyperLogLogMetricCalculator(params.accuracyError.value)
  }

  /** Null values column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   *                    When reversed is set to `true` then any non-null values are considered as metric failure.
   *                    Otherwise, null values are collected as metric failure.
   */
  final case class NullValuesMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = true
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName                 = MetricName.NullValues
    val paramString: Option[String]            = None
    def initMetricCalculator: ReversibleMetricCalculator = new NullValuesMetricCalculator(reversed)
  }

  /** Empty values column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class EmptyValuesMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = true
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName                 = MetricName.EmptyValues
    val paramString: Option[String]            = None
    def initMetricCalculator: ReversibleMetricCalculator = new EmptyStringValuesMetricCalculator(reversed)
  }

  /** Completeness column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class CompletenessMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: CompletenessParams = CompletenessParams(),
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = true
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.Completeness
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new CompletenessMetricCalculator(params.includeEmptyStrings, reversed)
  }

  /** Sequence completeness column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class SequenceCompletenessMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      params: SequenceCompletenessParams = SequenceCompletenessParams(),
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName      = MetricName.SequenceCompleteness
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: MetricCalculator =
      new SequenceCompletenessMetricCalculator(params.increment.value)
  }

  /** Sequence completeness column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class ApproxSequenceCompletenessMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      params: ApproxSequenceCompletenessParams = ApproxSequenceCompletenessParams(),
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName      = MetricName.ApproximateSequenceCompleteness
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: MetricCalculator =
      new HLLSequenceCompletenessMetricCalculator(params.accuracyError.value, params.increment.value)
  }

  /** Min string column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class MinStringMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends AnyColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.MinString
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new MinStringValueMetricCalculator()
  }

  /** Max string column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class MaxStringMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends AnyColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.MaxString
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new MaxStringValueMetricCalculator()
  }

  /** Average string column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class AvgStringMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends AnyColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.AvgString
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new AvgStringValueMetricCalculator()
  }

  /** String length column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class StringLengthMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: StringLengthParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.StringLength
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new StringLengthValuesMetricCalculator(params.length.value, params.compareRule.toString.toLowerCase, reversed)
  }

  /** String in domain column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class StringInDomainMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: StringDomainParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.StringInDomain
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new StringInDomainValuesMetricCalculator(params.domain.value.toSet, reversed)
  }

  /** String out domain column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class StringOutDomainMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: StringDomainParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.StringOutDomain
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new StringOutDomainValuesMetricCalculator(params.domain.value.toSet, reversed)
  }

  /** String values column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class StringValuesMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: StringValuesParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.StringValues
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new StringValuesMetricCalculator(params.compareValue.value, reversed)
  }

  /** Regex match column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class RegexMatchMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: RegexParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName                 = MetricName.RegexMatch
    val paramString: Option[String]            = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new RegexMatchMetricCalculator(params.regex.value, reversed)
  }

  /** Regex mismatch column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class RegexMismatchMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: RegexParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName                 = MetricName.RegexMismatch
    val paramString: Option[String]            = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new RegexMismatchMetricCalculator(params.regex.value, reversed)
  }

  /** Formatted date column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class FormattedDateMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: FormattedDateParams = FormattedDateParams(),
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.FormattedDate
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new DateFormattedValuesMetricCalculator(params.dateFormat.pattern, reversed)
  }

  /** Formatted number column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class FormattedNumberMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: FormattedNumberParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.FormattedNumber
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator = new NumberFormattedValuesMetricCalculator(
      params.precision.value,
      params.scale.value,
      params.compareRule.toString.toLowerCase,
      reversed
    )
  }

  /** Min number column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class MinNumberMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends AnyColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.MinNumber
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new MinNumericValueMetricCalculator()
  }

  /** Max number column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class MaxNumberMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends AnyColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.MaxNumber
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new MaxNumericValueMetricCalculator()
  }

  /** Sum number column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class SumNumberMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends AnyColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.SumNumber
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new SumNumericValueMetricCalculator()
  }

  /** Average number column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class AvgNumberMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.AvgNumber
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new StdAvgNumericValueCalculator()
  }

  /** Standard deviation number column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class StdNumberMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.StdNumber
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new StdAvgNumericValueCalculator()
  }

  /** Casted number column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class CastedNumberMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName                 = MetricName.CastedNumber
    val paramString: Option[String]            = None
    def initMetricCalculator: ReversibleMetricCalculator =
      new NumberCastValuesMetricCalculator(reversed)
  }

  /** Number in domain column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class NumberInDomainMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: NumberDomainParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.NumberInDomain
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new NumberInDomainValuesMetricCalculator(params.domain.value.toSet, reversed)
  }

  /** Number out domain column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class NumberOutDomainMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: NumberDomainParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.NumberOutDomain
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new NumberOutDomainValuesMetricCalculator(params.domain.value.toSet, reversed)
  }

  /** Number less than column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class NumberLessThanMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: NumberCompareParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.NumberLessThan
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new NumberLessThanMetricCalculator(params.compareValue, params.includeBound, reversed)
  }

  /** Number greater than column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class NumberGreaterThanMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: NumberCompareParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.NumberGreaterThan
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new NumberGreaterThanMetricCalculator(params.compareValue, params.includeBound, reversed)
  }

  /** Number between column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class NumberBetweenMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: NumberIntervalParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.NumberBetween
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator = new NumberBetweenMetricCalculator(
      params.lowerCompareValue, params.upperCompareValue, params.includeBound, reversed
    )
  }

  /** Number not between column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class NumberNotBetweenMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: NumberIntervalParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.NumberNotBetween
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator = new NumberNotBetweenMetricCalculator(
      params.lowerCompareValue, params.upperCompareValue, params.includeBound, reversed
    )
  }

  /** Number values column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class NumberValuesMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: NonEmptyStringSeq,
      params: NumberValuesParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.NumberValues
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new NumberValuesMetricCalculator(params.compareValue, reversed)
  }

  /** TDigest Median value column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class MedianValueMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      params: TDigestParams = TDigestParams(),
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName      = MetricName.MedianValue
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: MetricCalculator =
      new TDigestMetricCalculator(params.accuracyError.value, 0)
  }

  /** TDigest First quantile column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class FirstQuantileMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      params: TDigestParams = TDigestParams(),
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName      = MetricName.FirstQuantile
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: MetricCalculator =
      new TDigestMetricCalculator(params.accuracyError.value, 0)
  }

  /** TDigest Third quantile column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class ThirdQuantileMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      params: TDigestParams = TDigestParams(),
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName      = MetricName.ThirdQuantile
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: MetricCalculator =
      new TDigestMetricCalculator(params.accuracyError.value, 0)
  }

  /** TDigest Get quantile column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class GetQuantileMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      params: TDigestGeqQuantileParams,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName      = MetricName.GetQuantile
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: MetricCalculator =
      new TDigestMetricCalculator(params.accuracyError.value, params.target.value)
  }

  /** TDigest Get percentile column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class GetPercentileMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      params: TDigestGeqPercentileParams,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName      = MetricName.GetPercentile
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: MetricCalculator =
      new TDigestMetricCalculator(params.accuracyError.value, params.target)
  }

  /** Column equality metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class ColumnEqMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: MultiElemStringSeq,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends MultiColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName                 = MetricName.ColumnEq
    val paramString: Option[String]            = None
    def initMetricCalculator: ReversibleMetricCalculator = new EqualStringColumnsMetricCalculator(reversed)
  }

  /** Day distance column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class DayDistanceMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: DoubleElemStringSeq,
      params: DayDistanceParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends DoubleColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.DayDistance
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new DayDistanceMetricCalculator(params.dateFormat.pattern, params.threshold.value, reversed)
  }

  /** Levenshtein distance column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class LevenshteinDistanceMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: DoubleElemStringSeq,
      params: LevenshteinDistanceParams,
      metadata: Seq[SparkParam] = Seq.empty,
      reversed: Boolean = false
  ) extends DoubleColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.LevenshteinDistance
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: ReversibleMetricCalculator =
      new LevenshteinDistanceMetricCalculator(params.threshold, params.normalize, reversed)
  }

  /** Co-moment column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class CoMomentMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: DoubleElemStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends DoubleColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.CoMoment
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new CovarianceMetricCalculator()
  }

  /** Covariance column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class CovarianceMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: DoubleElemStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends DoubleColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.Covariance
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new CovarianceMetricCalculator()
  }

  /** Covariance Bessel column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class CovarianceBesselMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: DoubleElemStringSeq,
      metadata: Seq[SparkParam] = Seq.empty
  ) extends DoubleColumnRegularMetricConfig {
    val metricName: MetricName                 = MetricName.CovarianceBessel
    val paramString: Option[String]            = None
    def initMetricCalculator: MetricCalculator = new CovarianceMetricCalculator()
  }

  /** TopN column metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   */
  final case class TopNMetricConfig(
      id: ID,
      description: Option[NonEmptyString],
      source: NonEmptyString,
      columns: SingleElemStringSeq,
      params: TopNParams = TopNParams(),
      metadata: Seq[SparkParam] = Seq.empty
  ) extends SingleColumnRegularMetricConfig {
    val metricName: MetricName      = MetricName.TopN
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initMetricCalculator: MetricCalculator =
      new TopKMetricCalculator(params.maxCapacity.value, params.targetNumber.value)
  }

  /** Data Quality job configuration section describing column metrics
    *
    * @param rowCount
    *   Sequence of rowCount metrics
    * @param distinctValues
    *   Sequence of distinctValues metrics
    * @param approximateDistinctValues
    *   Sequence of approximateDistinctValues metrics
    * @param nullValues
    *   Sequence of nullValues metrics
    * @param emptyValues
    *   Sequence of emptyValues metrics
    * @param duplicateValues
    *   Sequence of duplicateValues metrics
    * @param completeness
    *   Sequence of completeness metrics
    * @param minString
    *   Sequence of minString metrics
    * @param maxString
    *   Sequence of maxString metrics
    * @param avgString
    *   Sequence of avgString metrics
    * @param stringLength
    *   Sequence of stringLength metrics
    * @param stringInDomain
    *   Sequence of stringInDomain metrics
    * @param stringOutDomain
    *   Sequence of stringOutDomain metrics
    * @param stringValues
    *   Sequence of stringValues metrics
    * @param regexMatch
    *   Sequence of regexMatch metrics
    * @param regexMismatch
    *   Sequence of regexMismatch metrics
    * @param formattedDate
    *   Sequence of formattedDate metrics
    * @param formattedNumber
    *   Sequence of formattedNumber metrics
    * @param minNumber
    *   Sequence of minNumber metrics
    * @param maxNumber
    *   Sequence of maxNumber metrics
    * @param sumNumber
    *   Sequence of sumNumber metrics
    * @param avgNumber
    *   Sequence of avgNumber metrics
    * @param stdNumber
    *   Sequence of stdNumber metrics
    * @param castedNumber
    *   Sequence of castedNumber metrics
    * @param numberInDomain
    *   Sequence of numberInDomain metrics
    * @param numberOutDomain
    *   Sequence of numberOutDomain metrics
    * @param numberLessThan
    *   Sequence of numberLessThan metrics
    * @param numberGreaterThan
    *   Sequence of numberGreaterThan metrics
    * @param numberBetween
    *   Sequence of numberBetween metrics
    * @param numberNotBetween
    *   Sequence of numberNotBetween metrics
    * @param numberValues
    *   Sequence of numberValues metrics
    * @param medianValue
    *   Sequence of medianValue metrics
    * @param firstQuantile
    *   Sequence of firstQuantile metrics
    * @param thirdQuantile
    *   Sequence of thirdQuantile metrics
    * @param getPercentile
    *   Sequence of getPercentile metrics
    * @param getQuantile
    *   Sequence of getQuantile metrics
    * @param columnEq
    *   Sequence of columnEq metrics
    * @param dayDistance
    *   Sequence of dayDistance metrics
    * @param levenshteinDistance
    *   Sequence of levenshteinDistance metrics
    * @param coMoment
    *   Sequence of coMoment metrics
    * @param covariance
    *   Sequence of covariance metrics
    * @param covarianceBessel
    *   Sequence of covarianceBessel metrics
    * @param topN
    *   Sequence of topN metrics
    */
  final case class RegularMetricsConfig(
      rowCount: Seq[RowCountMetricConfig] = Seq.empty,
      distinctValues: Seq[DistinctValuesMetricConfig] = Seq.empty,
      approximateDistinctValues: Seq[ApproxDistinctValuesMetricConfig] = Seq.empty,
      nullValues: Seq[NullValuesMetricConfig] = Seq.empty,
      emptyValues: Seq[EmptyValuesMetricConfig] = Seq.empty,
      duplicateValues: Seq[DuplicateValuesMetricConfig] = Seq.empty,
      completeness: Seq[CompletenessMetricConfig] = Seq.empty,
      sequenceCompleteness: Seq[SequenceCompletenessMetricConfig] = Seq.empty,
      approximateSequenceCompleteness: Seq[ApproxSequenceCompletenessMetricConfig] = Seq.empty,
      minString: Seq[MinStringMetricConfig] = Seq.empty,
      maxString: Seq[MaxStringMetricConfig] = Seq.empty,
      avgString: Seq[AvgStringMetricConfig] = Seq.empty,
      stringLength: Seq[StringLengthMetricConfig] = Seq.empty,
      stringInDomain: Seq[StringInDomainMetricConfig] = Seq.empty,
      stringOutDomain: Seq[StringOutDomainMetricConfig] = Seq.empty,
      stringValues: Seq[StringValuesMetricConfig] = Seq.empty,
      regexMatch: Seq[RegexMatchMetricConfig] = Seq.empty,
      regexMismatch: Seq[RegexMismatchMetricConfig] = Seq.empty,
      formattedDate: Seq[FormattedDateMetricConfig] = Seq.empty,
      formattedNumber: Seq[FormattedNumberMetricConfig] = Seq.empty,
      minNumber: Seq[MinNumberMetricConfig] = Seq.empty,
      maxNumber: Seq[MaxNumberMetricConfig] = Seq.empty,
      sumNumber: Seq[SumNumberMetricConfig] = Seq.empty,
      avgNumber: Seq[AvgNumberMetricConfig] = Seq.empty,
      stdNumber: Seq[StdNumberMetricConfig] = Seq.empty,
      castedNumber: Seq[CastedNumberMetricConfig] = Seq.empty,
      numberInDomain: Seq[NumberInDomainMetricConfig] = Seq.empty,
      numberOutDomain: Seq[NumberOutDomainMetricConfig] = Seq.empty,
      numberLessThan: Seq[NumberLessThanMetricConfig] = Seq.empty,
      numberGreaterThan: Seq[NumberGreaterThanMetricConfig] = Seq.empty,
      numberBetween: Seq[NumberBetweenMetricConfig] = Seq.empty,
      numberNotBetween: Seq[NumberNotBetweenMetricConfig] = Seq.empty,
      numberValues: Seq[NumberValuesMetricConfig] = Seq.empty,
      medianValue: Seq[MedianValueMetricConfig] = Seq.empty,
      firstQuantile: Seq[FirstQuantileMetricConfig] = Seq.empty,
      thirdQuantile: Seq[ThirdQuantileMetricConfig] = Seq.empty,
      getPercentile: Seq[GetPercentileMetricConfig] = Seq.empty,
      getQuantile: Seq[GetQuantileMetricConfig] = Seq.empty,
      columnEq: Seq[ColumnEqMetricConfig] = Seq.empty,
      dayDistance: Seq[DayDistanceMetricConfig] = Seq.empty,
      levenshteinDistance: Seq[LevenshteinDistanceMetricConfig] = Seq.empty,
      coMoment: Seq[CoMomentMetricConfig] = Seq.empty,
      covariance: Seq[CovarianceMetricConfig] = Seq.empty,
      covarianceBessel: Seq[CovarianceBesselMetricConfig] = Seq.empty,
      topN: Seq[TopNMetricConfig] = Seq.empty
  ) {
    def getAllRegularMetrics: Seq[RegularMetricConfig] =
      this.productIterator.toSeq.flatMap(_.asInstanceOf[Seq[Any]]).map(_.asInstanceOf[RegularMetricConfig])
  }

  /** Data Quality job configuration section describing all metrics
    *
    * @param regular
    *   Regular metrics of all subtypes
    * @param composed
    *   Sequence of composed metrics
    */
  final case class MetricsConfig(
      regular: Option[RegularMetricsConfig],
      composed: Seq[ComposedMetricConfig] = Seq.empty
  )
}
