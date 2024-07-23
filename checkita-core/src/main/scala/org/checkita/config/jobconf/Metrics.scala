package org.checkita.config.jobconf

import eu.timepit.refined.types.string.NonEmptyString
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.json4s.jackson.Serialization.write
import org.checkita.config.Enums.TrendCheckRule
import org.checkita.config.RefinedTypes._
import org.checkita.config.jobconf.MetricParams._
import org.checkita.core.metrics._
import org.checkita.core.metrics.df.DFMetricCalculator
import org.checkita.core.metrics.df.regular.ApproxCardinalityDFMetrics._
import org.checkita.core.metrics.df.regular.BasicNumericDFMetrics._
import org.checkita.core.metrics.df.regular.BasicStringDFMetrics._
import org.checkita.core.metrics.df.regular.FileDFMetrics._
import org.checkita.core.metrics.df.regular.GroupingDFMetrics._
import org.checkita.core.metrics.df.regular.MultiColumnDFMetrics._
import org.checkita.core.metrics.rdd.RDDMetricCalculator
import org.checkita.core.metrics.rdd.regular.BasicStringRDDMetrics._
import org.checkita.core.metrics.rdd.regular.BasicNumericRDDMetrics._
import org.checkita.core.metrics.rdd.regular.AlgebirdRDDMetrics._
import org.checkita.core.metrics.rdd.regular.MultiColumnRDDMetrics._
import org.checkita.core.metrics.rdd.regular.FileRDDMetrics._
import org.checkita.utils.Common.{getFieldsMap, jsonFormats}


object Metrics {

  /**
   * Base class for all metrics configurations. All metrics are described as DQ entities.
   */
  sealed abstract class MetricConfig extends JobConfigEntity {
    val metricId: String = id.value // actual ID value after validation.
  }

  /**
   * Base class for all regular metric configurations (except row count metric). All regular metrics must have a
   * reference to source ID over which the metric is being calculated. In addition, regular metrics must have non-empty
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

  /** Base class for regular metrics that works with any number of columns.
    */
  sealed abstract class AnyColumnRegularMetricConfig extends RegularMetricConfig {
    val columns: NonEmptyStringSeq
    val metricColumns: Seq[String] = columns.value
  }

  /** Base class for regular metrics that works only with single column.
    */
  sealed abstract class SingleColumnRegularMetricConfig extends RegularMetricConfig {
    val columns: SingleElemStringSeq
    val metricColumns: Seq[String] = columns.value
  }

  /** Base class for regular metrics that works only with two columns.
    */
  sealed abstract class DoubleColumnRegularMetricConfig extends RegularMetricConfig {
    val columns: DoubleElemStringSeq
    val metricColumns: Seq[String] = columns.value
  }

  /** Base class for regular metrics that works with at least two columns.
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

  /**
   * Base class for trend metrics that computes statistics over historical metric results.
   */
  sealed abstract class TrendMetricConfig extends MetricConfig with TrendMetric {
    val lookupMetric: ID
    val windowSize: NonEmptyString
    val windowOffset: Option[NonEmptyString]
    val paramString: Option[String]
    
    override val lookupMetricId: String = lookupMetric.value
    override val wSize: String = windowSize.value
    override val wOffset: Option[String] = windowOffset.map(_.value)
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
    def initRDDMetricCalculator: RDDMetricCalculator = new RowCountRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = RowCountDFMetricCalculator(metricId)
  }

  /** Duplicate values regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new DuplicateValuesRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = 
      DuplicateValuesDFMetricCalculator(metricId, columns)
  }

  /** Distinct values regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new DistinctValuesRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = DistinctValuesDFMetricCalculator(metricId, columns)
  }

  /** Approximate distinct values regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator =
      new HyperLogLogRDDMetricCalculator(params.accuracyError.value)
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = ApproximateDistinctValuesDFMetricCalculator(
      metricId, columns, params.accuracyError.value
    )
  }

  /** Null values regular metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator = new NullValuesRDDMetricCalculator(reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = NullValuesDFMetricCalculator(
      metricId, columns, reversed
    )
  }

  /** Empty values regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator = new EmptyValuesRDDMetricCalculator(reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = EmptyValuesDFMetricCalculator(
      metricId, columns, reversed
    )
  }

  /** Completeness regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new CompletenessRDDMetricCalculator(params.includeEmptyStrings, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = CompletenessDFMetricCalculator(
      metricId, columns, params.includeEmptyStrings, reversed
    )
  }

  /** Emptiness regular metric configuration
   *
   * @param id          Metric ID
   * @param description Metric description
   * @param source      Source ID over which metric is being calculated
   * @param columns     Sequence of columns which are used for metric calculation
   * @param params      Metric parameters
   * @param metadata    List of metadata parameters specific to this metric
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  final case class EmptinessMetricConfig(
                                          id: ID,
                                          description: Option[NonEmptyString],
                                          source: NonEmptyString,
                                          columns: NonEmptyStringSeq,
                                          params: CompletenessParams = CompletenessParams(),
                                          metadata: Seq[SparkParam] = Seq.empty,
                                          reversed: Boolean = true
                                        ) extends AnyColumnRegularMetricConfig with ReversibleMetric {
    val metricName: MetricName      = MetricName.Emptiness
    val paramString: Option[String] = Some(write(getFieldsMap(params)))
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new EmptinessRDDMetricCalculator(params.includeEmptyStrings, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = EmptinessDFMetricCalculator(
      metricId, columns, params.includeEmptyStrings, reversed
    )
  }

  /** Sequence completeness regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator =
      new SequenceCompletenessRDDMetricCalculator(params.increment.value)
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator =
      SequenceCompletenessDFMetricCalculator(metricId, columns, params.increment.value)
  }

  /** Sequence completeness regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator =
      new HLLSequenceCompletenessRDDMetricCalculator(params.accuracyError.value, params.increment.value)
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = ApproximateSequenceCompletenessDFMetricCalculator(
      metricId, columns, params.accuracyError.value, params.increment.value
    )
  }

  /** Min string regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new MinStringRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = MinStringDFMetricCalculator(metricId, columns)
  }

  /** Max string regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new MaxStringRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = MaxStringDFMetricCalculator(metricId, columns)
  }

  /** Average string regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new AvgStringRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = AvgStringDFMetricCalculator(metricId, columns)
  }

  /** String length regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new StringLengthRDDMetricCalculator(params.length.value, params.compareRule.toString.toLowerCase, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = StringLengthDFMetricCalculator(
      metricId, columns, params.length.value, params.compareRule.toString.toLowerCase, reversed
    )
  }

  /** String in domain regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new StringInDomainRDDMetricCalculator(params.domain.value.toSet, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = StringInDomainDFMetricCalculator(
      metricId, columns, params.domain.value.toSet, reversed
    )
  }

  /** String out domain regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new StringOutDomainRDDMetricCalculator(params.domain.value.toSet, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = StringOutDomainDFMetricCalculator(
      metricId, columns, params.domain.value.toSet, reversed
    )
  }

  /** String values regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new StringValuesRDDMetricCalculator(params.compareValue.value, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = StringValuesDFMetricCalculator(
      metricId, columns, params.compareValue.value, reversed
    )
  }

  /** Regex match regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new RegexMatchRDDMetricCalculator(params.regex.value, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = RegexMatchDFMetricCalculator(
      metricId, columns, params.regex.value, reversed
    )
  }

  /** Regex mismatch regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new RegexMismatchRDDMetricCalculator(params.regex.value, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = RegexMismatchDFMetricCalculator(
      metricId, columns, params.regex.value, reversed
    )
  }

  /** Formatted date regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new FormattedDateRDDMetricCalculator(params.dateFormat.pattern, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = FormattedDateDFMetricCalculator(
      metricId, columns, params.dateFormat.pattern, reversed
    )
  }

  /** Formatted number regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator = new FormattedNumberRDDMetricCalculator(
      params.precision.value,
      params.scale.value,
      params.compareRule.toString.toLowerCase,
      reversed
    )
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = FormattedNumberDFMetricCalculator(
      metricId,
      columns,
      params.precision.value,
      params.scale.value,
      params.compareRule.toString.toLowerCase,
      reversed
    )
  }

  /** Min number regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new MinNumberRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = MinNumberDFMetricCalculator(metricId, columns)
  }

  /** Max number regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new MaxNumberRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = MaxNumberDFMetricCalculator(metricId, columns)
  }

  /** Sum number regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new SumNumberRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = SumNumberDFMetricCalculator(metricId, columns)
  }

  /** Average number regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new StdAvgNumberRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = AvgNumberDFMetricCalculator(metricId, columns)
  }

  /** Standard deviation number regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new StdAvgNumberRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = StdNumberDFMetricCalculator(metricId, columns)
  }

  /** Casted number regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new CastedNumberRDDMetricCalculator(reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator =
      CastedNumberDFMetricCalculator(metricId, columns, reversed)
  }

  /** Number in domain regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new NumberInDomainRDDMetricCalculator(params.domain.value.toSet, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator =
      NumberInDomainDFMetricCalculator(metricId, columns, params.domain.value.toSet, reversed)
  }

  /** Number out domain regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new NumberOutDomainRDDMetricCalculator(params.domain.value.toSet, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator =
      NumberOutDomainDFMetricCalculator(metricId, columns, params.domain.value.toSet, reversed)
  }

  /** Number less than regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new NumberLessThanRDDMetricCalculator(params.compareValue, params.includeBound, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator =
      NumberLessThanDFMetricCalculator(metricId, columns, params.compareValue, params.includeBound, reversed)
  }

  /** Number greater than regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new NumberGreaterThanRDDMetricCalculator(params.compareValue, params.includeBound, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator =
      NumberGreaterThanDFMetricCalculator(metricId, columns, params.compareValue, params.includeBound, reversed)
  }

  /** Number between regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator = new NumberBetweenRDDMetricCalculator(
      params.lowerCompareValue, params.upperCompareValue, params.includeBound, reversed
    )
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = NumberBetweenDFMetricCalculator(
      metricId, columns, params.lowerCompareValue, params.upperCompareValue, params.includeBound, reversed
    )
  }

  /** Number not between regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator = new NumberNotBetweenRDDMetricCalculator(
      params.lowerCompareValue, params.upperCompareValue, params.includeBound, reversed
    )
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = NumberNotBetweenDFMetricCalculator(
      metricId, columns, params.lowerCompareValue, params.upperCompareValue, params.includeBound, reversed
    )
  }

  /** Number values regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new NumberValuesRDDMetricCalculator(params.compareValue, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = NumberValuesDFMetricCalculator(
      metricId, columns, params.compareValue, reversed
    )
  }

  /** TDigest Median value regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator =
      new TDigestRDDMetricCalculator(params.accuracyError.value, 0)
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = MedianValueDFMetricCalculator(
      metricId, columns, params.accuracyError.value
    )
  }

  /** TDigest First quantile regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator =
      new TDigestRDDMetricCalculator(params.accuracyError.value, 0)
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = FirstQuantileDFMetricCalculator(
      metricId, columns, params.accuracyError.value
    )
  }

  /** TDigest Third quantile regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator =
      new TDigestRDDMetricCalculator(params.accuracyError.value, 0)
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = ThirdQuantileDFMetricCalculator(
      metricId, columns, params.accuracyError.value
    )
  }

  /** TDigest Get quantile regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator =
      new TDigestRDDMetricCalculator(params.accuracyError.value, params.target.value)
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = GetQuantileDFMetricCalculator(
      metricId, columns, params.accuracyError.value, params.target.value
    )
  }

  /** TDigest Get percentile regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator =
      new TDigestRDDMetricCalculator(params.accuracyError.value, params.target)
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = GetPercentileDFMetricCalculator(
      metricId, columns, params.accuracyError.value, params.target
    )
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator = new ColumnEqRDDMetricCalculator(reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator =
      ColumnEqDFMetricCalculator(metricId, columns, reversed)
  }

  /** Day distance regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new DayDistanceRDDMetricCalculator(params.dateFormat.pattern, params.threshold.value, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = DayDistanceDFMetricCalculator(
      metricId, columns, params.dateFormat.pattern, params.threshold.value, reversed
    )
  }

  /** Levenshtein distance regular metric configuration
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
    def initRDDMetricCalculator: ReversibleRDDMetricCalculator =
      new LevenshteinDistanceRDDMetricCalculator(params.threshold, params.normalize, reversed)
    def initDFMetricCalculator(columns: Seq[String]): ReversibleDFMetricCalculator = LevenshteinDistanceDFMetricCalculator(
      metricId, columns, params.threshold, params.normalize, reversed
    )
  }

  /** Co-moment regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new CovarianceRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = CoMomentDFMetricCalculator(metricId, columns)
  }

  /** Covariance regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new CovarianceRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = CovarianceDFMetricCalculator(metricId, columns)
  }

  /** Covariance Bessel regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator = new CovarianceRDDMetricCalculator()
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = CovarianceBesselDFMetricCalculator(metricId, columns)
  }

  /** TopN regular metric configuration
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
    def initRDDMetricCalculator: RDDMetricCalculator =
      new TopKRDDMetricCalculator(params.maxCapacity.value, params.targetNumber.value)
    def initDFMetricCalculator(columns: Seq[String]): DFMetricCalculator = TopNDFMetricCalculator(
      metricId, columns, params.maxCapacity.value, params.targetNumber.value
    )
  }

  /**
   * Average trend metric configuration
   *
   * @param id           Metric ID
   * @param description  Metric Description
   * @param lookupMetric Metric which historical results to pull for statistic calculation
   * @param rule         Window calculation rule: by datetime or by number of records.
   * @param windowSize   Size of the window for average metric value calculation (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param metadata     List of metadata parameters specific to this metric
   */
  final case class AvgTrendMetricConfig(
                                         id: ID,
                                         description: Option[NonEmptyString],
                                         lookupMetric: ID,
                                         rule: TrendCheckRule,
                                         windowSize: NonEmptyString,
                                         windowOffset: Option[NonEmptyString],
                                         metadata: Seq[SparkParam] = Seq.empty
                                       ) extends TrendMetricConfig {
    val aggFunc: DescriptiveStatistics => Double = stats => stats.getMean
    val metricName: MetricName = MetricName.TrendAvg
    val paramString: Option[String] = None
  }

  /**
   * Standard deviation trend metric configuration
   *
   * @param id           Metric ID
   * @param description  Metric Description
   * @param lookupMetric Metric which historical results to pull for statistic calculation
   * @param rule         Window calculation rule: by datetime or by number of records.
   * @param windowSize   Size of the window for average metric value calculation (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param metadata     List of metadata parameters specific to this metric
   */
  final case class StdTrendMetricConfig(
                                         id: ID,
                                         description: Option[NonEmptyString],
                                         lookupMetric: ID,
                                         rule: TrendCheckRule,
                                         windowSize: NonEmptyString,
                                         windowOffset: Option[NonEmptyString],
                                         metadata: Seq[SparkParam] = Seq.empty
                                       ) extends TrendMetricConfig {
    val aggFunc: DescriptiveStatistics => Double = stats => stats.getStandardDeviation
    val metricName: MetricName = MetricName.TrendStd
    val paramString: Option[String] = None
  }

  /**
   * Min trend metric configuration
   *
   * @param id           Metric ID
   * @param description  Metric Description
   * @param lookupMetric Metric which historical results to pull for statistic calculation
   * @param rule         Window calculation rule: by datetime or by number of records.
   * @param windowSize   Size of the window for average metric value calculation (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param metadata     List of metadata parameters specific to this metric
   */
  final case class MinTrendMetricConfig(
                                         id: ID,
                                         description: Option[NonEmptyString],
                                         lookupMetric: ID,
                                         rule: TrendCheckRule,
                                         windowSize: NonEmptyString,
                                         windowOffset: Option[NonEmptyString],
                                         metadata: Seq[SparkParam] = Seq.empty
                                       ) extends TrendMetricConfig {
    val aggFunc: DescriptiveStatistics => Double = stats => stats.getMin
    val metricName: MetricName = MetricName.TrendMin
    val paramString: Option[String] = None
  }

  /**
   * Max trend metric configuration
   *
   * @param id           Metric ID
   * @param description  Metric Description
   * @param lookupMetric Metric which historical results to pull for statistic calculation
   * @param rule         Window calculation rule: by datetime or by number of records.
   * @param windowSize   Size of the window for average metric value calculation (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param metadata     List of metadata parameters specific to this metric
   */
  final case class MaxTrendMetricConfig(
                                         id: ID,
                                         description: Option[NonEmptyString],
                                         lookupMetric: ID,
                                         rule: TrendCheckRule,
                                         windowSize: NonEmptyString,
                                         windowOffset: Option[NonEmptyString],
                                         metadata: Seq[SparkParam] = Seq.empty
                                       ) extends TrendMetricConfig {
    val aggFunc: DescriptiveStatistics => Double = stats => stats.getMax
    val metricName: MetricName = MetricName.TrendMax
    val paramString: Option[String] = None
  }

  /**
   * Sum trend metric configuration
   *
   * @param id           Metric ID
   * @param description  Metric Description
   * @param lookupMetric Metric which historical results to pull for statistic calculation
   * @param rule         Window calculation rule: by datetime or by number of records.
   * @param windowSize   Size of the window for average metric value calculation (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param metadata     List of metadata parameters specific to this metric
   */
  final case class SumTrendMetricConfig(
                                         id: ID,
                                         description: Option[NonEmptyString],
                                         lookupMetric: ID,
                                         rule: TrendCheckRule,
                                         windowSize: NonEmptyString,
                                         windowOffset: Option[NonEmptyString],
                                         metadata: Seq[SparkParam] = Seq.empty
                                       ) extends TrendMetricConfig {
    val aggFunc: DescriptiveStatistics => Double = stats => stats.getSum
    val metricName: MetricName = MetricName.TrendSum
    val paramString: Option[String] = None
  }

  /**
   * Median trend metric configuration
   *
   * @param id           Metric ID
   * @param description  Metric Description
   * @param lookupMetric Metric which historical results to pull for statistic calculation
   * @param rule         Window calculation rule: by datetime or by number of records.
   * @param windowSize   Size of the window for average metric value calculation (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param metadata     List of metadata parameters specific to this metric
   */
  final case class MedianTrendMetricConfig(
                                            id: ID,
                                            description: Option[NonEmptyString],
                                            lookupMetric: ID,
                                            rule: TrendCheckRule,
                                            windowSize: NonEmptyString,
                                            windowOffset: Option[NonEmptyString],
                                            metadata: Seq[SparkParam] = Seq.empty
                                          ) extends TrendMetricConfig {
    val aggFunc: DescriptiveStatistics => Double = stats => stats.getPercentile(50)
    val metricName: MetricName = MetricName.TrendMedian
    val paramString: Option[String] = None
  }

  /**
   * First Quartile trend metric configuration
   *
   * @param id           Metric ID
   * @param description  Metric Description
   * @param lookupMetric Metric which historical results to pull for statistic calculation
   * @param rule         Window calculation rule: by datetime or by number of records.
   * @param windowSize   Size of the window for average metric value calculation (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param metadata     List of metadata parameters specific to this metric
   */
  final case class FirstQuartileTrendMetricConfig(
                                                   id: ID,
                                                   description: Option[NonEmptyString],
                                                   lookupMetric: ID,
                                                   rule: TrendCheckRule,
                                                   windowSize: NonEmptyString,
                                                   windowOffset: Option[NonEmptyString],
                                                   metadata: Seq[SparkParam] = Seq.empty
                                                 ) extends TrendMetricConfig {
    val aggFunc: DescriptiveStatistics => Double = stats => stats.getPercentile(25)
    val metricName: MetricName = MetricName.TrendFirstQ
    val paramString: Option[String] = None
  }

  /**
   * Third Quartile trend metric configuration
   *
   * @param id           Metric ID
   * @param description  Metric Description
   * @param lookupMetric Metric which historical results to pull for statistic calculation
   * @param rule         Window calculation rule: by datetime or by number of records.
   * @param windowSize   Size of the window for average metric value calculation (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param metadata     List of metadata parameters specific to this metric
   */
  final case class ThirdQuartileTrendMetricConfig(
                                                   id: ID,
                                                   description: Option[NonEmptyString],
                                                   lookupMetric: ID,
                                                   rule: TrendCheckRule,
                                                   windowSize: NonEmptyString,
                                                   windowOffset: Option[NonEmptyString],
                                                   metadata: Seq[SparkParam] = Seq.empty
                                                 ) extends TrendMetricConfig {
    val aggFunc: DescriptiveStatistics => Double = stats => stats.getPercentile(75)
    val metricName: MetricName = MetricName.TrendThirdQ
    val paramString: Option[String] = None
  }

  /**
   * Quantile trend metric configuration
   *
   * @param id           Metric ID
   * @param description  Metric Description
   * @param lookupMetric Metric which historical results to pull for statistic calculation
   * @param quantile     Quantile to compute over historical metric results (must be a number in range [0, 1]).
   * @param rule         Window calculation rule: by datetime or by number of records.
   * @param windowSize   Size of the window for average metric value calculation (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param metadata     List of metadata parameters specific to this metric
   */
  final case class QuantileTrendMetricConfig(
                                              id: ID,
                                              description: Option[NonEmptyString],
                                              lookupMetric: ID,
                                              quantile: PercentileDouble,
                                              rule: TrendCheckRule,
                                              windowSize: NonEmptyString,
                                              windowOffset: Option[NonEmptyString],
                                              metadata: Seq[SparkParam] = Seq.empty
                                            ) extends TrendMetricConfig {
    val aggFunc: DescriptiveStatistics => Double = stats => stats.getPercentile(quantile.value * 100)
    val metricName: MetricName = MetricName.TrendQuantile
    val paramString: Option[String] = Some(write(Map("quantile" -> quantile.value)))
  }
  
  /** Data Quality job configuration section describing regular metrics
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
      emptiness: Seq[EmptinessMetricConfig] = Seq.empty,
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
   * @param regular  Regular metrics of all subtypes
   * @param composed Sequence of composed metrics
   * @param trend    Sequence of trend metrics
   */
  final case class MetricsConfig(
      regular: Option[RegularMetricsConfig],
      composed: Seq[ComposedMetricConfig] = Seq.empty,
      trend: Seq[TrendMetricConfig] = Seq.empty
  )
}
