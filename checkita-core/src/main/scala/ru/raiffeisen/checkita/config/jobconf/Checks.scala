package ru.raiffeisen.checkita.config.jobconf

import eu.timepit.refined.types.string.NonEmptyString
import ru.raiffeisen.checkita.config.Enums.TrendCheckRule
import ru.raiffeisen.checkita.config.RefinedTypes.{AccuracyDouble, ID, PositiveInt}
import ru.raiffeisen.checkita.core.checks.CheckCalculator
import ru.raiffeisen.checkita.core.checks.snapshot._
import ru.raiffeisen.checkita.core.checks.trend._

object Checks {

  /**
   * Base class for check configurations.
   * All checks must have an ID and a optional field with check description.
   * Additionally, it is required to define method for appropriate check calculator initiation.
   */
  sealed abstract class CheckConfig {
    val id: ID
    val metric: NonEmptyString
    val description: Option[NonEmptyString]
    def getCalculator: CheckCalculator
  }

  /**
   * Base class for snapshot check configurations.
   * All snapshot checks must have a reference to metric ID over which the check is performed.
   * In addition, snapshot check configuration must have either comparison threshold 
   * or a metric ID to compare with. For some checks it is required to set both.
   */
  sealed abstract class SnapshotCheckConfig extends CheckConfig {
    val compareMetric: Option[NonEmptyString]
    val threshold: Option[Double]
  }

  /**
   * Base class for trend check configurations.
   * All trend checks must have a reference to metric ID over which the check is performed.
   */
  sealed abstract class TrendCheckConfig extends CheckConfig

  /**
   * Trait for average bound checks. These type of checks are characterized
   * by presence of following parameters:
   *   - rule: Window calculation rule: by datetime or by number of records.
   *   - windowSize: Size of the window for average metric value calculation
   *                 (either a number of records or duration).
   *   - windowOffset: Optional window offset (either a number of records or duration)
   */
  sealed trait AverageBoundCheckConfig {
    val rule: TrendCheckRule
    val windowSize: NonEmptyString
    val windowOffset: Option[NonEmptyString]
  }
  
//  /**
//   * Base class for sql check configurations.
//   * All sql checks must have a reference to source ID (must be a JDBC source)
//   * and also a query to execute.
//   */
//  sealed abstract class SqlCheckConfig extends CheckConfig {
//    val source: NonEmptyString
//    val query: NonEmptyString
//  }

  /**
   * 'Differ by less than' check configuration
   * @param id Check ID
   * @param description Check description
   * @param metric Reference to a metric ID over which the check is performed
   * @param compareMetric Reference to a metric ID to compare with
   * @param threshold Threshold value indicating maximum difference between metric and compareMetric
   */
  final case class DifferByLtCheckConfig(
                                    id: ID,
                                    description: Option[NonEmptyString],
                                    metric: NonEmptyString,
                                    compareMetric: Option[NonEmptyString],
                                    threshold: Option[Double]
                                  ) extends SnapshotCheckConfig {
    def getCalculator: CheckCalculator = DifferByLTCheckCalculator(
      id.value, metric.value, compareMetric.map(_.value), threshold.get
    ) // there is a validation check to ensure that compareMetric and threshold are not empty.
  }

  /**
   * 'Equal to' check configuration
   * @param id Check ID
   * @param description Check description
   * @param metric Reference to a metric ID over which the check is performed
   * @param compareMetric Reference to a metric ID to compare with
   * @param threshold Explicit threshold value to compare with
   */
  final case class EqualToCheckConfig(
                                    id: ID,
                                    description: Option[NonEmptyString],
                                    metric: NonEmptyString,
                                    compareMetric: Option[NonEmptyString],
                                    threshold: Option[Double]
                                  ) extends SnapshotCheckConfig {
    def getCalculator: CheckCalculator = EqualToCheckCalculator(
      id.value, metric.value, compareMetric.map(_.value), threshold
    )
  }

  /**
   * 'Less Than' check configuration
   * @param id Check ID
   * @param description Check description
   * @param metric Reference to a metric ID over which the check is performed
   * @param compareMetric Reference to a metric ID to compare with
   * @param threshold Explicit threshold value to compare with
   */
  final case class LessThanCheckConfig(
                                  id: ID,
                                  description: Option[NonEmptyString],
                                  metric: NonEmptyString,
                                  compareMetric: Option[NonEmptyString],
                                  threshold: Option[Double]
                                ) extends SnapshotCheckConfig {
    def getCalculator: CheckCalculator = LessThanCheckCalculator(
      id.value, metric.value, compareMetric.map(_.value), threshold
    )
  }

  /**
   * 'Greater than' check configuration
   * @param id Check ID
   * @param description Check description
   * @param metric Reference to a metric ID over which the check is performed
   * @param compareMetric Reference to a metric ID to compare with
   * @param threshold Explicit threshold value to compare with
   */
  final case class GreaterThanCheckConfig(
                                     id: ID,
                                     description: Option[NonEmptyString],
                                     metric: NonEmptyString,
                                     compareMetric: Option[NonEmptyString],
                                     threshold: Option[Double]
                                   ) extends SnapshotCheckConfig {
    def getCalculator: CheckCalculator = GreaterThanCheckCalculator(
      id.value, metric.value, compareMetric.map(_.value), threshold
    )
  }

  /**
   * 'Average bound full' check configuration
   * @param id Check ID
   * @param description Check description
   * @param metric Reference to a metric ID over which the check is performed
   * @param rule Window calculation rule: by datetime or by number of records.
   * @param windowSize Size of the window for average metric value calculation
   *                   (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param threshold Threshold value to calculate upper and lower bounds to compare with.
   */
  final case class AverageBoundFullCheckConfig(
                                        id: ID,
                                        description: Option[NonEmptyString],
                                        metric: NonEmptyString,
                                        rule: TrendCheckRule,
                                        windowSize: NonEmptyString,
                                        windowOffset: Option[NonEmptyString],
                                        threshold: AccuracyDouble
                                        ) extends TrendCheckConfig with AverageBoundCheckConfig {
    def getCalculator: CheckCalculator = AverageBoundFullCheckCalculator(
      id.value, metric.value, threshold.value, rule, windowSize.value, windowOffset.map(_.value)
    )
  }

  /**
   * 'Average bound upper' check configuration
   * @param id Check ID
   * @param description Check description
   * @param metric Reference to a metric ID over which the check is performed
   * @param rule Window calculation rule: by datetime or by number of records.
   * @param windowSize Size of the window for average metric value calculation
   *                   (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param threshold Threshold value to calculate upper bound to compare with.
   */
  final case class AverageBoundUpperCheckConfig(
                                          id: ID,
                                          description: Option[NonEmptyString],
                                          metric: NonEmptyString,
                                          rule: TrendCheckRule,
                                          windowSize: NonEmptyString,
                                          windowOffset: Option[NonEmptyString],
                                          threshold: AccuracyDouble
                                        ) extends TrendCheckConfig with AverageBoundCheckConfig {
    def getCalculator: CheckCalculator = AverageBoundUpperCheckCalculator(
      id.value, metric.value, threshold.value, rule, windowSize.value, windowOffset.map(_.value)
    )
  }

  /**
   * 'Average bound lower' check configuration
   * @param id Check ID
   * @param description Check description
   * @param metric Reference to a metric ID over which the check is performed
   * @param rule Window calculation rule: by datetime or by number of records.
   * @param windowSize Size of the window for average metric value calculation
   *                   (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param threshold Threshold value to calculate lower bound to compare with.
   */
  final case class AverageBoundLowerCheckConfig(
                                           id: ID,
                                           description: Option[NonEmptyString],
                                           metric: NonEmptyString,
                                           rule: TrendCheckRule,
                                           windowSize: NonEmptyString,
                                           windowOffset: Option[NonEmptyString],
                                           threshold: AccuracyDouble
                                         ) extends TrendCheckConfig with AverageBoundCheckConfig {
    def getCalculator: CheckCalculator = AverageBoundLowerCheckCalculator(
      id.value, metric.value, threshold.value, rule, windowSize.value, windowOffset.map(_.value)
    )
  }

  /**
   * 'Average bound range' check configuration
   * @param id Check ID
   * @param description Check description
   * @param metric Reference to a metric ID over which the check is performed
   * @param rule Window calculation rule: by datetime or by number of records.
   * @param windowSize Size of the window for average metric value calculation
   *                   (either a number of records or duration).
   * @param windowOffset Optional window offset (either a number of records or duration)
   * @param thresholdLower Threshold value to calculate lower bound to compare with.
   * @param thresholdUpper Threshold value to calculate upper bound to compare with.
   */
  final case class AverageBoundRangeCheckConfig(
                                           id: ID,
                                           description: Option[NonEmptyString],
                                           metric: NonEmptyString,
                                           rule: TrendCheckRule,
                                           windowSize: NonEmptyString,
                                           windowOffset: Option[NonEmptyString],
                                           thresholdLower: AccuracyDouble,
                                           thresholdUpper: AccuracyDouble
                                         ) extends TrendCheckConfig with AverageBoundCheckConfig {
    def getCalculator: CheckCalculator = AverageBoundRangeCheckCalculator(
      id.value, metric.value, thresholdLower.value, thresholdUpper.value,
      rule, windowSize.value, windowOffset.map(_.value)
    )
  }

  /**
   * TopN rank check configuration
   * @param id Check ID
   * @param description Check description
   * @param metric Reference to a metric ID over which the check is performed
   * @param targetNumber Number of records from TopN metric result (R <= N)
   * @param threshold Threshold value representing maximum allowed Jacquard distance
   *                  between current and previous R-records from TopN metric result.
   */
  final case class TopNRankCheckConfig(
                                  id: ID,
                                  description: Option[NonEmptyString],
                                  metric: NonEmptyString,
                                  targetNumber: PositiveInt,
                                  threshold: AccuracyDouble
                                ) extends TrendCheckConfig {
    def getCalculator: CheckCalculator = TopNRankCheckCalculator(
      id.value, metric.value, targetNumber.value, threshold.value
    )
  }

  /**
   * Data Quality job configuration section describing snapshot checks.
   * @param differByLT Sequence of 'differ by less than' checks
   * @param equalTo Sequence of 'equal to' checks
   * @param lessThan Sequence of 'less than' checks
   * @param greaterThan Sequence of 'greater than' checks
   */
  final case class SnapshotChecks(
                                   differByLT: Seq[DifferByLtCheckConfig] = Seq.empty,
                                   equalTo: Seq[EqualToCheckConfig] = Seq.empty,
                                   lessThan: Seq[LessThanCheckConfig] = Seq.empty,
                                   greaterThan: Seq[GreaterThanCheckConfig] = Seq.empty
                                 ) {
    def getAllSnapShotChecks: Seq[SnapshotCheckConfig] =
      this.productIterator.toSeq.flatMap(_.asInstanceOf[Seq[Any]]).map(_.asInstanceOf[SnapshotCheckConfig])
  }

  /**
   * Data Quality job configuration section describing trend checks.
   * @param averageBoundFull Sequence of 'average bound full' checks
   * @param averageBoundUpper Sequence of 'average bound upper' checks
   * @param averageBoundLower Sequence of 'average bound lower' checks
   * @param averageBoundRange Sequence of 'average bound range' checks
   * @param topNRank Sequence of TopN rank checks
   */
  final case class TrendChecks(
                                averageBoundFull: Seq[AverageBoundFullCheckConfig] = Seq.empty,
                                averageBoundUpper: Seq[AverageBoundUpperCheckConfig] = Seq.empty,
                                averageBoundLower: Seq[AverageBoundLowerCheckConfig] = Seq.empty,
                                averageBoundRange: Seq[AverageBoundRangeCheckConfig] = Seq.empty,
                                topNRank: Seq[TopNRankCheckConfig] = Seq.empty
                              ) {
    def getAllTrendChecks: Seq[TrendCheckConfig] =
      this.productIterator.toSeq.flatMap(_.asInstanceOf[Seq[Any]]).map(_.asInstanceOf[TrendCheckConfig])
  }

  /**
   * Data Quality job configuration section describing all checks.
   * @param snapshot Snapshot checks of all subtypes
   * @param trend Trend checks of all subtypes
   */
  final case class ChecksConfig(
                                 snapshot: Option[SnapshotChecks],
                                 trend: Option[TrendChecks]
                               ) {
    def getAllChecks: Seq[CheckConfig] =
      snapshot.map(_.getAllSnapShotChecks).getOrElse(Seq.empty).map(_.asInstanceOf[CheckConfig]) ++
        trend.map(_.getAllTrendChecks).getOrElse(Seq.empty).map(_.asInstanceOf[CheckConfig])
  }
}
