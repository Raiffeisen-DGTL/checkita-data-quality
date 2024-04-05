package ru.raiffeisen.checkita.core.metrics.df.regular

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ru.raiffeisen.checkita.core.metrics.MetricName
import ru.raiffeisen.checkita.core.metrics.df.{ConditionalDFCalculator, DFMetricCalculator}

object BasicNumericDFMetrics {

  /**
   * Calculates minimal numeric value for provided elements
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   */
  case class MinNumberDFMetricCalculator(metricId: String,
                                         columns: Seq[String]) extends DFMetricCalculator {

    val metricName: MetricName = MetricName.MinNumber

    /**
     * Minimum number metric calculator should return maximum double value when DF is empty.
     */
    protected val emptyValue: Column = lit(Double.MaxValue)

    /**
     * Metric error message for cases when all requested column values are nulls for processed rows.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = "Couldn't calculate minimum number out of provided values."

    /**
     * Determines minimum number for row values in requested columns.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    protected def resultExpr: Column =
      if (columns.size == 1) col(columns.head).cast(DoubleType)
      else least(columns.map(c => col(c).cast(DoubleType)): _*)

    /**
     * If least function returned null, then it is a signal that all values in
     * requested columns of processed row were nulls. Thus, minimum number couldn't be
     * calculated. This is a metric increment failure.
     */
    protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * Aggregation function for finding minimum number is simply `min`
     */
    protected val resultAggregateFunction: Column => Column = min
  }


  /**
   * Calculates maximum numeric value for provided elements
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   */
  case class MaxNumberDFMetricCalculator(metricId: String,
                                         columns: Seq[String]) extends DFMetricCalculator {

    val metricName: MetricName = MetricName.MaxNumber

    /**
     * Maximum number metric calculator should return minimum double value when DF is empty.
     */
    protected val emptyValue: Column = lit(Double.MinValue)

    /**
     * Metric error message for cases when all requested column values are nulls for processed rows.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = "Couldn't calculate maximum number out of provided values."

    /**
     * Determines maximum number for row values in requested columns.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    protected def resultExpr: Column =
      if (columns.size == 1) col(columns.head).cast(DoubleType)
      else greatest(columns.map(c => col(c).cast(DoubleType)): _*)

    /**
     * If greatest function returned null, then it is a signal that all values in
     * requested columns of processed row were nulls. Thus, maximum number couldn't be
     * calculated. This is a metric increment failure.
     */
    protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * Aggregation function for finding maximum number is simply `max`
     */
    protected val resultAggregateFunction: Column => Column = max
  }

  /**
   * Calculates sum of provided elements
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   */
  case class SumNumberDFMetricCalculator(metricId: String,
                                         columns: Seq[String]) extends DFMetricCalculator {

    val metricName: MetricName = MetricName.SumNumber

    /**
     * Sum number metric calculator should return zero value when DF is empty.
     */
    protected val emptyValue: Column = lit(0).cast(DoubleType)

    /**
     * Metric error message for cases when some of column values cannot be cast to number.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = "Some of the provided values cannot be cast to number"

    /**
     * Determines sum number for row values in requested columns.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    protected def resultExpr: Column =
      if (columns.size == 1) col(columns.head).cast(DoubleType)
      else columns.map(c => col(c).cast(DoubleType)).foldLeft(lit(0))(_ + _)

    /**
     * When some of the values in processed columns cannot be cast to DoubleType
     * it is a signal of metric increment failure: sum will not be complete for this row since
     * at least one value is null.
     */
    protected def errorConditionExpr: Column = columns.map{ c =>
      when(col(c).cast(DoubleType).isNull, lit(0)).otherwise(lit(1))
    }.foldLeft(lit(0))(_ + _) < lit(columns.size)

    /**
     * Aggregation function to find sum of all values is just `sum`
     */
    protected val resultAggregateFunction: Column => Column = sum
  }


  /**
   * Calculates mean (average) value of provided elements
   *
   * Works for single column only!
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   *
   * @note Null values are omitted:
   *       For values: "3", "7", null
   *       Metric result would be: (3 + 7) / 2 = 5
   */
  case class AvgNumberDFMetricCalculator(metricId: String,
                                         columns: Seq[String]) extends DFMetricCalculator {

    assert(columns.size == 1, "avgNumber metric work for single column only!")

    val metricName: MetricName = MetricName.AvgNumber

    /**
     * Sum number metric calculator should return NaN value when DF is empty.
     */
    protected val emptyValue: Column = lit(Double.NaN)

    /**
     * Metric error message for cases when some of column value cannot be cast to number.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = "Provided value cannot be cast to number"

    /**
     * Retrieves number from requested column of row.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    protected def resultExpr: Column = col(columns.head).cast(DoubleType)

    /**
     * If casting value to DoubleType yields null, then it is a signal that value
     * is not a number. Thus, average number computation can't be
     * incremented for this row. This is a metric increment failure.
     */
    protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * Aggregation function to find average number is just `avg`
     */
    protected val resultAggregateFunction: Column => Column = avg
  }

  /**
   * Calculates standard deviation calculated from provided elements
   *
   * Works for single column only!
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   *
   * @note Null values are omitted.
   * @note Computes population standard deviation.
   */
  case class StdNumberDFMetricCalculator(metricId: String,
                                         columns: Seq[String]) extends DFMetricCalculator {

    assert(columns.size == 1, "stdNumber metric work for single column only!")

    val metricName: MetricName = MetricName.StdNumber

    /**
     * Std number metric calculator should return NaN value when DF is empty.
     */
    protected val emptyValue: Column = lit(Double.NaN)

    /**
     * Metric error message for cases when some of column value cannot be cast to number.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = "Provided value cannot be cast to number"

    /**
     * Retrieves number from requested column of row.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    protected def resultExpr: Column = col(columns.head).cast(DoubleType)

    /**
     * If casting value to DoubleType yields null, then it is a signal that value
     * is not a number. Thus, standard deviation computation can't be
     * incremented for this row. This is a metric increment failure.
     */
    protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * Aggregation function to find standard deviation is just `stddev_pop`
     */
    protected val resultAggregateFunction: Column => Column = stddev_pop
  }

  // todo: maybe this calculator is a candidate for a custom Spark function.
  /**
   * Calculates amount of elements that fit (or do not fit) provided Decimal format: Decimal(precision, scale)
   *
   * @param metricId    Id of the metric.
   * @param columns     Sequence of columns which are used for metric calculation
   * @param precision   Precision threshold
   * @param scale       Required scale
   * @param compareRule Either "inbound" or "outbound": defines wither number should fit
   *                    within provided decimal format or to be outside of the provided precision and scale
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class FormattedNumberDFMetricCalculator(metricId: String,
                                               columns: Seq[String],
                                               precision: Int,
                                               scale: Int,
                                               compareRule: String,
                                               protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    val metricName: MetricName = MetricName.FormattedNumber

    private def criteriaStringRepr: String =
      if (compareRule == "inbound") s"(precision <= $precision; scale <= $scale)"
      else s"(precision > $precision; scale > $scale)"

    /**
     * For direct error collection logic strings numeric values which do not meet provided
     * precision and scale criteria are considered as metric failure.
     * For reversed error collection logic strings values which DO meet provided
     * precision and scale criteria are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) "Some of the provided values CAN be cast to number which meets given" +
        s"precision and scale criteria of $criteriaStringRepr"
      else "Some of the provided values could not be cast to number which meets given" +
        s"precision and scale criteria of $criteriaStringRepr"

    /**
     * Returns expression that gets string representation of a number and then
     * retrieves number of digits out of it.
     * @param colName Column name to apply expression to.
     * @return Spark expression yielding precision of number
     */
    private def getPrecision(colName: String): Column = length(
      regexp_replace(col(colName).cast(DoubleType).cast(StringType), "[\\.-]", "")
    )

    /**
     * Returns expression that gets string representation of a number and then
     * retrieves number of digits after dot.
     * @param colName Column name to apply expression to.
     * @return Spark expression yielding scale of number.
     */
    private def getScale(colName: String): Column = length(
      split(col(colName).cast(DoubleType).cast(StringType), "\\.").getItem(1)
    )

    /**
     * Create spark expression which checks if number meets precision and scale criterion.
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column = compareRule match {
      case "inbound" => getPrecision(colName) <= lit(precision) && getScale(colName) <= lit(scale)
      case "outbound" => getPrecision(colName) > lit(precision) && getScale(colName) > lit(scale)
      case s => throw new IllegalArgumentException(s"Unknown compare rule for FORMATTED_NUMBER metric: '$s'")
    }
  }

  /**
   * Calculates amount of element that can be cast to numerical (double format)
   *
   * @param metricId    Id of the metric.
   * @param columns     Sequence of columns which are used for metric calculation
   * @param reversed    Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class CastedNumberDFMetricCalculator(metricId: String,
                                            columns: Seq[String],
                                            protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    val metricName: MetricName = MetricName.CastedNumber

    /**
     * For direct error collection logic strings numeric values which
     * cannot  be cast to number are considered as metric failure.
     * For reversed error collection logic strings values which
     * CAN be cast to number are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) "Some of the provided values CAN be cast to number"
      else "Some of the provided values cannot be cast to number."

    /**
     * Create spark expression which checks if value can be cast to number (double).
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column = !col(colName).cast(DoubleType).isNull
  }


  /**
   * Calculated amount of numbers in provided domain set
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   * @param domain   Set of numbers that represents the requested domain
   * @param reversed Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class NumberInDomainDFMetricCalculator(metricId: String,
                                              columns: Seq[String],
                                              domain: Set[Double],
                                              protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    override val metricName: MetricName = MetricName.NumberInDomain

    /**
     * For direct error collection logic numeric values which are outside of
     * provided domain are considered as metric failure.
     * For reversed error collection logic numeric values which are within
     * provided domain are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Some of the provided numeric values are IN the given domain of ${domain.mkString("[", ",", "]")}"
      else s"Some of the provided numeric values are not in the given domain of ${domain.mkString("[", ",", "]")}"

    /**
     * Create spark expression which checks if column value is within provided domain.
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column = col(colName).cast(DoubleType).isInCollection(domain)
  }


  /**
   * Calculated amount of numbers that are out of provided domain set
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   * @param domain   Set of numbers that represents the requested domain
   * @param reversed Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class NumberOutDomainDFMetricCalculator(metricId: String,
                                              columns: Seq[String],
                                              domain: Set[Double],
                                              protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    override val metricName: MetricName = MetricName.NumberOutDomain

    /**
     * For direct error collection logic numeric values which are outside of
     * provided domain are considered as metric failure.
     * For reversed error collection logic numeric values which are within
     * provided domain are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Some of the provided numeric values are not in the given domain of ${domain.mkString("[", ",", "]")}"
      else s"Some of the provided numeric values are IN the given domain of ${domain.mkString("[", ",", "]")}"

    /**
     * Create spark expression which checks if column value is within provided domain.
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column = !col(colName).cast(DoubleType).isInCollection(domain)
  }

  /**
   * Counts number of appearances of requested number in processed elements
   *
   * @param metricId     Id of the metric.
   * @param columns      Sequence of columns which are used for metric calculation
   * @param compareValue Requested number to find
   * @param reversed     Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class NumberValuesDFMetricCalculator(metricId: String,
                                            columns: Seq[String],
                                            compareValue: Double,
                                            protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    override val metricName: MetricName = MetricName.NumberValues

    /**
     * For direct error collection logic numeric values which are not equal to
     * provided number value are considered as metric failure.
     *
     * For reversed error collection logic numeric values which are equal to
     * provided number value are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Some of the provided values DO equal to requested number value of '$compareValue'"
      else s"Some of the provided values do not equal to requested number value of '$compareValue'"

    /**
     * Create spark expression which checks if column value is equal to requested value.
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column = col(colName).cast(DoubleType) === lit(compareValue)
  }


  trait NumberCriteriaRepr { this: ConditionalDFCalculator =>

    val criteriaRepr: String

    /**
     * For direct error collection logic values which do not meet
     * provided numeric criteria are considered as metric failure.
     *
     * For reversed error collection logic values which DO meet
     * provided numeric criteria are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Some of the provided values DO meet numeric criteria of '$criteriaRepr'"
      else s"Some of the provided values do not meet numeric criteria of '$criteriaRepr'"

  }

  /**
   * Calculates count of rows for which column value is less than compareValue
   *
   * @param metricId     Id of the metric.
   * @param columns      Sequence of columns which are used for metric calculation
   * @param compareValue Target value to compare with
   * @param includeBound Flag which sets whether compareValue is included or excluded from the interval
   * @param reversed     Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class NumberLessThanDFMetricCalculator(metricId: String,
                                              columns: Seq[String],
                                              compareValue: Double,
                                              includeBound: Boolean,
                                              protected val reversed: Boolean)
    extends ConditionalDFCalculator with NumberCriteriaRepr {

    override val metricName: MetricName = MetricName.NumberLessThan

    override val criteriaRepr: String = if (includeBound) s"<=$compareValue" else s"<$compareValue"

    /**
     * Create spark expression which checks if column value meets numeric criteria
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column =
      if (includeBound) col(colName).cast(DoubleType) <= lit(compareValue)
      else col(colName).cast(DoubleType) < lit(compareValue)
  }

  /**
   * Calculates count of rows for which column value is greater than compareValue
   *
   * @param metricId     Id of the metric.
   * @param columns      Sequence of columns which are used for metric calculation
   * @param compareValue Target value to compare with
   * @param includeBound Flag which sets whether compareValue is included or excluded from the interval
   * @param reversed     Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class NumberGreaterThanDFMetricCalculator(metricId: String,
                                                 columns: Seq[String],
                                                 compareValue: Double,
                                                 includeBound: Boolean,
                                                 protected val reversed: Boolean)
    extends ConditionalDFCalculator with NumberCriteriaRepr {

    override val metricName: MetricName = MetricName.NumberGreaterThan

    override val criteriaRepr: String = if (includeBound) s">=$compareValue" else s">$compareValue"

    /**
     * Create spark expression which checks if column value meets numeric criteria
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column =
      if (includeBound) col(colName).cast(DoubleType) >= lit(compareValue)
      else col(colName).cast(DoubleType) > lit(compareValue)
  }


  /**
   * Calculates count of rows for which column value is within
   * the lowerCompareValue:upperCompareValue interval
   *
   * @param metricId          Id of the metric.
   * @param columns           Sequence of columns which are used for metric calculation
   * @param lowerCompareValue Target lower interval bound to compare with
   * @param upperCompareValue Target upper interval bound to compare with
   * @param includeBound      Flag which sets whether compareValue is included or excluded from the interval
   * @param reversed          Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class NumberBetweenThanDFMetricCalculator(metricId: String,
                                                 columns: Seq[String],
                                                 lowerCompareValue: Double,
                                                 upperCompareValue: Double,
                                                 includeBound: Boolean,
                                                 protected val reversed: Boolean)
    extends ConditionalDFCalculator with NumberCriteriaRepr {

    override val metricName: MetricName = MetricName.NumberBetween

    override val criteriaRepr: String =
      if (includeBound) s">=$lowerCompareValue AND <=$upperCompareValue"
      else s">$lowerCompareValue AND <$upperCompareValue"

    /**
     * Create spark expression which checks if column value meets numeric criteria
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column =
      if (includeBound)
        col(colName).cast(DoubleType) >= lit(lowerCompareValue) &&
          col(colName).cast(DoubleType) <= lit(upperCompareValue)
      else
        col(colName).cast(DoubleType) > lit(lowerCompareValue) &&
          col(colName).cast(DoubleType) < lit(upperCompareValue)

  }


  /**
   * Calculates count of rows for which column value is not within
   * the lowerCompareValue:upperCompareValue interval
   *
   * @param metricId          Id of the metric.
   * @param columns           Sequence of columns which are used for metric calculation
   * @param lowerCompareValue Target lower interval bound to compare with
   * @param upperCompareValue Target upper interval bound to compare with
   * @param includeBound      Flag which sets whether compareValue is included or excluded from the interval
   * @param reversed          Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class NumberNotBetweenThanDFMetricCalculator(metricId: String,
                                                 columns: Seq[String],
                                                 lowerCompareValue: Double,
                                                 upperCompareValue: Double,
                                                 includeBound: Boolean,
                                                 protected val reversed: Boolean)
    extends ConditionalDFCalculator with NumberCriteriaRepr {

    override val metricName: MetricName = MetricName.NumberNotBetween

    override val criteriaRepr: String =
      if (includeBound) s"<=$lowerCompareValue OR >=$upperCompareValue"
      else s"<$lowerCompareValue OR >$upperCompareValue"

    /**
     * Create spark expression which checks if column value meets numeric criteria
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column =
      if (includeBound)
        col(colName).cast(DoubleType) <= lit(lowerCompareValue) ||
          col(colName).cast(DoubleType) >= lit(upperCompareValue)
      else
        col(colName).cast(DoubleType) < lit(lowerCompareValue) ||
          col(colName).cast(DoubleType) > lit(upperCompareValue)

  }
}
