package ru.raiffeisen.checkita.core.metrics.df.regular

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ru.raiffeisen.checkita.core.metrics.MetricName
import ru.raiffeisen.checkita.core.metrics.df.{ConditionalDFCalculator, DFMetricCalculator, ReversibleDFCalculator}

object BasicStringDFMetrics {

  /**
   * Calculates amount of values that match the provided regular expression
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   * @param regex    Regex pattern
   * @param reversed Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class RegexMatchDFMetricCalculator(metricId: String,
                                          columns: Seq[String],
                                          regex: String,
                                          protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    val metricName: MetricName = MetricName.RegexMatch

    /**
     * For direct error collection logic string values that failed to match regex pattern
     * are considered as metric failure.
     * For reverses error collection logic string values that DO match regex pattern
     * are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String =
      if (reversed) s"Some of the values DO match regex pattern '$regex'."
      else s"Some of the values failed to match regex pattern '$regex'."

    def metricCondExpr(colName: String): Column = col(colName).cast(StringType).rlike(regex)
  }

  /**
   * Calculates amount of values that do not match the provided regular expression
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   * @param regex    Regex pattern
   * @param reversed Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class RegexMismatchDFMetricCalculator(metricId: String,
                                             columns: Seq[String],
                                             regex: String,
                                             protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    val metricName: MetricName = MetricName.RegexMismatch

    /**
     * For direct error collection logic string values that DO match regex pattern
     * are considered as metric failure.
     * For reverses error collection logic string values that failed to match regex pattern
     * are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String =
      if (reversed) s"Some of the values failed to match regex pattern '$regex'."
      else s"Some of the values DO match regex pattern '$regex'."

    def metricCondExpr(colName: String): Column = !col(colName).cast(StringType).rlike(regex)
  }

  /**
   * Calculates amount of null values in processed elements
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   * @param reversed Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class NullValuesDFMetricCalculator(metricId: String,
                                          columns: Seq[String],
                                          protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    val metricName: MetricName = MetricName.NullValues

    /**
     * For direct error collection logic any non-null values are considered as metric failure.
     * For reversed error collection logic null values are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String =
      if (reversed) s"There are null values found within processed values."
      else s"There are non-null values found within processed values."

    def metricCondExpr(colName: String): Column = col(colName).isNull
  }

  /**
   * Calculates completeness of values in the specified columns
   *
   * @param metricId            Id of the metric.
   * @param columns             Sequence of columns which are used for metric calculation
   * @param includeEmptyStrings Flag which sets whether empty strings are considered in addition to null values.
   * @param reversed            Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class CompletenessDFMetricCalculator(metricId: String,
                                            columns: Seq[String],
                                            includeEmptyStrings: Boolean,
                                            protected val reversed: Boolean)
    extends DFMetricCalculator with ReversibleDFCalculator {

    val metricName: MetricName = MetricName.Completeness

    /**
     * Completeness metric returns NaN when DF is empty.
     */
    protected val emptyValue: Column = lit(Double.NaN)

    /**
     * For direct error collection logic any non-null (or non-empty if `includeEmptyStrings` is `true`)
     * values are considered as metric failure.
     * For reversed error collection logic null (or empty if `includeEmptyStrings` is `true`) values
     * are considered as metric failure
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = (includeEmptyStrings, reversed) match {
      case (true, false) => s"There are non-null or non-empty values found within processed values."
      case (true, true) => s"There are null or empty values found within processed values."
      case (false, false) => s"There are non-null values found within processed values."
      case (false, true) => s"There are null values found within processed values."
    }

    def metricCondExpr(colName: String): Column =
      if (includeEmptyStrings) col(colName).isNull || col(colName).cast(StringType) === lit("")
      else col(colName).isNull

    /**
     * Spark expression yielding numeric result for processed row.
     * Metric will be incremented by number of non-null (or non-empty if `includeEmptyStrings` is `true`)
     * values within requested columns.
     * @return Spark row-level expression yielding numeric result.
     */
    override protected val resultExpr: Column = columns.map{ c =>
      when(metricCondExpr(c), lit(0)).otherwise(lit(1))
    }.foldLeft(lit(0))(_ + _)

    override protected val errorConditionExpr: Column =
      if (reversed) resultExpr < lit(columns.size) else resultExpr > 0

    /**
     * Completeness metric is aggregated as ration of total number of non-null (or non-empty)
     * cells to total number of cells that were processed.
     */
    override protected val resultAggregateFunction: Column => Column =
      rowRes => sum(rowRes) / count(lit(1)) / lit(columns.size)
  }

  /**
   * Calculates amount of empty strings in processed elements.
   *
   * @param metricId            Id of the metric.
   * @param columns             Sequence of columns which are used for metric calculation.
   * @param reversed            Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class EmptyValuesDFMetricCalculator(metricId: String,
                                           columns: Seq[String],
                                           protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    val metricName: MetricName = MetricName.EmptyValues

    /**
     * For direct error collection logic any non-empty value is considered as metric failure.
     * For reversed error collection logic any empty value is considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = {
      if (reversed) s"There are empty strings found within processed values."
      else s"There are non-empty strings found within processed values."
    }

    def metricCondExpr(colName: String): Column = col(colName).cast(StringType) === lit("")
  }

  /**
   * Calculates minimal string length of processed elements
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   */
  case class MinStringDFMetricCalculator(metricId: String,
                                         columns: Seq[String]) extends DFMetricCalculator {

    val metricName: MetricName = MetricName.MinString

    /**
     * Minimum string metric calculator should return maximum integer value when DF is empty.
     */
    protected val emptyValue: Column = lit(Int.MaxValue).cast(DoubleType)
    /**
     * Metric error message for cases when all requested column values are nulls for processed rows.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = "Couldn't calculate minimum string length out of provided values."

    /**
     * Determines minimum string length for row values in requested columns.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    protected def resultExpr: Column =
      if (columns.size == 1) length(col(columns.head).cast(StringType))
      else least(columns.map(c => length(col(c).cast(StringType))): _*)

    /**
     * If least function returned null, then it is a signal that all values in
     * requested columns of processed row were nulls. Thus, minimum string length couldn't be
     * calculated. This is a metric increment failure.
     */
    protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * Aggregation function for finding minimum string length is simply `min`
     */
    protected val resultAggregateFunction: Column => Column = min
  }

  /**
   * Calculates maximum string length of processed elements
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   */
  case class MaxStringDFMetricCalculator(metricId: String,
                                         columns: Seq[String]) extends DFMetricCalculator {

    val metricName: MetricName = MetricName.MaxString

    /**
     * Max string metric calculator should return minimum integer value when DF is empty.
     */
    protected val emptyValue: Column = lit(Int.MinValue).cast(DoubleType)
    /**
     * Metric error message for cases when all requested column values are nulls for processed rows.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = "Couldn't calculate maximum string length out of provided values."

    /**
     * Determines maximum string length for row values in requested columns.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    protected def resultExpr: Column =
      if (columns.size == 1) length(col(columns.head).cast(StringType))
      else greatest(columns.map(c => length(col(c).cast(StringType))): _*)

    /**
     * If greatest function returned null, then it is a signal that all values in
     * requested columns of processed row were nulls. Thus, maximum string length couldn't be
     * calculated. This is a metric increment failure.
     */
    protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * Aggregation function for finding maximum string length is simply `max`
     */
    protected val resultAggregateFunction: Column => Column = max
  }

  /**
   * Calculates average string length of processed elements
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   *
   * @note Null values are omitted:
   *       For values: "foo", "bar-buz", null
   *       Metric result would be: (3 + 7) / 2 = 5
   */
  case class AvgStringDFMetricCalculator(metricId: String,
                                         columns: Seq[String]) extends DFMetricCalculator {

    val metricName: MetricName = MetricName.AvgString

    /**
     * Avg string return NaN when DF is empty.
     */
    protected val emptyValue: Column = lit(Double.NaN)

    /**
     * Metric error message for cases when all requested column values are nulls for processed rows.
     *
     * @return Metric increment failure message.
     */
    def errorMessage: String = "Couldn't calculate average string length for provided values."

    /**
     * Spark expression yielding numeric result for processed row.
     * Metric will be incremented with this result using associated aggregation function.
     *
     * @return Spark row-level expression yielding numeric result.
     */
    protected def resultExpr: Column =
      if (columns.size == 1) length(col(columns.head).cast(StringType))
      else columns.map(c => coalesce(length(col(c)), lit(0))).foldLeft(lit(0))(_ + _)

    /**
     * Additional expression to count number of non-null values used to
     * compute average string length at the aggregation stage.
     * @return Spark row-level expression yielding number of non-null values.
     */
    private def countExpr: Column = columns.map { c =>
      when(col(c).isNull, lit(0)).otherwise(lit(1))
    }.foldLeft(lit(0))(_ + _)


    /**
     * If expression summing all string lengths returned null, then it is a signal that all values in
     * requested columns of processed row were nulls. Thus, average string length couldn't be
     * calculated. This is a metric increment failure.
     *
     * @return Spark row-level expression yielding boolean result.
     */
    override protected def errorConditionExpr: Column = resultExpr.isNull

    /**
     * Average string length is aggregated as ration of sum of all string length
     * to number of cells that were processed (excluding null cells).
     */
    override protected val resultAggregateFunction: Column => Column =
      rowRes => sum(rowRes) / sum(countExpr)
  }

  /**
   * Calculates amount of strings in provided date format.
   *
   * @param metricId   Id of the metric.
   * @param columns    Sequence of columns which are used for metric calculation
   * @param dateFormat Requested date format
   * @param reversed   Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class FormattedDateDFMetricCalculator(metricId: String,
                                             columns: Seq[String],
                                             dateFormat: String,
                                             protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    val metricName: MetricName = MetricName.FormattedDate

    /**
     * For direct error collection logic strings values which cannot be cast to date
     * with given format are considered as metric failure.
     * For reversed error collection logic strings values which CAN be cast to date
     * with given format are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Some of the provided values CAN be cast to date with given format of '$dateFormat'."
      else s"Some of the provided values cannot be cast to date with given format of '$dateFormat'."

    /**
     * Create spark expression which checks if column can be converted to
     * date with given format. In this case `to_date` function would
     * yield non-null value.
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column =
      !to_timestamp(col(colName), dateFormat).isNull
  }

  /**
   * Calculates amount of strings with specific requested length.
   *
   * @param metricId      Id of the metric.
   * @param columns       Sequence of columns which are used for metric calculation
   * @param compareLength Requested length
   * @param compareRule   Comparison rule. Could be:
   *                    - "eq" - equals to,
   *                    - "lt" - less than,
   *                    - "lte" - less than or equals to,
   *                    - "gt" - greater than,
   *                    - "gte" - greater than or equals to.
   * @param reversed      Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class StringLengthDFMetricCalculator(metricId: String,
                                            columns: Seq[String],
                                            compareLength: Int,
                                            compareRule: String,
                                            protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    override val metricName: MetricName = MetricName.StringLength

    /**
     * For direct error collection logic strings values which length doesn't meet provided
     * criteria are considered as metric failure.
     * For reversed error collection logic strings values which length DOES meet provided
     * criteria are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"There are values found that DO meet string length criteria '$criteriaStringRepr'"
      else s"There are values found that do not meet string length criteria '$criteriaStringRepr'"


    /**
     * Create spark expression which checks if string length of given column
     * meet provided criteria.
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column = compareRule match {
      case "eq" => length(col(colName)) === lit(compareLength)
      case "lt" => length(col(colName)) < lit(compareLength)
      case "lte" => length(col(colName)) <= lit(compareLength)
      case "gt" => length(col(colName)) > lit(compareLength)
      case "gte" => length(col(colName)) >= lit(compareLength)
    }

    private def criteriaStringRepr: String = compareRule match {
      case "eq" => s"==$compareLength"
      case "lt" => s"<$compareLength"
      case "lte" => s"<=$compareLength"
      case "gt" => s">$compareLength"
      case "gte" => s">=$compareLength"
    }
  }

  /**
   * Calculates amount of strings that are within provided domain
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   * @param domain   Set of strings that represents the requested domain
   * @param reversed Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class StringInDomainDFMetricCalculator(metricId: String,
                                              columns: Seq[String],
                                              domain: Set[String],
                                              protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    override val metricName: MetricName = MetricName.StringInDomain

    /**
     * For direct error collection logic strings values which are outside of
     * provided domain are considered as metric failure.
     * For reversed error collection logic strings values which are within
     * provided domain are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Some of the provided values are IN the given domain of ${domain.mkString("[", ",", "]")}"
      else s"Some of the provided values are not in the given domain of ${domain.mkString("[", ",", "]")}"

    /**
     * Create spark expression which checks if column value is within provided domain.
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column = col(colName).cast(StringType).isInCollection(domain)
  }

  /**
   * Calculates amount of strings that are out of provided domain.
   *
   * @param metricId Id of the metric.
   * @param columns  Sequence of columns which are used for metric calculation
   * @param domain   Set of strings that represents the requested domain
   * @param reversed Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class StringOutDomainDFMetricCalculator(metricId: String,
                                              columns: Seq[String],
                                              domain: Set[String],
                                              protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    override val metricName: MetricName = MetricName.StringOutDomain

    /**
     * For direct error collection logic strings values which are
     * within of provided domain are considered as metric failure.
     *
     * For reversed error collection logic strings values which are
     * outside of provided domain are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Some of the provided values are not in the given domain of ${domain.mkString("[", ",", "]")}"
      else s"Some of the provided values are IN the given domain of ${domain.mkString("[", ",", "]")}"

    /**
     * Create spark expression which checks if column value is outside of the provided domain.
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column = !col(colName).cast(StringType).isInCollection(domain)
  }

  /**
   * Counts number of appearances of requested string in processed elements
   *
   * @param metricId     Id of the metric.
   * @param columns      Sequence of columns which are used for metric calculation
   * @param compareValue Requested string to find
   * @param reversed     Boolean flag indicating whether error collection logic should be reversed for this metric
   */
  case class StringValuesDFMetricCalculator(metricId: String,
                                            columns: Seq[String],
                                            compareValue: String,
                                            protected val reversed: Boolean)
    extends ConditionalDFCalculator {

    override val metricName: MetricName = MetricName.StringValues

    /**
     * For direct error collection logic strings values which are not equal to
     * provided value are considered as metric failure.
     *
     * For reversed error collection logic strings values which are equal to
     * provided value are considered as metric failure.
     *
     * @return Metric increment failure message.
     */
    override def errorMessage: String =
      if (reversed) s"Some of the provided values DO equal to requested string value of '$compareValue'"
      else s"Some of the provided values do not equal to requested string value of '$compareValue'"

    /**
     * Create spark expression which checks if column value is equal to requested value.
     *
     * @param colName Column to which the metric condition is applied
     */
    override def metricCondExpr(colName: String): Column = col(colName).cast(StringType) === lit(compareValue)
  }
}
