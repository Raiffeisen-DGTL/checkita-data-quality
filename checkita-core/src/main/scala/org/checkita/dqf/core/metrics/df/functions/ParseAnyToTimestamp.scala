package org.checkita.dqf.core.metrics.df.functions

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExpressionDescription, GetTimestamp, ImplicitCastInputTypes, ParseToTimestamp, RuntimeReplaceable, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Modified version of Spark `ParseToTimestamp` function to support inputs of any type.
 * 
 * Tries to cast a column of any type to a timestamp based on the supplied format.
 * If column is of type other than supported by Spark `ParseToTimestamp` function, 
 * then it first is cast to StingType.
 * Supported types for Spark `ParseToTimestamp` function are:
 *   - StringType
 *   - DateType, 
 *   - TimestampType
 *   - TimestampNTZType
 */
@ExpressionDescription(
  usage = """
    _FUNC_(timestamp_str[, fmt]) - Parses the `timestamp_str` expression with the `fmt` expression
      to a timestamp. Returns null with invalid input. By default, it follows casting rules to
      a timestamp if the `fmt` is omitted. The result data type is consistent with the value of
      configuration `spark.sql.timestampType`.
  """,
  arguments = """
    Arguments:
      * timestamp_str - A string to be parsed to timestamp.
      * fmt - Timestamp format pattern to follow. See <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a> for valid
              date and time format patterns.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('2016-12-31 00:12:00');
       2016-12-31 00:12:00
      > SELECT _FUNC_('2016-12-31', 'yyyy-MM-dd');
       2016-12-31 00:00:00
  """,
  group = "datetime_funcs"
)
// scalastyle:on line.size.limit
case class ParseAnyToTimestamp(
                                left: Expression,
                                format: Option[Expression],
                                override val dataType: DataType,
                                timeZoneId: Option[String] = None,
                                failOnError: Boolean = SQLConf.get.ansiEnabled
                              ) extends RuntimeReplaceable with TimeZoneAwareExpression{

  def this(left: Expression, format: Expression) = {
    this(left, Option(format), SQLConf.get.timestampType)
  }

  def this(left: Expression) =
    this(left, None, SQLConf.get.timestampType)

  override def nodeName: String = "any_to_timestamp"
  override def nodePatternsInternal(): Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)
  
  private val nonCastTypes: Seq[DataType] = Seq(StringType, DateType, TimestampType, TimestampNTZType)

  override def replacement: Expression = {
    val expr = if (nonCastTypes.contains(left.dataType)) left
      else Cast(left, StringType, ansiEnabled = SQLConf.get.ansiEnabled)
    format.map(f => GetTimestamp(expr, f, dataType, timeZoneId, failOnError = failOnError))
      .getOrElse(Cast(left, dataType, timeZoneId, ansiEnabled = failOnError))
  }

  override def children: Seq[Expression] = left +: format.toSeq

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    if (format.isDefined) {
      copy(left = newChildren.head, format = Some(newChildren.last))
    } else {
      copy(left = newChildren.head)
    }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = copy(timeZoneId = Some(timeZoneId))
}
