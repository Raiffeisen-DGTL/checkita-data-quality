package org.checkita.dqf.core.metrics.df

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, sum, when}
import org.apache.spark.sql.types.{DataType, DoubleType}

/**
 * Abstract class for all conditional DF metric calculators.
 * Thus, conditional calculator has a condition defined.
 * When this condition is met for particular column value,
 * then metric value is incremented by one.
 * Otherwise metric value remain unchanged.
 *
 * All conditional metrics are reversible: direct error collection logic
 * implies metric increment fails when condition is not met.
 * Correspondingly, for reversed error collection logic, metric increment
 * fails when condition IS met.
 */
abstract class ConditionalDFCalculator extends DFMetricCalculator with ReversibleDFCalculator {

  /**
   * Create spark expression which applies metric condition to provided column
   * and will yield boolean result.
   *
   * @param colName Column to which the metric condition is applied
   * @param colTypes Map of column names to their datatype.
   */
  protected def metricCondExpr(colName: String)(implicit colTypes: Map[String, DataType]): Column

  /**
   * All conditional metrics should return zero when DF is empty.
   */
  protected val emptyValue: Column = lit(0).cast(DoubleType)

  /**
   * Spark expression yielding numeric result for processed row.
   * For conditional metrics, the increment is 1 when condition is met,
   * otherwise increment is 0 (metric is not incremented).
   */
  protected def resultExpr(implicit colTypes: Map[String, DataType]): Column = columns.map { c =>
    when(metricCondExpr(c), lit(1)).otherwise(lit(0))
  }.foldLeft(lit(0))(_ + _)

  /**
   * For direct error collection logic metric increment is considered failed
   * when for one or more of metric columns the condition is not met.
   * For reversed error collection logic metric increment is considered failed
   * when for one or more of metric columns the condition IS met.
   */
  protected def errorConditionExpr(implicit colTypes: Map[String, DataType]): Column =
    if (reversed) resultExpr > 0 else resultExpr < lit(columns.size)

  /**
   * Aggregation function for all conditional metrics is just a summation.
   */
  protected val resultAggregateFunction: Column => Column = sum
}
