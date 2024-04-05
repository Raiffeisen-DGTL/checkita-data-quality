package ru.raiffeisen.checkita.core.metrics.df.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

/**
 * Column-oriented instantiation of custom spark function.
 */
object api {

  private def withAggregateFunction(func: AggregateFunction, isDistinct: Boolean = false): Column = {
    new Column(func.toAggregateExpression(isDistinct))
  }

  def collect_list_limit(e: Column, limit: Int): Column = withAggregateFunction(CollectListWithLimit(e.expr, limit))
  def collect_list_limit(e: Column): Column             = withAggregateFunction(CollectListWithLimit(e.expr))
  def collect_list_limit(columnName: String, limit: Int): Column = collect_list_limit(new Column(columnName), limit)
  def collect_list_limit(columnName: String): Column             = collect_list_limit(new Column(columnName))
}
