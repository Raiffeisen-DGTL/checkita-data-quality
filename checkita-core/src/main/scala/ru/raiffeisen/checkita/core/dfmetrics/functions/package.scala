package ru.raiffeisen.checkita.core.dfmetrics

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

package object functions {
  private def withAggregateFunction(
                                     func: AggregateFunction,
                                     isDistinct: Boolean = false): Column = {
    new Column(func.toAggregateExpression(isDistinct))
  }

  def collect_list_limit(e: Column, limit: Int): Column = withAggregateFunction(CollectListWithLimit(e.expr, limit))
  def collect_list_limit(e: Column): Column = withAggregateFunction(CollectListWithLimit(e.expr))
  def collect_list_limit(columnName: String, limit: Int): Column = collect_list_limit(new Column(columnName), limit)
  def collect_list_limit(columnName: String): Column = collect_list_limit(new Column(columnName))
}
