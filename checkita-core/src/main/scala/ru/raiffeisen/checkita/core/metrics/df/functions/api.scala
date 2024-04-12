package ru.raiffeisen.checkita.core.metrics.df.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

/**
 * Column-oriented instantiation of custom spark function.
 */
object api {

  private def withAggregateFunction(func: AggregateFunction, isDistinct: Boolean = false): Column = {
    new Column(func.toAggregateExpression(isDistinct))
  }

  private def withExpr(expr: Expression): Column = new Column(expr)

  def collect_list_limit(c: Column, limit: Int): Column = withAggregateFunction(CollectListWithLimit(c.expr, limit))
  def collect_list_limit(c: Column): Column = withAggregateFunction(CollectListWithLimit(c.expr))
  def collect_list_limit(columnName: String, limit: Int): Column = collect_list_limit(new Column(columnName), limit)
  def collect_list_limit(columnName: String): Column = collect_list_limit(new Column(columnName))

  def merge_list_limit(c: Column, limit: Int): Column = withAggregateFunction(MergeListWithLimit(c.expr, limit))
  def merge_list_limit(c: Column): Column = withAggregateFunction(MergeListWithLimit(c.expr))
  def merge_list_limit(columnName: String, limit: Int): Column = merge_list_limit(new Column(columnName), limit)
  def merge_list_limit(columnName: String): Column = merge_list_limit(new Column(columnName))

  def check_number_format(number: Column, precision: Column, scale: Column, isOutbound: Column): Column =
    withExpr(CheckNumberFormat(number.expr, precision.expr, scale.expr, isOutbound.expr))
  def check_number_format(number: Column, precision: Int, scale: Int, isOutbound: Boolean): Column =
    check_number_format(
      number,
      new Column(Literal(precision)),
      new Column(Literal(scale)),
      new Column(Literal(isOutbound))
    )
  def check_number_format(number: String, precision: Int, scale: Int, isOutbound: Boolean): Column =
    check_number_format(
      new Column(number),
      new Column(Literal(precision)),
      new Column(Literal(scale)),
      new Column(Literal(isOutbound))
    )

  def tdigest_percentile(c: Column,
                         percentage: Column,
                         accuracy: Column = new Column(Literal(0.005)),
                         direct: Column = new Column(Literal(false))): Column =
    withAggregateFunction(TDigestPercentile(c.expr, percentage.expr, accuracy.expr, direct.expr))

  def tdigest_percentile(c: Column, percentage: Double, accuracy: Double, direct: Boolean): Column = tdigest_percentile(
    c, new Column(Literal(percentage)), new Column(Literal(accuracy)), new Column(Literal(direct))
  )

  def tdigest_percentile(c: Column, percentage: Double, accuracy: Double): Column = tdigest_percentile(
    c, new Column(Literal(percentage)), new Column(Literal(accuracy))
  )

  def tdigest_percentile(c: Column, percentage: Double, direct: Boolean): Column = tdigest_percentile(
    c, new Column(Literal(percentage)), direct = new Column(Literal(direct))
  )

  def tdigest_percentile(c: Column, percentage: Double): Column = tdigest_percentile(c, new Column(Literal(percentage)))

  def tdigest_percentile(colName: String, percentage: Double, accuracy: Double, direct: Boolean): Column =
    tdigest_percentile(new Column(colName), percentage, accuracy, direct)
  def tdigest_percentile(colName: String, percentage: Double, accuracy: Double): Column =
    tdigest_percentile(new Column(colName), percentage, accuracy)
  def tdigest_percentile(colName: String, percentage: Double, direct: Boolean): Column =
    tdigest_percentile(new Column(colName), percentage, direct)
  def tdigest_percentile(colName: String, percentage: Double): Column =
    tdigest_percentile(new Column(colName), percentage)

  def hll_count_distinct(c: Column, accuracy: Column): Column =
    withAggregateFunction(HLLCountDistinct(c.expr, accuracy.expr))
  def hll_count_distinct(c: Column): Column = hll_count_distinct(c, new Column(Literal(0.01)))
  def hll_count_distinct(c: Column, accuracy: Double): Column = hll_count_distinct(c, new Column(Literal(accuracy)))
  def hll_count_distinct(colName: String, accuracy: Double): Column =
    hll_count_distinct(new Column(colName), new Column(Literal(accuracy)))
  def hll_count_distinct(colName: String): Column = hll_count_distinct(new Column(colName))

  def comoment(c1: Column, c2: Column): Column = withAggregateFunction(CoMoment(c1.expr, c2.expr))
  def comoment(colName1: String, colName2: String): Column = comoment(new Column(colName1), new Column(colName2))

  def space_saving_top_n(c: Column, targetNumber: Column, maxCapacity: Column): Column = withAggregateFunction(
    SpaceSavingTopN(c.expr, targetNumber.expr, maxCapacity.expr)
  )
  def space_saving_top_n(c: Column, targetNumber: Column): Column =
    space_saving_top_n(c, targetNumber, new Column(Literal(100)))
  def space_saving_top_n(c: Column, targetNumber: Int, maxCapacity: Int): Column =
    space_saving_top_n(c, new Column(Literal(targetNumber)), new Column(Literal(maxCapacity)))
  def space_saving_top_n(colName: String, targetNumber: Int, maxCapacity: Int): Column =
    space_saving_top_n(new Column(colName), new Column(Literal(targetNumber)), new Column(Literal(maxCapacity)))
  def space_saving_top_n(c: Column, targetNumber: Int): Column =
    space_saving_top_n(c, new Column(Literal(targetNumber)))
  def space_saving_top_n(colName: String, targetNumber: Int): Column =
    space_saving_top_n(new Column(colName), new Column(Literal(targetNumber)))



}
