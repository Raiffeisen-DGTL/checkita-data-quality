package org.checkita.dqf.core.metrics.df.functions

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, If, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.Covariance
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DoubleType

@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns the population co-moment of a set of number pairs.",
  examples = """
    Examples:
      > SELECT _FUNC_(c1, c2) FROM VALUES (1,1), (2,2), (3,3) AS tab(c1, c2);
       2.0
  """,
  group = "agg_funcs",
)
case class CoMoment(
                     override val left: Expression,
                     override val right: Expression,
                     nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate)
  extends Covariance(left, right, nullOnDivideByZero) {

  def this(left: Expression, right: Expression) =
    this(left, right, !SQLConf.get.legacyStatisticalAggregate)

  override val evaluateExpression: Expression = {
    If(n === 0.0, Literal.create(null, DoubleType), ck)
  }
  override def prettyName: String = "comoment"

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): CoMoment =
    copy(left = newLeft, right = newRight)
}
