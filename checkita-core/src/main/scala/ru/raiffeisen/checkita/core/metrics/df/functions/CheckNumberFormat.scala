package ru.raiffeisen.checkita.core.metrics.df.functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Predicate}
import org.apache.spark.sql.catalyst.trees.QuaternaryLike
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType}

@ExpressionDescription(
  usage = """
    _FUNC_(expr, precision, scale[, isOutbound]) - Returns number format validation result:
      if number meets required precision and scale limitation then returns `true`, otherwise returns `false`.
      It is possible to check if number format is outside of the provided precision and scale limits by settings
      `isOutbound` boolean flag to `true` (default is `false`).
      Input expression `expr` must be of DoubleType. `precision` and `scale` must be of IntegerType.
      `isOutbound` must be boolean (if defined).
   """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, 0,5) FROM VALUES (0), (3), (8), (4), (0), (5), (5), (8), (9), (3), (2), (2), (6), (2), (6) AS tab(col);
       (true), (true), (false)
      > SELECT _FUNC_(col, 3, 2, true) FROM VALUES (43.141), (2.1), (NULL) AS tab(col);
       (true), (false), (false)
  """,
  group = "predicate_funcs"
)
case class CheckNumberFormat(
                              number: Expression,
                              precision: Expression,
                              scale: Expression,
                              isOutbound: Expression
                            ) extends Predicate with QuaternaryLike[Expression] with ExactInputTypes {

  override def prettyName: String = "check_number_format"
  override def nullable: Boolean = false

  override def first: Expression = number
  override def second: Expression = precision
  override def third: Expression = scale
  override def fourth: Expression = isOutbound

  override def exactInputTypes: Seq[(Expression, DataType)] = Seq(
    (number, DoubleType),
    (precision, IntegerType),
    (scale, IntegerType),
    (isOutbound, BooleanType)
  )

  private def nullSafeEval(number: Double, precision: Int, scale: Int, isOutbound: Boolean): Boolean =
    if (precision > 0 && scale >= 0) {
      val bigD = BigDecimal.valueOf(number)
      if (isOutbound) bigD.precision > precision && bigD.scale > scale
      else bigD.precision <= precision && bigD.scale <= scale
    } else false

  override def eval(input: InternalRow): Any = {
    val evaluated = Seq(number, precision, scale, isOutbound).map(_.eval(input))
    if (evaluated.contains(null)) false
    else evaluated match {
      case Seq(number, precision, scale, isOutbound) => nullSafeEval(
        number.asInstanceOf[Double],
        precision.asInstanceOf[Int],
        scale.asInstanceOf[Int],
        isOutbound.asInstanceOf[Boolean]
      )
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val numberGen = number.genCode(ctx)
    val precisionGen = precision.genCode(ctx)
    val scaleGen = scale.genCode(ctx)
    val isOutboundGen = isOutbound.genCode(ctx)

    val tmpResultArg = ctx.freshName("tmpResult")
    val numberArg= ctx.freshName("number")
    val precisionArg = ctx.freshName("precision")
    val scaleArg = ctx.freshName("scale")
    val isOutboundArg = ctx.freshName("isOutbound")

    val code =code"""
      ${numberGen.code}
      ${precisionGen.code}
      ${scaleGen.code}
      ${isOutboundGen.code}
      boolean $tmpResultArg = false;
      if (!(${numberGen.isNull} || ${precisionGen.isNull} || ${scaleGen.isNull} || ${isOutboundGen.isNull})) {
        int $precisionArg = ${precisionGen.value};
        int $scaleArg = ${scaleGen.value};
        boolean $isOutboundArg = ${isOutboundGen.value};
        if ($precisionArg > 0 && $scaleArg >= 0) {
          java.math.BigDecimal $numberArg = java.math.BigDecimal.valueOf(${numberGen.value});
          if ($isOutboundArg) {
              $tmpResultArg = $numberArg.precision() > $precisionArg && $numberArg.scale() > $scaleArg;
          } else {
              $tmpResultArg = $numberArg.precision() <= $precisionArg && $numberArg.scale() <= $scaleArg;
          }
        }
      }
      final boolean ${ev.value} = $tmpResultArg;
    """

    ev.copy(code = code, isNull = FalseLiteral)
  }

  override protected def withNewChildrenInternal(newFirst: Expression,
                                                 newSecond: Expression,
                                                 newThird: Expression,
                                                 newFourth: Expression): Expression =
    copy(number = newFirst, precision = newSecond, scale = newThird, isOutbound = newFourth)
}
