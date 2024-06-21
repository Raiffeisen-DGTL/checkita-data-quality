package ru.raiffeisen.checkita.utils

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Recursively creates a tree of operations and executes it.
 */
trait FormulaParser extends JavaTokenParsers {

  sealed abstract class Tree

  case class UnaryFunc(t1: Tree, f: Double => Double) extends Tree
  case class BinaryFunc(t1: Tree, t2: Tree, f: (Double, Double) => Double) extends Tree
  case class Num(t: Double) extends Tree

  case class BoolUnaryFunc(t1: Tree, f: Boolean => Boolean) extends Tree
  case class BoolBinaryFunc(t1: Tree, t2: Tree, f: (Boolean, Boolean) => Boolean) extends Tree
  case class BoolCompareFunc(t1: Tree, t2: Tree, f: (Double, Double) => Boolean) extends Tree
  case class Bool(t: Boolean) extends Tree

  /**
   * API to parse and evaluate arithmetic expression.
   * @param formula Arithmetic expression to parse
   * @return Double evaluation result.
   */
  def evalArithmetic(formula: String): Double = {
    val tree = this.parseAll(expr, formula).get
    this.eval(tree)
  }

  /**
   * API to parse and evaluate boolean expression.
   *
   * @param formula Boolean expression to parse
   * @return Boolean evaluation result.
   */
  def evalBoolean(formula: String): Boolean = {
    val tree = this.parseAll(boolExpr, formula).get
    this.boolEval(tree)
  }

  /**
   * Evaluates tree of arithmetic operations. 
   * Boolean operators are not supported here!
   *
   * @param t Tree to evaluate
   * @return Evaluation result (double).
   */
  private def eval(t: Tree): Double = t match {
    case UnaryFunc(t1, f) => f(eval(t1))
    case BinaryFunc(t1, t2, f) => f(eval(t1), eval(t2))
    case Num(t) => t
    case other => throw new MatchError(s"Illegal operation for arithmetic expression: ${other.getClass.getSimpleName}")
  }

  /**
   * Evaluates tree of boolean operations. 
   * Arithmetic operations are evaluated only if boolean comparison is presented.
   *
   * @param t Tree to evaluate
   * @return Evaluation result (boolean)
   */
  private def boolEval(t: Tree): Boolean = t match {
    case BoolUnaryFunc(t, f) => f(boolEval(t))
    case BoolBinaryFunc(t1, t2, f) => f(boolEval(t1), boolEval(t2))
    case BoolCompareFunc(t1, t2, f) => f(eval(t1), eval(t2))
    case Bool(b) => b
    case other => throw new MatchError(s"Illegal operation for boolean expression: ${other.getClass.getSimpleName}")
  }

  /**
   * Supported unary arithmetic functions.
   */
  private val unaryOps: Map[String, Double => Double] = Map(
    "abs" -> math.abs,
    "sqrt" -> math.sqrt,
    "floor" -> math.floor,
    "ceil" -> math.ceil,
    "round" -> (d => math.round(d).toDouble),
    "ln" -> math.log,
    "lg" -> math.log10,
    "exp" -> math.exp
  )

  /**
   * Supported binary arithmetic operators and functions.
   */
  private val binaryOps: Map[String, (Double, Double) => Double] = Map(
    "+" -> ((a, b) => a + b),
    "-" -> ((a, b) => a - b),
    "*" -> ((a, b) => a * b),
    "/" -> ((a, b) => a / b),
    "^" -> ((a, b) => math.pow(a, b)),
    "max" -> math.max,
    "min" -> math.min
  )

  /**
   * Supported logical unary operators.
   */
  private val boolUnaryOps: Map[String, Boolean => Boolean] = Map(
    "not" -> (b => !b)
  )

  /**
   * Supported logical binary operators.
   */
  private val boolBinaryOps: Map[String, (Boolean, Boolean) => Boolean] = Map(
    "&&" -> ((a, b) => a && b),
    "||" -> ((a, b) => a || b)
  )

  /**
   * Supported comparison operators.
   */
  private val boolCompareOps: Map[String, (Double, Double) => Boolean] = Map(
    "==" -> ((a, b) => a == b),
    "<>" -> ((a, b) => a != b),
    ">=" -> ((a, b) => a >= b),
    "<=" -> ((a, b) => a <= b),
    ">" -> ((a, b) => a > b),
    "<" -> ((a, b) => a < b)
  )

  private lazy val boolExpr: Parser[Tree] = boolTerm ~ rep("||" ~> boolTerm) ^^ {
    case t ~ ts => ts.foldLeft(t) {
      case (t1, t2) => BoolBinaryFunc(t1, t2, boolBinaryOps("||"))
    }
  }

  private lazy val boolTerm: Parser[Tree] = boolFactor ~ rep("&&" ~> boolFactor) ^^ {
    case t ~ ts => ts.foldLeft(t) {
      case (t1, t2) => BoolBinaryFunc(t1, t2, boolBinaryOps("&&"))
    }
  }

  private lazy val boolFactor: Parser[Tree] = unaryBool | "(" ~> boolExpr <~ ")" | "(" ~> boolCompare <~ ")" | boolCompare | bool

  private lazy val unaryBool: Parser[Tree] = "[a-zA-Z]+".r ~ ("(" ~> boolExpr <~ ")") ^^ {
    case f ~ t => boolUnaryOps.get(f.toLowerCase).map(BoolUnaryFunc(t, _)).getOrElse(
      throw new MatchError(s"Illegal boolean unary function name $f")
    )
  }

  private lazy val boolCompare: Parser[Tree] = expr ~ ("==" | "<>" | ">=" | "<=" | ">" | "<") ~ expr ^^ {
    case t1 ~ f ~ t2 => boolCompareOps.get(f).map(BoolCompareFunc(t1, t2, _)).getOrElse(
      throw new MatchError(s"Illegal comparison operator token $f")
    )
  }

  private lazy val bool: Parser[Bool] = "true" ^^ (_ => Bool(true)) | "false" ^^ (_ => Bool(false))

  private lazy val expr: Parser[Tree] = opt("[+-]".r) ~ term ~ rep("[+-]".r ~ term) ^^ {
    case s ~ t ~ ts =>
      val signed = s match {
        case None => t
        case Some("+") => t
        case Some("-") => BinaryFunc(Num(-1), t, binaryOps("*"))
        case Some(e) => throw new MatchError(s"Illegal sign token $e")
      }
      ts.foldLeft(signed) {
        case (t1, "+" ~ t2) => BinaryFunc(t1, t2, binaryOps("+"))
        case (t1, "-" ~ t2) => BinaryFunc(t1, t2, binaryOps("-"))
        case (_, t ~ _) => throw new MatchError(s"Illegal token '$t'")
      }
  }

  private lazy val term: Parser[Tree] = power ~ rep("[*/]".r ~ power) ^^ {
    case t ~ ts =>
      ts.foldLeft(t) {
        case (t1, "*" ~ t2) => BinaryFunc(t1, t2, binaryOps("*"))
        case (t1, "/" ~ t2) => BinaryFunc(t1, t2, binaryOps("/"))
        case (_, t ~ _) => throw new MatchError(s"Illegal token '$t'")
      }
  }

  private lazy val power: Parser[Tree] = factor ~ rep("['^]".r ~ factor) ^^ {
    case t ~ ts =>
      ts.foldLeft(t) {
        case (t1, "^" ~ t2) => BinaryFunc(t1, t2, binaryOps("^"))
        case (_, t ~ _) => throw new MatchError(s"Illegal token '$t'")
      }
  }

  private lazy val factor: Parser[Tree] = binary | unary | "(" ~> expr <~ ")" | num

  private lazy val unary: Parser[Tree] = "[a-zA-Z]+".r ~ ("(" ~> expr <~ ")") ^^ {
    case f ~ t => unaryOps.get(f.toLowerCase).map(UnaryFunc(t, _)).getOrElse(
      throw new MatchError(s"Illegal unary function name $f")
    )
  }

  private lazy val binary: Parser[Tree] = "[a-zA-Z]+".r ~ ("(" ~> expr) ~ ("," ~> expr) <~ ")" ^^ {
    case f ~ t1 ~ t2 => binaryOps.get(f.toLowerCase).map(BinaryFunc(t1, t2, _)).getOrElse(
      throw new MatchError(s"Illegal binary function name $f")
    )
  }

  private lazy val num: Parser[Num] = floatingPointNumber ^^ (t => Num(t.toDouble))
}
