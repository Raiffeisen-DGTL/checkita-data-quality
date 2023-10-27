package ru.raiffeisen.checkita.utils

import scala.math.pow
import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Recursively creates a tree of operations and executes it
 */
trait FormulaParser extends JavaTokenParsers {

  sealed abstract class Tree
  case class Add(t1: Tree, t2: Tree) extends Tree
  case class Sub(t1: Tree, t2: Tree) extends Tree
  case class Mul(t1: Tree, t2: Tree) extends Tree
  case class Div(t1: Tree, t2: Tree) extends Tree
  case class Pow(t1: Tree, t2: Tree) extends Tree
  case class Num(t: Double) extends Tree

  def eval(t: Tree): Double = t match {
    case Add(t1, t2) => eval(t1) + eval(t2)
    case Sub(t1, t2) => eval(t1) - eval(t2)
    case Mul(t1, t2) => eval(t1) * eval(t2)
    case Div(t1, t2) => eval(t1) / eval(t2)
    case Pow(t1, t2) => pow(eval(t1), eval(t2))
    case Num(t) => t
  }

  lazy val expr: Parser[Tree] = term ~ rep("[+-]".r ~ term) ^^ {
    case t ~ ts =>
      ts.foldLeft(t) {
        case (t1, "+" ~ t2) => Add(t1, t2)
        case (t1, "-" ~ t2) => Sub(t1, t2)
        case (_, t ~ _) => throw new MatchError(s"Illegal token '$t'")
      }
  }

  lazy val term: Parser[Tree] = power ~ rep("[*/]".r ~ power) ^^ {
    case t ~ ts =>
      ts.foldLeft(t) {
        case (t1, "*" ~ t2) => Mul(t1, t2)
        case (t1, "/" ~ t2) => Div(t1, t2)
        case (_, t ~ _) => throw new MatchError(s"Illegal token '$t'")
      }
  }

  lazy val power: Parser[Tree] = factor ~ rep("['^]".r ~ factor) ^^ {
    case t ~ ts =>
      ts.foldLeft(t) {
        case (t1, "^" ~ t2) => Pow(t1, t2)
        case (_, t ~ _) => throw new MatchError(s"Illegal token '$t'")
      }
  }

  lazy val factor: Parser[Tree] = "(" ~> expr <~ ")" | num

  lazy val num: Parser[Num] = floatingPointNumber ^^ { t =>
    Num(t.toDouble)
  }
}
