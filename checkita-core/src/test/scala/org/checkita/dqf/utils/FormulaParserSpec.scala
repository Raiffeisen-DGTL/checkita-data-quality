package org.checkita.dqf.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FormulaParserSpec extends AnyWordSpec with Matchers with FormulaParser {
  
  private val checkArithmetic: (String, Double) => Unit = (f, r) => evalArithmetic(f) shouldEqual r
  private val checkBoolean: (String, Boolean) => Unit = (f, r) => evalBoolean(f) shouldEqual r
  
  "Formula parser" must {
    "correctly parse valid arithmetic expressions" in {
      Seq(
        "- 1.31" -> -1.31,
        "9.99" -> 9.99,
        "2.33 + 4.12 * 0.81" -> (2.33 + 4.12 * 0.81),
        "8.31 - 10.55 / 3.09" -> (8.31 - 10.55 / 3.09),
        "3.14 * (2.77 - 1.31) ^ 0.5 + 0.99" -> (3.14 * math.pow(2.77 - 1.31, 0.5) + 0.99),
        "3.14 * ln((2.77 - 1.31) ^ 0.5 + 5.55) + 0.99" -> (3.14 * math.log(math.pow(2.77 - 1.31, 0.5) + 5.55) + 0.99),
        "-3.14 * ln(max((2.77 - 1.31) ^ 0.5, - 5.55)) + 0.99" -> (-3.14 * math.log(math.max(math.pow(2.77 - 1.31, 0.5), - 5.55)) + 0.99)
      ).foreach{ case (f, r) => checkArithmetic(f, r) }
    }
    "correctly parse boolean expressions" in {
      Seq(
        "true" -> true,
        "false" -> false,
        "false || true && false" -> false,
        "(1 < 0) || (3.14 * (2.77 - 1.31) ^ 0.5 > 3.5) && not(-3.14 * ln(max((2.77 - 1.31) ^ 0.5, - 5.55)) + 0.99 < 0)" -> true
      ).foreach{ case (f, r) => checkBoolean(f, r) }
    }
    "correctly parse case-when expressions" in {
      Seq(
        "CASE WHEN 1 > 0 THEN 777.7 ELSE 0 END" -> 777.7,
        "case when 1 < 0 then 777.7 when 2 > 1 then 17 else 0 end" -> 17.0,
        "case when 1 < 0 THEN 777.7 else 0 END" -> 0.0
      ).foreach { case (f, r) => checkArithmetic(f, r) }
    }

    "correctly parse if-else expressions" in {
      Seq(
        "IF 1 > 0 777.7 ELSE 0" -> 777.7,
        "if 1 < 0 777.7 else 0" -> 0.0,
        "if (3.14 > 2.0) (2.71 * 2) ELSE (1.41 + 0.5)" -> (2.71 * 2)
      ).foreach { case (f, r) => checkArithmetic(f, r) }
    }
    "throw error when arithmetic expression is invalid" in {
      Seq(
        "sin(3.14)",
        "3.14 || 9.99",
        "-3.14 * ln(max((2.77 - 1.31) ^ 0.5, - 5.55)) >= 0.99"
      ).foreach(f => an [RuntimeException] should be thrownBy evalArithmetic(f))
    }
    "throw error when boolean expression is invalid" in {
      Seq(
        "9.99",
        "3.14 || true",
        "2.3 >= 3.4 >= 4.3",
        "(1.41 > 0) == (-3.14 < 0)"
      ).foreach(f => an [RuntimeException] should be thrownBy evalBoolean(f))
    }
    "throw error when case-when expression is invalid" in {
      Seq(
        "case when true then 3.14 else true end"
      ).foreach(f => an[RuntimeException] should be thrownBy evalArithmetic(f))
    }

    "throw error when if-else expression is invalid" in {
      Seq(
        "IF 1 3.14 ELSE 0",
        "if true 3.14 else true"
      ).foreach(f => an[RuntimeException] should be thrownBy evalArithmetic(f))
    }
  }
}
