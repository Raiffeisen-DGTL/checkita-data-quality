package org.checkita.dqf.config

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.config.Parsers._


class ParsersSpec extends AnyWordSpec with Matchers {

  "ExpressionParsingOps" must {
    "yield valid list of columns for allowed simple SQL expressions" in {
      val testExpressions: Seq[(Column, Set[String])] = Seq(
        expr("partition_column like '2024-%'") -> Set("partition_column"),
        expr("partition_column = 3.14") -> Set("partition_column"),
        expr("partition_column in ('one', 'two', 'three')") -> Set("partition_column"),
        expr("partition_column = last_part('some_schema.some_table')") -> Set("partition_column"),
        expr("partition_column = date_format(dt_column, 'yyyy-MM-dd')") -> Set("partition_column", "dt_column"),
        expr(
          "partition_column in (last_part('some.table'), 'part_two', date_format(dt_column, 'yyyy-MM-dd'))"
        ) -> Set("partition_column", "dt_column"),
        expr(
          "partition_column >= '2024-01-01' and partition_column < current_date() and partition_column != null"
        ) -> Set("partition_column"),
        expr(
          """
            |partition_column = CASE
            |  WHEN dayofweek(current_date()) = 1 THEN date_add(current_date(), -2)
            |  WHEN dayofweek(current_date()) = 2 THEN date_add(current_date(), -3)
            |  ELSE date_add(current_date(), -1)
            |END
            |""".stripMargin) -> Set("partition_column"),
        expr("partition_column = max(dt_column)") -> Set("partition_column", "dt_column"),
        expr("partition_column >= '2024-01-01' and true") -> Set("partition_column"),
      )

      testExpressions.foreach {
        case (expression, columns) => expression.dependentColumns.toSet shouldEqual columns
      }
    }

    "throw exception when SQL expression includes sub-query" in {
      an [IllegalArgumentException] should be thrownBy expr(
        "partition_column in (select max(dt_column) from some.table)"
      ).dependentColumns
    }

    "return invalid list of columns when using parameterless functions without parentheses" in {
      val testExpressions: Seq[(Column, Set[String])] = Seq(
        expr(
          "partition_column >= '2024-01-01' and partition_column < current_date and partition_column != null"
        ) -> Set("partition_column", "current_date"),
        expr(
          """
            |partition_column = CASE
            |  WHEN dayofweek(current_date) = 1 THEN date_add(current_date, -2)
            |  WHEN dayofweek(current_date) = 2 THEN date_add(current_date, -3)
            |  ELSE date_add(current_date, -1)
            |END
            |""".stripMargin) -> Set("partition_column", "current_date")
      )

      testExpressions.foreach {
        case (expression, columns) => expression.dependentColumns.toSet shouldEqual columns
      }
    }
  }
}
