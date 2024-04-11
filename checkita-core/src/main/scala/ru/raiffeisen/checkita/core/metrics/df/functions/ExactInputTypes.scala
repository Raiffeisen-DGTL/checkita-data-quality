package ru.raiffeisen.checkita.core.metrics.df.functions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes.{toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

trait ExactInputTypes { this: Expression =>

  def exactInputTypes: Seq[(Expression, DataType)]

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeMismatch = exactInputTypes.zipWithIndex.collectFirst {
      case ((input, expectedType), idx) if expectedType != input.dataType => DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> idx.toString,
          "requiredType" -> toSQLType(expectedType),
          "inputSql" -> toSQLExpr(input),
          "inputType" -> toSQLType(input.dataType)
        )
      )
    }
    typeMismatch.getOrElse(TypeCheckResult.TypeCheckSuccess)
  }
}
