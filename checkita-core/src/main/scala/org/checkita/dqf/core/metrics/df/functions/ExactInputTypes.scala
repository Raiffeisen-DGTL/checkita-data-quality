package org.checkita.dqf.core.metrics.df.functions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

trait ExactInputTypes { this: Expression =>

  def exactInputTypes: Seq[(Expression, DataType)]

  override def checkInputDataTypes(): TypeCheckResult = {
    val typeMismatch = exactInputTypes.zipWithIndex.collectFirst {
      case ((input, expectedType), idx) if expectedType != input.dataType => 
        val msg = s"argument ${idx + 1} requires ${expectedType.simpleString} type, " +
          s"however, '${input.sql}' is of ${input.dataType.catalogString} type."
        TypeCheckResult.TypeCheckFailure(msg)
    }
    typeMismatch.getOrElse(TypeCheckResult.TypeCheckSuccess)
  }
}
