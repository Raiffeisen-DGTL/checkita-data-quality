package ru.raiffeisen.checkita.core.metrics.df.functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.{Collect, ImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.collection.compat._
import scala.collection.mutable

@ExpressionDescription(
  usage = """
    _FUNC_(expr[, maxSize]) - Merges lists into a final list of non-unique elements.
      The maximum allowed size of list is limited by `maxSize` parameter.
      If `maxSize` is not defined or non-positive, then size of list is not limited.""",
  examples = """
    Examples:
      > SELECT _FUNC_(col, 4) FROM VALUES ([1, 2]), ([3, 4]), ([5, 6]) AS tab(col);
       [1,2,3,4]
  """,
  note = """
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.
  """,
  group = "agg_funcs"
)
case class MergeListWithLimit(
                               child: Expression,
                               limit: Int = -1,
                               mutableAggBufferOffset: Int = 0,
                               inputAggBufferOffset: Int = 0
                             ) extends Collect[mutable.ArrayBuffer[Any]] {

  def this(child: Expression) = this(child, -1, 0, 0)

  override def prettyName: String = "merge_list_limit"
  override def dataType: DataType = child.dataType
  override protected lazy val bufferElementType: DataType = dataType.asInstanceOf[ArrayType].elementType

  /**
   * As this function merges lists, then input `expr` must have an ArrayType.
   */
  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case ArrayType(_, _) => TypeCheckResult.TypeCheckSuccess
    case _ =>
      val msg = s"argument 1 requires array<datatype> type, " +
        s"however, '${child.sql}' is of ${child.dataType.catalogString} type."
      TypeCheckResult.TypeCheckFailure(msg)
  }

  override protected def convertToBufferElement(value: Any): mutable.ArrayBuffer[Any] = {
    val unsafeArray = InternalRow.copyValue(value).asInstanceOf[UnsafeArrayData]
    unsafeArray.toArray[Any](bufferElementType).to(mutable.ArrayBuffer)
  }

  override def createAggregationBuffer(): mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  override def eval(buffer: mutable.ArrayBuffer[Any]): Any = new GenericArrayData(buffer.toArray)

  override def update(buffer: mutable.ArrayBuffer[Any], input: InternalRow): mutable.ArrayBuffer[Any] = {
    val value = child.eval(input)
    if (value != null) {
      merge(buffer, convertToBufferElement(value))
    } else buffer
  }

  override def merge(buffer: mutable.ArrayBuffer[Any], other: mutable.ArrayBuffer[Any]): mutable.ArrayBuffer[Any] = {
    if (limit <= 0) buffer ++= other
    else if (buffer.size >= limit) buffer
    else if (other.size >= limit) other
    else (buffer ++= other).take(limit)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
