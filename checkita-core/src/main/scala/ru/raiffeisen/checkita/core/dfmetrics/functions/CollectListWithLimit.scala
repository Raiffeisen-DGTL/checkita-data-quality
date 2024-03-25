package ru.raiffeisen.checkita.core.dfmetrics.functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Collect, ImperativeAggregate}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@ExpressionDescription(
  usage = """
    _FUNC_(expr[, maxSize]) - Collects and returns a list of non-unique elements.
      The maximum allowed size of list is limited by `maxSize` parameter.
      If `maxSize` is not defined or non-positive, then size of list is not limited.""",
  examples = """
    Examples:
      > SELECT _FUNC_(col, 2) FROM VALUES (1), (2), (1) AS tab(col);
       [1,2]
  """,
  note = """
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle.
  """,
  group = "agg_funcs")
case class CollectListWithLimit(child: Expression,
                                limit: Int = -1,
                                mutableAggBufferOffset: Int = 0,
                                inputAggBufferOffset: Int = 0) extends Collect[mutable.ArrayBuffer[Any]] {

  def this(child: Expression) = this(child, -1, 0, 0)

  override protected def convertToBufferElement(value: Any): Any = InternalRow.copyValue(value)

  override protected lazy val bufferElementType: DataType = child.dataType

  override def createAggregationBuffer(): ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  override def eval(buffer: ArrayBuffer[Any]): Any = new GenericArrayData(buffer.toArray)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def update(buffer: mutable.ArrayBuffer[Any], input: InternalRow): mutable.ArrayBuffer[Any] = {
    if (limit <= 0 || buffer.size < limit) super.update(buffer, input)
    else buffer
  }

  override def merge(buffer: mutable.ArrayBuffer[Any], other: mutable.ArrayBuffer[Any]): mutable.ArrayBuffer[Any] = {
    if (limit <= 0) super.merge(buffer, other)
    else if (buffer.size >= limit) buffer
    else if (other.size >= limit) other
    else (buffer ++= other).take(limit)
  }

  override def prettyName: String = "collect_list_limit"
}
