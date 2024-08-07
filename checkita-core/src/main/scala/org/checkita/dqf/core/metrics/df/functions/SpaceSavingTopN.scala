package org.checkita.dqf.core.metrics.df.functions

import com.twitter.algebird.SpaceSaver
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.checkita.dqf.core.metrics.df.functions.SpaceSavingTopN._

import java.io.InvalidClassException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.util.Try


@ExpressionDescription(
  usage = """
    _FUNC_(expr, targetNumber[, maxCapacity]) - Get approximate most frequent top-N values from column.
      Uses SpaceSaver buffer from Algebird library (abstract algebra for Scala)
      https://github.com/twitter/algebird.
      Input expression `expr` must to be of StringType. Null values are skipped.
      `targetNumber` and `maxCapacity` must be of IntegerType.
      `targetNumber` sets the number of top-elements to retrieve.
      `maxCapacity` sets the maximum SpaceSaver container capacity. Default is 100.
      Returns array with top-N elements, where each elements is a StructType with following fields:
        - `value` (string) - top-N element value
        - `frequency` (double) - frequency of this element occurrences within column values.
    """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, 3) FROM VALUES ("a"), ("a"), ("a"), ("a"), ("b"), ("b"), ("b") ("c"), ("c"), ("d") AS tab(col);
       (("a", 0.4), ("b", 0.3), ("c", 0.2))
  """,
  group = "agg_funcs"
)
case class SpaceSavingTopN(
                            child: Expression,
                            targetNumber: Expression,
                            maxCapacity: Expression,
                            override val mutableAggBufferOffset: Int = 0,
                            override val inputAggBufferOffset: Int = 0
                          )
  extends TypedImperativeAggregate[SpaceSaverBuffer] with TernaryLike[Expression] with ExactInputTypes {
  override def prettyName: String = "space_saving_top_n"
  override def dataType: DataType = ArrayType(StructType(Seq(
    StructField("value", StringType, nullable = false),
    StructField("frequency", DoubleType, nullable = false)
  )), containsNull = false)

  override def nullable: Boolean = false

  override def first: Expression = child
  override def second: Expression = targetNumber
  override def third: Expression = maxCapacity

  override def exactInputTypes: Seq[(Expression, DataType)] = Seq(
    (child, StringType), (maxCapacity, IntegerType), (targetNumber, IntegerType)
  )

  private lazy val capacity: Int = maxCapacity.eval().asInstanceOf[Int]
  private lazy val target: Int = targetNumber.eval().asInstanceOf[Int]

  override def createAggregationBuffer(): SpaceSaverBuffer = SpaceSaverBuffer.empty(capacity)

  override def update(buffer: SpaceSaverBuffer, input: InternalRow): SpaceSaverBuffer = {
    val value = child.eval(input)
    if (value != null) {
      val strValue = value.asInstanceOf[UTF8String].toString
      buffer + strValue
    } else buffer
  }

  override def merge(buffer: SpaceSaverBuffer, input: SpaceSaverBuffer): SpaceSaverBuffer = buffer ++ input

  override def eval(buffer: SpaceSaverBuffer): Any = {
    val topN = buffer.buffer.topK(target).map {
      case (value, approximate, _) => InternalRow.apply(
        UTF8String.fromString(value),
        approximate.estimate.toDouble / buffer.counter
      )
    }
    new GenericArrayData(topN)
  }

  override def serialize(buffer: SpaceSaverBuffer): Array[Byte] = SpaceSaverBuffer.serialize(buffer)
  override def deserialize(storageFormat: Array[Byte]): SpaceSaverBuffer = SpaceSaverBuffer.deserialize(storageFormat)

  override protected def withNewChildrenInternal(newFirst: Expression,
                                                 newSecond: Expression,
                                                 newThird: Expression): Expression = copy(
    child = newFirst, targetNumber = newSecond, maxCapacity = newThird
  )
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

object SpaceSavingTopN {

  case class SpaceSaverBuffer(
                               buffer: SpaceSaver[String],
                               counter: Long,
                               capacity: Int
                             ) {

    def +(value: String): SpaceSaverBuffer = SpaceSaverBuffer(
      this.buffer ++ SpaceSaver(capacity, value), this.counter + 1, this.capacity
    )

    def ++(that: SpaceSaverBuffer): SpaceSaverBuffer = {
      require(this.capacity == that.capacity, "Unable to merge two SpaceSaver containers with different capacity!")
      SpaceSaverBuffer(this.buffer ++ that.buffer, this.counter + that.counter, this.capacity)
    }
  }
  object SpaceSaverBuffer {
    def empty(capacity: Int): SpaceSaverBuffer =
      SpaceSaverBuffer(SpaceSaver(capacity, "", 0), 0, capacity)

    def serialize(buffer: SpaceSaverBuffer): Array[Byte] = {
      val bufferBytes = SpaceSaver.toBytes(buffer.buffer, (s: String) => s.getBytes)

      // 8 bytes for counter [Long] + 4 bytes for capacity [Int] + SpaceSaver bytes.
      val resBuffer = new Array[Byte](bufferBytes.length + 12)
      ByteBuffer
        .wrap(resBuffer)
        .putLong(buffer.counter)
        .putInt(buffer.capacity)
        .put(bufferBytes)

      resBuffer
    }

    def deserialize(bytes: Array[Byte]): SpaceSaverBuffer = {
      val bytesBuffer = ByteBuffer.wrap(bytes)
      val counter = bytesBuffer.getLong
      val capacity = bytesBuffer.getInt
      val spaceSaverBytes = new Array[Byte](bytes.length - 12)
      bytesBuffer.get(spaceSaverBytes)
      val spaceSaver = SpaceSaver.fromBytes(
        spaceSaverBytes,
        (b: Array[Byte]) => Try(new String(b, StandardCharsets.UTF_8))
      )
      spaceSaver.map(s => SpaceSaverBuffer(s, counter, capacity)).getOrElse(
        throw new InvalidClassException("Unable to deserialize array of bytes into SpaceSaverBuffer.")
      )
    }
  }
}

