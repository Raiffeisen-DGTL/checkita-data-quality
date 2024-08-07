package org.checkita.dqf.core.metrics.df.functions

import com.twitter.algebird.{HLL, HyperLogLog, HyperLogLogMonoid}
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Try

@ExpressionDescription(
  usage = """
    _FUNC_(expr[, accuracy]) - Calculates approximate number of distinct values in the dataset
      with use HyperLogLog algorithm implementtation from Algebird library (abstract algebra for Scala)
      https://github.com/twitter/algebird.
      Input expression `expr` is expected to be of primitive type, hence complex types can also be processed.
      Null values are ignored.
      Distinct values estimation accuracy is determined by `accuracy` value (default is `0.01`).
      `accuracy` must be of DoubleType.
      Return type is DoubeType.
    """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (3), (4), (5) AS tab(col);
       (5.0)
      > SELECT _FUNC_(col) FROM VALUES ("aaa"), ("bbb"), ("ccc"), (null), (null), ("ddd"), ("eee") AS tab(col);
       (5.0)
      > SELECT _FUNC_(col, 0.005) FROM VALUES (1.11), (2.22), (3.33), (null), (4.44), (5.55) AS tab(col);
       (5.0)
  """,
  group = "agg_funcs"
)
case class HLLCountDistinct(
                             child: Expression,
                             accuracy: Expression,
                             override val mutableAggBufferOffset: Int = 0,
                             override val inputAggBufferOffset: Int = 0
                           )
  extends TypedImperativeAggregate[HLL] with BinaryLike[Expression] {

  override def prettyName: String = "hll_count_distinct"
  override def left: Expression = child
  override def right: Expression = accuracy
  override def nullable: Boolean = false
  override def dataType: DataType = DoubleType

  /**
   * Child expression can have any type, as it will be converted to Array[Byte] in order to update HLL monoid.
   * Accuracy, on the other hand, must have a DoubleType.
   */
  override def checkInputDataTypes(): TypeCheckResult = {
    if (accuracy.dataType != DoubleType) {
      val msg = s"argument 2 requires ${DoubleType.simpleString} type, " +
        s"however, '${accuracy.sql}' is of ${accuracy.dataType.catalogString} type."
      TypeCheckResult.TypeCheckFailure(msg)

    } else TypeCheckResult.TypeCheckSuccess
  }

  private lazy val hllAccuracy: Double = accuracy.eval().asInstanceOf[Double]

  /**
   * Accuracy is limited to a value that correspond to bits number of 30 or less.
   * This is done to prevent integer overflow when computing HLL monoid size.
   *
   * @param accuracy Required computation accuracy
   * @return Number of bits that correspond to requested accuracy.
   */
  private def getBitsForError(accuracy: Double): Int = HyperLogLog.bitsForError(math.max(accuracy, 0.00003174))

  /**
   * For empty string we will generate some array of bytes in order to consider empty string as a distinct value.
   * For that purpose we will generate some UUID from static stirng and then convert it to array of bytes.
   * This will give us high probability that this array of bytes won't collide with any other value in the column.
   * @param bytes Array of bytes generated from column value
   * @return Same array if it is not empty or other predefined array as a replacement of empty string value.
   */
  private def bytesOr(bytes: Array[Byte]): Array[Byte] =
    if (bytes.isEmpty) UUID.nameUUIDFromBytes(prettyName.getBytes).toString.getBytes
    else bytes

  /**
   * Converting value to array of bytes depending on types of the value.
   * @param value Value to convert to bytes
   * @return Array of bytes for given value
   */
  private def valueToBytes(value: Any): Option[Array[Byte]] = value match {
    case byte: java.lang.Byte => Some(Array(byte))
    case short: java.lang.Short => Some(ByteBuffer.allocate(2).putShort(short).array())
    case int: java.lang.Integer => Some(ByteBuffer.allocate(4).putInt(int).array())
    case long: java.lang.Long => Some(ByteBuffer.allocate(8).putLong(long).array())
    case double: java.lang.Double =>  Some(ByteBuffer.allocate(8).putDouble(double).array())
    case float: java.lang.Float => Some(ByteBuffer.allocate(4).putFloat(float).array())
    case string: String => Some(bytesOr(string.getBytes))
    case char: Char => Some(bytesOr(char.toString.getBytes))
    case decimal: java.math.BigDecimal => Some(ByteBuffer.allocate(8).putLong(decimal.longValue()).array())
    case date: java.sql.Date => Some(ByteBuffer.allocate(8).putLong(date.toLocalDate.toEpochDay).array())
    case localDate: java.time.LocalDate => Some(ByteBuffer.allocate(8).putLong(localDate.toEpochDay).array())
    case timestamp: java.sql.Timestamp => Some(ByteBuffer.allocate(8).putLong(timestamp.toInstant.toEpochMilli).array())
    case instant: java.time.Instant => Some(ByteBuffer.allocate(8).putLong(instant.toEpochMilli).array())
    case arrayByte: Array[Byte] => Some(arrayByte)
    // spark unsafe types:
    case utf8String: UTF8String => Some(bytesOr(utf8String.getBytes))
    case calInt: CalendarInterval => Some(ByteBuffer.allocate(8).putLong(calInt.extractAsDuration().toMillis).array())
    // other serializable types:
    case serializable: Serializable => Try(SerializationUtils.serialize(serializable)).toOption
    // null and any non-serializable types:
    case _ => None
  }

  /**
   * Creates new HLL monoid for provided array of bytes.
   * @param bytes Input array of bytes
   * @return HLL monoid
   */
  private def wrapBytesToHLL(bytes: Array[Byte]): HLL = {
    val bits = getBitsForError(hllAccuracy)
    val hll = new HyperLogLogMonoid(bits)
    hll.create(bytes)
  }

  override def createAggregationBuffer(): HLL = {
    val bits = getBitsForError(hllAccuracy)
    new HyperLogLogMonoid(bits).zero
  }

  override def update(buffer: HLL, input: InternalRow): HLL = valueToBytes(child.eval(input)) match {
    case Some(bytes) => buffer + wrapBytesToHLL(bytes)
    case None => buffer
  }

  override def merge(buffer: HLL, input: HLL): HLL = buffer + input

  override def eval(buffer: HLL): Any = buffer.approximateSize.estimate.toDouble

  override def serialize(buffer: HLL): Array[Byte] = HyperLogLog.toBytes(buffer)
  override def deserialize(storageFormat: Array[Byte]): HLL = HyperLogLog.fromBytes(storageFormat)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(child = newLeft, accuracy = newRight)
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)
  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}
