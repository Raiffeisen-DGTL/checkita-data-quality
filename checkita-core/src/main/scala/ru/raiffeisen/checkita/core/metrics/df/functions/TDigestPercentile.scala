package ru.raiffeisen.checkita.core.metrics.df.functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.trees.QuaternaryLike
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType}
import org.isarnproject.sketches.java.TDigest

import java.nio.ByteBuffer

@ExpressionDescription(
  usage = """
    _FUNC_(expr, percentage[, accuracy, direct]) - Calculates percentiles, quantiles for provided elements
      with use of TDigest library: https://github.com/isarn/isarn-sketches.
      Input expression `expr` must be of DoubleType. Null values are ignored.
      `percentage` and `accuracy` must be of DoubleType as well.
      `direct` is a boolean flag indicating whether function returns percentile value
      provided with `percentage` value (`direct` = `false`) or returns percentage of
      provided percentile value (`direct` = `true`).
      In order to determine percentiles the `percentage` value must be within interval [0, 1].
      If `direct` = `true` then `percentage` should contain some percentile from set of column `expr` numbers.
      Percentile computation accuracy is determined by `accuracy` value (default is `0.005`).
    """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, 0.5) FROM VALUES (1), (2), (3), (4), (5) AS tab(col);
       (3.0)
      > SELECT _FUNC_(col, 0.5) FROM VALUES (1), (2), (3), (null), (null), (4), (5) AS tab(col);
       (3.0)
      > SELECT _FUNC_(col, 0.1) FROM VALUES (1), (2), (3), (null), (4), (5) AS tab(col);
       (1.3333333333333333)
      > SELECT _FUNC_(col, 2.0, 0.005, true) FROM VALUES (1), (2), (3), (4), (5) AS tab(col);
       (0.3)

  """,
  group = "agg_funcs"
)
case class TDigestPercentile(
                              child: Expression,
                              percentage: Expression,
                              accuracy: Expression,
                              direct: Expression,
                              override val mutableAggBufferOffset: Int = 0,
                              override val inputAggBufferOffset: Int = 0
                            )
  extends TypedImperativeAggregate[TDigest] with QuaternaryLike[Expression] with ExactInputTypes {

  override def prettyName: String = "tdigest_percentile"
  override def dataType: DataType = child.dataType
  // for empty inputs:
  // - returns zero percentage value when direct=true
  // - returns Double.NaN percentile when direct=false
  override def nullable: Boolean = false

  override def first: Expression = child
  override def second: Expression = percentage
  override def third: Expression = accuracy
  override def fourth: Expression = direct

  override def exactInputTypes: Seq[(Expression, DataType)] = Seq(
    (child, DoubleType),
    (percentage, DoubleType),
    (accuracy, DoubleType),
    (direct, BooleanType)
  )

  private lazy val tdAccuracy: Double = accuracy.eval().asInstanceOf[Double]
  private lazy val tdPercentage: Double = percentage.eval().asInstanceOf[Double]
  private lazy val tdDirect: Boolean = direct.eval().asInstanceOf[Boolean]

  override def createAggregationBuffer(): TDigest = TDigest.empty(tdAccuracy)

  override def update(buffer: TDigest, input: InternalRow): TDigest = {
    val value = child.eval(input)
    if (value != null) {
      val dblValue = value.asInstanceOf[Double]
      buffer.update(dblValue)
    }
    buffer
  }

  override def merge(buffer: TDigest, input: TDigest): TDigest = TDigest.merge(buffer, input)

  override def eval(buffer: TDigest): Any =
    if (tdDirect) buffer.cdf(tdPercentage) else buffer.cdfInverse(tdPercentage)
  
  override def serialize(buffer: TDigest): Array[Byte] = {

    // Byte array size is determined as follows:
    // 8 bytes for compression parameter (double)
    // 4 bytes for maxDiscrete parameter (integer)
    // 4 bytes for cluster centers array size (integer)
    // 4 bytes for cluster masses array size (integer)
    // nC * 8 bytes for cluster centers array (array<double>)
    // nM * 8 bytes for cluster masses array (array<double>)

    val (compression, maxDiscrete, centers, masses) = (
      buffer.getCompression, 
      buffer.getMaxDiscrete,
      java.util.Arrays.copyOf(buffer.getCentUnsafe, buffer.size),
      java.util.Arrays.copyOf(buffer.getMassUnsafe, buffer.size)
    )
    
    val byteBuffer = ByteBuffer.allocate(8 + 4 * 3 + 8 * centers.size + 8 * masses.size)
      .putDouble(compression)
      .putInt(maxDiscrete)
      .putInt(centers.size)
      .putInt(masses.size)

    centers.foldLeft(byteBuffer)(_.putDouble(_))
    masses.foldLeft(byteBuffer)(_.putDouble(_))

    byteBuffer.array
  }

  override def deserialize(storageFormat: Array[Byte]): TDigest = {
    val byteBuffer = ByteBuffer.wrap(storageFormat)
    val compression = byteBuffer.getDouble
    val maxDiscrete = byteBuffer.getInt
    val cSize = byteBuffer.getInt
    val mSize = byteBuffer.getInt

    val retrieveArray: Int => Array[Double] = arrSize => 
      (1 to arrSize).foldLeft(Array.empty[Double])((arr, _) => arr :+ byteBuffer.getDouble)

    val centers = retrieveArray(cSize)
    val masses = retrieveArray(mSize)
    new TDigest(compression, maxDiscrete, centers, masses)
  }

  override protected def withNewChildrenInternal(newFirst: Expression,
                                                 newSecond: Expression,
                                                 newThird: Expression,
                                                 newFourth: Expression): Expression = copy(
    child = newFirst, percentage = newSecond, accuracy = newThird, direct = newFourth
  )

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}
