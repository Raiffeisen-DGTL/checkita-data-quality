package org.checkita.dqf.core.serialization

import com.twitter.algebird.{HLL, HyperLogLog, SpaceSaver}
import org.isarnproject.sketches.java.TDigest

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InvalidClassException}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.util.Try

trait SerializersBasic {

  /**
   * SerDe for integers
   */
  implicit object IntSerDe extends SerDe[Int] {
    override def serialize(bf: ByteArrayOutputStream, value: Int): Unit = encodeSize(bf, value)
    override def deserialize(bf: ByteArrayInputStream): Int = decodeSize(bf)
  }

  /**
   * SerDe for longs
   */
  implicit object LongSerDe extends SerDe[Long] {
    override def serialize(bf: ByteArrayOutputStream, value: Long): Unit =
      bf.write(ByteBuffer.allocate(8).putLong(value).array)

    override def deserialize(bf: ByteArrayInputStream): Long = {
      val bytes = new Array[Byte](8)
      bf.read(bytes)
      ByteBuffer.wrap(bytes).getLong
    }
  }

  /**
   * SerDe for doubles
   */
  implicit object DoubleSerDe extends SerDe[Double] {
    override def serialize(bf: ByteArrayOutputStream, value: Double): Unit = 
      bf.write(ByteBuffer.allocate(8).putDouble(value).array)

    override def deserialize(bf: ByteArrayInputStream): Double = {
      val bytes = new Array[Byte](8)
      bf.read(bytes)
      ByteBuffer.wrap(bytes).getDouble
    }
  }

  /**
   * SerDe for booleans.
   */
  implicit object BooleanSerDe extends SerDe[Boolean] {
    override def serialize(bf: ByteArrayOutputStream, value: Boolean): Unit = 
      if (value) bf.write(1.toByte) else bf.write(0.toByte)

    override def deserialize(bf: ByteArrayInputStream): Boolean = {
      val byte = bf.read()
      if (byte < 0) throw new IndexOutOfBoundsException(
        "End of byte array input stream is reached before values is deserialized."
      )
      byte > 0
    }
  }

  /**
   * SerDe for strings.
   */
  implicit object StringSerDe extends SerDe[String] {

    override def serialize(bf: ByteArrayOutputStream, value: String): Unit = 
      writeVarSizeValueBytes(bf, value.getBytes(StandardCharsets.UTF_8))

    override def deserialize(bf: ByteArrayInputStream): String = 
      new String(readVarSizeValueBytes(bf), StandardCharsets.UTF_8)
  }

  /**
   * Implicit SerDe for Algebird HLL monoid.
   */
  implicit object HllSerDe extends SerDe[HLL] {
    override def serialize(bf: ByteArrayOutputStream, value: HLL): Unit =
      writeVarSizeValueBytes(bf, HyperLogLog.toBytes(value))

    override def deserialize(bf: ByteArrayInputStream): HLL =
      HyperLogLog.fromBytes(readVarSizeValueBytes(bf))
  }

  /**
   * Implicit SerDe for Algebird SpaceSaver monoid.
   */
  implicit object StringSpaceSaverSerDe extends SerDe[SpaceSaver[String]] {
    override def serialize(bf: ByteArrayOutputStream, value: SpaceSaver[String]): Unit =
      writeVarSizeValueBytes(bf, SpaceSaver.toBytes(value, (s: String) => s.getBytes))

    override def deserialize(bf: ByteArrayInputStream): SpaceSaver[String] =
      SpaceSaver.fromBytes(
        readVarSizeValueBytes(bf),
        (b: Array[Byte]) => Try(new String(b, StandardCharsets.UTF_8))
      ).getOrElse(
        throw new InvalidClassException("Unable to deserialize array of bytes into SpaceSaverBuffer.")
      )
  }

  /**
   * Implicit SerDe for TDigest objects.
   */
  implicit object TDigestSerDe extends SerDe[TDigest] {
    // Byte array size is determined as follows:
    // 8 bytes for compression parameter (double)
    // 4 bytes for maxDiscrete parameter (integer)
    // 4 bytes for cluster centers array size (integer)
    // 4 bytes for cluster masses array size (integer)
    // nC * 8 bytes for cluster centers array (array<double>)
    // nM * 8 bytes for cluster masses array (array<double>)

    def toBytes(value: TDigest): Array[Byte] = {

      val (compression, maxDiscrete, centers, masses) = (
        value.getCompression,
        value.getMaxDiscrete,
        java.util.Arrays.copyOf(value.getCentUnsafe, value.size),
        java.util.Arrays.copyOf(value.getMassUnsafe, value.size)
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

    def fromBytes(bytes: Array[Byte]): TDigest = {
      val byteBuffer = ByteBuffer.wrap(bytes)
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

    override def serialize(bf: ByteArrayOutputStream, value: TDigest): Unit =
      writeVarSizeValueBytes(bf, toBytes(value))

    override def deserialize(bf: ByteArrayInputStream): TDigest =
      fromBytes(readVarSizeValueBytes(bf))
  }
}
