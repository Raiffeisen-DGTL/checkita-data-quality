package ru.raiffeisen.checkita.core.metrics.serialization

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

trait SerializersBasic {

  /**
   * SerDe for integers
   */
  implicit object IntSerDe extends SerDe[Int] {
    override def serializeValue(value: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(value).array
    override def deserializeValue(bytes: Array[Byte]): Int = ByteBuffer.wrap(bytes).getInt
  }

  /**
   * SerDe for longs
   */
  implicit object LongSerDe extends SerDe[Long] {
    override def serializeValue(value: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(value).array
    override def deserializeValue(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong
  }

  /**
   * SerDe for doubles
   */
  implicit object DoubleSerDe extends SerDe[Double] {
    override def serializeValue(value: Double): Array[Byte] = ByteBuffer.allocate(8).putDouble(value).array
    override def deserializeValue(bytes: Array[Byte]): Double = ByteBuffer.wrap(bytes).getDouble
  }

  /**
   * SerDe for booleans.
   */
  implicit object BooleanSerDe extends SerDe[Boolean] {
    override def serializeValue(value: Boolean): Array[Byte] = Array(if (value) 1.toByte else 0.toByte)
    override def deserializeValue(bytes: Array[Byte]): Boolean = bytes.head != 0
  }

  /**
   * SerDe for strings.
   */
  implicit object StringSerDe extends SerDe[String] {
    override def serializeValue(value: String): Array[Byte] = value.getBytes
    override def deserializeValue(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)
  }
  
}
