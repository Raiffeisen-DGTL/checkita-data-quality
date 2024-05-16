package ru.raiffeisen.checkita.core.metrics.serialization

import java.nio.ByteBuffer

/**
 * Type class definition for binary serialization and deserialization.
 *
 * Used during buffer state serialization in checkpoint mechanism
 * of streaming applications.
 *
 * @note For each serialized value the first 4 bytes store integer
 *       number which specifies the number of bytes used to store the value.
 *       Thus, the total binary size of the serialized value is:
 *       4 bytes for value binary size + number of bytes used to store value itself.
 *       
 * @tparam T Type of value to be serialized/deserialized.
 */
private[serialization] trait SerDe[T] {

  /**
   * Serializes value to a variable length byte array.
   *
   * @param value Value to serialize
   * @return Array of bytes (variable length).
   */
  def serializeValue(value: T): Array[Byte]

  /**
   * Deserialize variable length byte array into value of target type.
   * @param bytes Array of bytes (variable length)
   * @return Value of this SerDe's type.
   */
  def deserializeValue(bytes: Array[Byte]): T

  /**
   * Determine the binary size of encoded value and split input array thus
   * returning part which contains value bytes and part with remaining bytes.
   *
   * @param bytes Input array of bytes.
   * @return Two byte subarrays. 
   *         First subarray contains bytes of the value to be decoded.
   *         Second one contains remaining bytes.
   */
  private def splitBySize(bytes: Array[Byte]): (Array[Byte], Array[Byte]) = {
    val (sizeBytes, valueBytes) = bytes.splitAt(4)
    val valueSize = ByteBuffer.wrap(sizeBytes).getInt
    valueBytes.splitAt(valueSize)
  }

  /**
   * Deserialize leading bytes provided byte array into value of target type and returns
   * both decoded value and remaining bytes.
   *
   * @param bytes Array of bytes to deserialize value from.
   * @return Value of this SerDe's type and remaining array of bytes.
   */
  def extractValue(bytes: Array[Byte]): (T, Array[Byte]) = {
    val (current, remaining) = splitBySize(bytes)
    deserializeValue(current) -> remaining
  }

  /**
   * Serializes value to array of bytes.
   *
   * @param value Value to serialize
   * @return Array of bytes.
   *
   * @note First 4 bytes stores binary size of serialized value.
   */
  def serialize(value: T): Array[Byte] = {
    val bytes = serializeValue(value)
    val buffer = ByteBuffer.allocate(4 + bytes.length)
      .putInt(bytes.length)
      .put(bytes)
    buffer.array
  }

  /**
   * Deserialize array of bytes to value of target type.
   *
   * @param bytes Array of bytes to deserialize
   * @return Value of this SerDe's type.
   *
   * @note Number of bytes to retrieve for serialization is determined from
   *       first 4 bytes of the array, which store integer number specifying
   *       value binary size.
   */
  def deserialize(bytes: Array[Byte]): T = {
    val (current, _) = splitBySize(bytes)
    deserializeValue(current)
  }
}
