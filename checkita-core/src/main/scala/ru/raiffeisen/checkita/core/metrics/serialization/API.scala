package ru.raiffeisen.checkita.core.metrics.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

object API {

  /**
   * Encodes value to byte array.
   *
   * @param value Value to encode.
   * @param serde Implicit SerDe used to encode value.
   * @tparam T Type of encoded value.
   * @return Array of bytes.
   */
  def encode[T](value: T)(implicit serde: SerDe[T]): Array[Byte] = {
    val bf = new ByteArrayOutputStream()
    serde.serialize(bf, value)
    bf.toByteArray
  }

  /**
   * Decodes value from byte array.
   *
   * @param bytes Array of bytes to decode.
   * @param serde Implicit SerDe used to decode value.
   * @tparam T Type of decoded value.
   * @return Decoded value of required type.
   */
  def decode[T](bytes: Array[Byte])(implicit serde: SerDe[T]): T = {
    val bf = new ByteArrayInputStream(bytes)
    serde.deserialize(bf)
  }
}
