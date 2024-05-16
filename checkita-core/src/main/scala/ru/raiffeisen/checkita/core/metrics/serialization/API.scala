package ru.raiffeisen.checkita.core.metrics.serialization

object API extends SerializersBasic 
  with SerializersCollections 
  with SerializersEnums
  with SerializersTuples
  with SerializersProducts {
  
  /**
   * Encodes value to byte array.
   *
   * @param value Value to encode.
   * @param serde Implicit SerDe used to encode value.
   * @tparam T Type of encoded value.
   * @return Array of bytes.
   */
  def encode[T](value: T)(implicit serde: SerDe[T]): Array[Byte] = serde.serialize(value)

  /**
   * Decodes value from byte array.
   *
   * @param bytes Array of bytes to decode.
   * @param serde Implicit SerDe used to decode value.
   * @tparam T Type of decoded value.
   * @return Decoded value of required type.
   */
  def decode[T](bytes: Array[Byte])(implicit serde: SerDe[T]): T = serde.deserialize(bytes)

  /**
   * Decodes value from byte array that contains multiple values:
   * retrieves required number of leading bytes from array, 
   * decodes them and returns both decoded value and remaining bytes.
   *
   * @param bytes Array of bytes to decode value from.
   * @param serde Implicit SerDe used to decode value.
   * @tparam T Type of decoded value.
   * @return Decoded value of required type and array of remaining bytes.
   */
  def decodeValue[T](bytes: Array[Byte])(implicit serde: SerDe[T]): (T, Array[Byte]) = serde.extractValue(bytes)
}
