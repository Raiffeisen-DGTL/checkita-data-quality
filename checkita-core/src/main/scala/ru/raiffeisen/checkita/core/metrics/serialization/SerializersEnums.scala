package ru.raiffeisen.checkita.core.metrics.serialization

import enumeratum.EnumEntry
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.metrics.MetricName

trait SerializersEnums {

  /**
   * Implicit conversion to generate SerDe for enumerations.
   * All enumeration values are mapped to byte values and, 
   * therefore, are encoded as single byte value.
   *
   * @param byteEncoding Implicit byte enumeration byte encoding.
   * @tparam E Type of enumeration
   * @return SerDe to serialize/deserialize enumeration values.
   */
  implicit def getEnumSerDe[E <: EnumEntry](implicit byteEncoding: Map[E, Byte]): SerDe[E] =
    new SerDe[E] {
      override def serializeValue(value: E): Array[Byte] = Array(byteEncoding(value))
      override def deserializeValue(bytes: Array[Byte]): E = {
        val valueEncoding = byteEncoding.map(_.swap)
        valueEncoding(bytes.head)
      }
    }

  /**
   * Build byte encoding for enumeration values.
   *
   * @param values Sequence of enumeration values.
   * @tparam E Type of enumeration
   * @return Enumeration byte encoding.
   */
  private def valuesToMap[E](values: Seq[E]): Map[E, Byte] =
    values.zipWithIndex.map{ case (c, i) => c -> i.toByte }.toMap
    
  implicit val calculatorStatusEncoding: Map[CalculatorStatus, Byte] = valuesToMap(CalculatorStatus.values)
  implicit val metricNameEncoding: Map[MetricName, Byte] = valuesToMap(MetricName.values)
  
}
