package ru.raiffeisen.checkita.core.serialization

import enumeratum.{Enum, EnumEntry}
import eu.timepit.refined.api.Refined

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

trait SerializersSpecific { this: SerDeTransformations =>

  /**
   * Implicit conversion to generate SerDe for enumerations.
   * All enumeration values are mapped to byte values and, 
   * therefore, are encoded as single byte value.
   *
   * @param e Enumeration object holding all enumeration entries of current type.
   * @tparam E Type of enumeration
   * @return SerDe to serialize/deserialize enumeration values.
   */
  protected def getEnumSerDe[E <: EnumEntry](e: Enum[E]): SerDe[E] =
    new SerDe[E] {
      private val byteEncoding: Map[E, Byte] =
        e.values.zipWithIndex.map{ case (c, i) => c -> i.toByte }.toMap
      private val valueEncoding: Map[Byte, E] = byteEncoding.map(_.swap)

      override def serialize(bf: ByteArrayOutputStream, value: E): Unit = bf.write(byteEncoding(value))
      override def deserialize(bf: ByteArrayInputStream): E =
        valueEncoding(bf.read().toByte)
    }
  
  /**
   * Implicit coversion to serialize/deserialize refined types.
   *
   * @param tSerDe Implicit SerDe for value within refined type.
   * @tparam T Type of value
   * @tparam P Type of refinement predicate
   * @tparam R Actual refined type
   * @return SerDe for refined type.
   */
  implicit def refinedSerDe[T, P, R <: Refined[T, P]](implicit tSerDe: SerDe[T]): SerDe[Refined[T, P]] =
    transform(tSerDe, Refined.unsafeApply[T, P], v => v.value)

  /**
   * Builds SerDe for case classes. Case classes will be serialized as tuple of their fields.
   * Thus, in order to build SerDe for case class it is required to have SerDe of corresponding tuple.
   *
   * @param toT    Function to convert case class to tuple containing its fields' values.
   * @param fromT  Function to create case class instance from tuple containing its fields' values.
   * @param tSerDe SerDe for tupled representation of case class.
   * @tparam P Case class type.
   * @tparam T Tuple type
   * @return SerDe for case class P.
   */
  protected def getProductSerDe[P <: Product, T](toT: P => Option[T], fromT: T => P)
                                                (implicit tSerDe: SerDe[T]): SerDe[P] =
    transform(tSerDe, fromT, (p: P) => toT(p).get)
  
}
