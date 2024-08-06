package org.checkita.dqf.core.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

trait SerDeTransformations {

  /**
   * Transforms SerDe of current type to a SerDe of target type.
   * Values of the target type will be serializes/deserialized as values of
   * current type. Transformation between current and target type
   * is performed with use of provided conversion functions.
   * 
   * @param aSerDe SerDe for current type A.
   * @param f Conversion function from current type A to target type B.
   * @param g Conversion function from target type B to current type A.
   * @tparam A Current type
   * @tparam B Target type
   * @return SerDe of target type B.
   */
  def transform[A, B](aSerDe: SerDe[A], f: A => B, g: B => A): SerDe[B] =
    new SerDe[B] {
      override def serialize(bf: ByteArrayOutputStream, value: B): Unit = aSerDe.serialize(bf, g(value))
      override def deserialize(bf: ByteArrayInputStream): B = f(aSerDe.deserialize(bf))
    }

  /**
   * Build SerDe for kinded type families: family of classes that extends common parent trait or class.
   * 
   * Such SerDe is required to correctly serialize/deserialize generic collection 
   * containing various family classes. 
   * 
   * All family classes are mapped to their kind identifier, which is also encoded into serialized value.
   * During deserialization the class kind is decoded first and based on its value the appropriate SerDe
   * is retrieved to decode the actual class instance.
   * 
   * @param kSerDe SerDe to serialize/deserialize class kind identifier.
   * @param f Function to get class identifier for any family class.
   * @param g Function to retrieve class SerDe based on kind identifier.
   * @tparam K Type if kind identifier
   * @tparam T Type of family root trait or class.
   * @return Kinded SerDe for class family.
   */
  def kinded[K, T](kSerDe: SerDe[K], f: T => K, g: K => SerDe[T]): SerDe[T] =
    new SerDe[T] {
      override def serialize(bf: ByteArrayOutputStream, value: T): Unit = {
        val kind: K = f(value)
        val vSerDe = g(kind)
        kSerDe.serialize(bf, kind)
        vSerDe.serialize(bf, value)
      }

      override def deserialize(bf: ByteArrayInputStream): T = {
        val kind = kSerDe.deserialize(bf)
        val vSerDe = g(kind)
        vSerDe.deserialize(bf)
      }
    }
  
  /**
   * Unions SerDe's of two type into SerDe of tuple of these types.
   *
   * @param serDeA SerDe for type A.
   * @param serDeB SerDe for type B.
   * @tparam A Type of first SerDe
   * @tparam B Type of second SerDe
   * @return SerDe of tuple of input types: (A, B).
   */
  def union[A, B](serDeA: SerDe[A], serDeB: SerDe[B]): SerDe[(A, B)] = 
    new SerDe[(A, B)] {
      override def serialize(bf: ByteArrayOutputStream, value: (A, B)): Unit = {
        serDeA.serialize(bf, value._1)
        serDeB.serialize(bf, value._2)
      }

      override def deserialize(bf: ByteArrayInputStream): (A, B) = {
        val a = serDeA.deserialize(bf)
        val b = serDeB.deserialize(bf)
        a -> b
      }
    }
}
