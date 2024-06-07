package ru.raiffeisen.checkita.core.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.compat._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer

trait SerializersCollections { this: SerDeTransformations =>

  /**
   * Implicit conversion to generate SerDe for traversable collections of elements
   *
   * @note Deserialization is performed with use of array buffer as appending element to it
   *       takes constant time.
   * @param f      Function to build target collection from array buffer.
   * @param tSerDe Implicit SerDe for collection elements.
   * @tparam T Type of collection elements
   * @tparam W Actual type of collection
   * @return SerDe for collection.
   */
  private def getCollectionSerDe[T, W[T] <: IterableOnce[T]](f: ArrayBuffer[T] => W[T])
                                                               (implicit tSerDe: SerDe[T]): SerDe[W[T]] =
    new SerDe[W[T]] {
      override def serialize(bf: ByteArrayOutputStream, value: W[T]): Unit = {
        encodeSize(bf, value.iterator.size)
        value.iterator.foreach(v => tSerDe.serialize(bf, v))
      }

      override def deserialize(bf: ByteArrayInputStream): W[T] = {
        val size = decodeSize(bf)
        val arrayBuffer = (1 to size).foldLeft(ArrayBuffer.empty[T]){ (b, _) => b += tSerDe.deserialize(bf); b }
        f(arrayBuffer)
      }
    }

  /**
   * Implicit conversion to generate SerDe for traversable collections of tuple of two elements.
   *
   * @note Deserialization is performed with use of array buffer as appending element to it
   *       takes constant time.
   * @param f      Function to build target collection from array buffer.
   * @param tSerDe Implicit SerDe for collection elements.
   * @tparam K Type of collection keys
   * @tparam V Type of collection values
   * @tparam W Actual type of collection
   * @return SerDe for collection of tuple of two elements.
   */
  private def getTupledCollectionSerDe[K, V, W[K, V] <: IterableOnce[(K, V)]]
                                      (f: ArrayBuffer[(K, V)] => W[K, V])
                                      (implicit tSerDe: SerDe[(K, V)]): SerDe[W[K, V]] =
    new SerDe[W[K, V]] {
      override def serialize(bf: ByteArrayOutputStream, value: W[K, V]): Unit = {
        encodeSize(bf, value.iterator.size)
        value.iterator.foreach(v => tSerDe.serialize(bf, v))
      }

      override def deserialize(bf: ByteArrayInputStream): W[K, V] = {
        val size = decodeSize(bf)
        val arrayBuffer = (1 to size).foldLeft(ArrayBuffer.empty[(K, V)]){ (b, _) => b += tSerDe.deserialize(bf); b }
        f(arrayBuffer)
      }
    }
  
  /**
   * Implicit conversion to generate SerDe for Seq[T]
   */
  implicit def seqSerDe[T](implicit tSerDe: SerDe[T]): SerDe[Seq[T]] =
    getCollectionSerDe[T, Seq](bf => bf.toSeq)

  /** 
   * Implicit conversion to generate SerDe for List[T]
   */
  implicit def listSerDe[T](implicit tSerDe: SerDe[T]): SerDe[List[T]] =
    getCollectionSerDe[T, List](bf => bf.toList)

  /**
   * Implicit conversion to generate SerDe for Vector[T]
   */
  implicit def vectorSerDe[T](implicit tSerDe: SerDe[T]): SerDe[Vector[T]] =
    getCollectionSerDe[T, Vector](bf => bf.toVector)
  
  /**
   * Implicit conversion to generate SerDe for Set[T]
   */
  implicit def setSerDe[T](implicit tSerDe: SerDe[T]): SerDe[Set[T]] =
    getCollectionSerDe[T, Set](bf => bf.toSet)

  /**
   * Implicit conversion conversion to generate SerDe for Map[K, V]
   */
  implicit def mapSerDe[K, V](implicit tSerDe: SerDe[(K, V)]): SerDe[Map[K, V]] =
    getTupledCollectionSerDe[K, V, Map](bf => bf.toMap)

  /**
   * Implicit conversion conversion to generate SerDe for concurrent TrieMap.
   */
  implicit def trieMapSerDe[K, V](implicit tSerDe: SerDe[(K, V)]): SerDe[TrieMap[K, V]] =
    getTupledCollectionSerDe[K, V, TrieMap](bf => TrieMap(bf.toSeq: _*))
    

  /**
   * Implicit conversion to generate SerDe for options.
   *
   * @param tSerDe Implicit SerDe for element within option.
   * @tparam T Type of option element.
   * @return SerDe for Option[T]
   */
  implicit def optionSerDe[T](implicit tSerDe: SerDe[T]): SerDe[Option[T]] =
    new SerDe[Option[T]] {
      override def serialize(bf: ByteArrayOutputStream, value: Option[T]): Unit = value match {
        case Some(v) => 
          encodeSize(bf, 1)
          tSerDe.serialize(bf, v)
        case None => encodeSize(bf, 0)
      }
      override def deserialize(bf: ByteArrayInputStream): Option[T] = {
        val size = decodeSize(bf)
        if (size == 0) None else Some(tSerDe.deserialize(bf))
      }
    }
  
}
