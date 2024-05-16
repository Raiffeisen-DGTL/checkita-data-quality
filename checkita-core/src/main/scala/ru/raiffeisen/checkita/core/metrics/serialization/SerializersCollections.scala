package ru.raiffeisen.checkita.core.metrics.serialization

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap

trait SerializersCollections {

  /**
   * Type class definition for serializing/deserializing traversable collections 
   * of single-typed value.
   *
   * @tparam T Type of collection values.
   * @tparam W Higher-kinded type of traversable collection
   */
  sealed trait SingleTraversableSerDe[T, W[T] <: TraversableOnce[T]] extends SerDe[W[T]] {
    
    /**
     * Initiates empty collection.
     *
     * @return Empty collection of required type.
     */
    protected def getEmpty: W[T]

    /**
     * Appends element to collection.
     *
     * @param current Current collection state
     * @param element Element to append
     * @return Updated collection.
     */
    protected def appendElem(current: W[T], element: T): W[T]

    /**
     * SerDe used to serialize/deserialize collection elements.
     */
    protected val serDeT: SerDe[T]

    /**
     * Recursive function used to deserialize collection elements
     * from byte array and add them to a resulting collection.
     *
     * @param bytes Array of bytes to deserialize
     * @param acc   Accumulator storing current collection state.
     * @return Deserialized collection
     */
    @tailrec
    private def deserializeValues(bytes: Array[Byte], acc: W[T]): W[T] =
      if (bytes.isEmpty) acc else {
        val (currentValue, remaining) = serDeT.extractValue(bytes)
        deserializeValues(remaining, appendElem(acc, currentValue))
      }
    
    override def serializeValue(value: W[T]): Array[Byte] =
      value.foldLeft(Array.empty[Byte])((b, v) => b ++ serDeT.serialize(v))

    override def deserializeValue(bytes: Array[Byte]): W[T] = deserializeValues(bytes, getEmpty)
  }

  /**
   * Type class definition for serializing/deserializing traversable collections
   * of two type arguments (e.g. maps).
   *
   * @tparam K Collection key type argument
   * @tparam V Collection value type argument
   * @tparam W Higher-kinded type of traversable collection.
   */
  sealed trait TupledTraversableSerDe[K, V, W[K, V] <: TraversableOnce[(K, V)]] extends SerDe[W[K, V]] {

    /**
     * Initiates empty collection.
     *
     * @return Empty collection of required type.
     */    
    protected def getEmpty: W[K, V]

    /**
     * Appends element to collection. 
     * For collections with two type arguments, the element is a tuple
     * of this two types.
     *
     * @param current Current collection state
     * @param element Element to append
     * @return Updated collection.
     */
    protected def appendElem(current: W[K, V], element: (K, V)): W[K, V]

    /**
     * SerDe used to serialize/deserialize collection elements.
     */
    protected val serDeT: SerDe[(K, V)]

    /**
     * Recursive function used to deserialize collection elements
     * from byte array and add them to a resulting collection.
     *
     * @param bytes Array of bytes to deserialize
     * @param acc   Accumulator storing current collection state.
     * @return Deserialized collection
     */
    @tailrec
    private def deserializeValues(bytes: Array[Byte], acc: W[K, V]): W[K, V] =
      if (bytes.isEmpty) acc else {
        val (currentValue, remaining) = serDeT.extractValue(bytes)
        deserializeValues(remaining, appendElem(acc, currentValue))
      }

    override def serializeValue(value: W[K, V]): Array[Byte] =
      value.foldLeft(Array.empty[Byte])((b, v) => b ++ serDeT.serialize(v))

    override def deserializeValue(bytes: Array[Byte]): W[K, V] = deserializeValues(bytes, getEmpty)
  }

  /**
   * Implicit conversion to generate SerDe for sequence-like collections.
   *
   * @param simpleSerDe Implicit SerDe for collection elements
   * @tparam T Type of collection elements
   * @tparam W Higher-kinded type of sequence-like collection
   * @return SerDe for collection W[T]
   */
  implicit def sequenceSerDe[T, W[T] <: Seq[T]](implicit simpleSerDe: SerDe[T]): SerDe[W[T]] =
    new SingleTraversableSerDe[T, W] {
      override protected def getEmpty: W[T] = Seq.empty[T].asInstanceOf[W[T]]
      override protected def appendElem(current: W[T], element: T): W[T] = (current :+ element).asInstanceOf[W[T]]
      override protected val serDeT: SerDe[T] = simpleSerDe
    }

  /**
   * Implicit conversion to generate SerDe for sets.
   *
   * @param simpleSerDe Implicit SerDe for set elements
   * @tparam T Type of set elements
   * @return SerDe for Set[T]
   */
  implicit def setSerDe[T](implicit simpleSerDe: SerDe[T]): SerDe[Set[T]] =
    new SingleTraversableSerDe[T, Set] {
      override protected def getEmpty: Set[T] = Set.empty[T]
      override protected def appendElem(current: Set[T], element: T): Set[T] = current + element
      override protected val serDeT: SerDe[T] = simpleSerDe
    }

  /**
   * Implicit conversion conversion to generate SerDe for maps.
   *
   * @param simpleSerDe Implicit SerDe for map elements (tuple key -> value)
   * @tparam K Type map keys
   * @tparam V Type of map values
   * @return SerDe for Map[K, V]
   */
  implicit def mapSerDe[K, V](implicit simpleSerDe: SerDe[(K, V)]): SerDe[Map[K, V]] =
    new TupledTraversableSerDe[K, V, Map] {
      override protected def getEmpty: Map[K, V] = Map.empty[K, V]
      override protected def appendElem(current: Map[K, V], element: (K, V)): Map[K, V] = current + element
      override protected val serDeT: SerDe[(K, V)] = simpleSerDe
    }

  /**
   * Implicit conversion conversion to generate SerDe for concurrent TrieMap.
   *
   * @param simpleSerDe Implicit SerDe for map elements (tuple key -> value)
   * @tparam K Type map keys
   * @tparam V Type of map values
   * @return SerDe for TrieMap[K, V]
   */
  implicit def trieMapSerDe[K, V](implicit simpleSerDe: SerDe[(K, V)]): SerDe[TrieMap[K, V]] =
    new TupledTraversableSerDe[K, V, TrieMap] {
      override protected def getEmpty: TrieMap[K, V] = TrieMap.empty[K, V]
      override protected def appendElem(current: TrieMap[K, V], element: (K, V)): TrieMap[K, V] = {
        current += element
        current
      }
      override protected val serDeT: SerDe[(K, V)] = simpleSerDe
    }

  /**
   * Implicit conversion to generate SerDe for options.
   *
   * @param simpleSerDe Implicit SerDe for element within option.
   * @tparam T Type of option element.
   * @return SerDe for Option[T]
   */
  implicit def optionSerDe[T](implicit simpleSerDe: SerDe[T]): SerDe[Option[T]] =
    new SerDe[Option[T]] {
      override def serializeValue(value: Option[T]): Array[Byte] = value match {
        case None => Array.empty
        case Some(v) => simpleSerDe.serialize(v)
      }
      override def deserializeValue(bytes: Array[Byte]): Option[T] =
        if (bytes.isEmpty) None else Some(simpleSerDe.deserialize(bytes))
    }
  
}
