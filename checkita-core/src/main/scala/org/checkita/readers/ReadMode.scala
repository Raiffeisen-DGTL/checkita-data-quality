package org.checkita.readers

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

/**
 * Sources can be read in two modes: as a static dataframe and as a stream one.
 */
sealed trait ReadMode extends EnumEntry
object ReadMode extends Enum[ReadMode] {
  case object Batch extends ReadMode
  case object Stream extends ReadMode
  override def values: immutable.IndexedSeq[ReadMode] = findValues
}
