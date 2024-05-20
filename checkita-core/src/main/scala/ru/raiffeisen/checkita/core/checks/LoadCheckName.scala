package ru.raiffeisen.checkita.core.checks

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

/**
 * Enumeration holding all load check calculator names
 *
 * @param entryName load check calculator name
 */
sealed abstract class LoadCheckName(override val entryName: String) extends EnumEntry
object LoadCheckName extends Enum[LoadCheckName] {
  case object ExactColNum extends LoadCheckName("EXACT_COLUMN_NUM")
  case object MinColNum extends LoadCheckName("MIN_COLUMN_NUM")
  case object ColumnsExist extends LoadCheckName("COLUMNS_EXIST")
  case object SchemaMatch extends LoadCheckName("SCHEMA_MATCH")

  override def values: immutable.IndexedSeq[LoadCheckName] = findValues
}
