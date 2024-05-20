package ru.raiffeisen.checkita.core.checks

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

/**
 * Enumeration holding all check calculator names
 *
 * @param entryName check calculator name
 * @param needStorage Boolean flag indicating whether metric requires connection to Storage in order to be evaluated.
 */
sealed abstract class CheckName(override val entryName: String, val needStorage: Boolean) extends EnumEntry
object CheckName extends Enum[CheckName] {
  case object DifferByLT extends CheckName("DIFFER_BY_LT", false)
  case object EqualTo extends CheckName("EQUAL_TO", false)
  case object GreaterThan extends CheckName("GREATER_THAN", false)
  case object LessThan extends CheckName("LESS_THAN", false)
  case object AverageBoundFull extends CheckName("AVERAGE_BOUND_FULL", true)
  case object AverageBoundLower extends CheckName("AVERAGE_BOUND_LOWER", true)
  case object AverageBoundUpper extends CheckName("AVERAGE_BOUND_UPPER", true)
  case object AverageBoundRange extends CheckName("AVERAGE_BOUND_RANGE", true)
  case object TopNRank extends CheckName("TOP_N_RANK", true)
  
  override def values: immutable.IndexedSeq[CheckName] = findValues
}
