package org.checkita.core.metrics.df

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

object Helpers {

  sealed abstract class DFMetricOutput(override val entryName: String) extends EnumEntry
  object DFMetricOutput extends Enum[DFMetricOutput] {
    case object Result extends DFMetricOutput("result")
    case object Errors extends DFMetricOutput("errors")
    case object GroupResult extends DFMetricOutput("groupResult")
    case object GroupErrors extends DFMetricOutput("groupErrors")
    override def values: immutable.IndexedSeq[DFMetricOutput] = findValues
  }


  def addColumnSuffix(columnName: String, suffix: String): String =
    if (columnName.endsWith("`")) columnName.dropRight(1) + "_" + suffix + "`"
    else columnName + "_" + suffix

  def dropColumnSuffix(columnName: String, suffix: String): String =
    if (columnName.endsWith("`")) columnName.dropRight(suffix.length + 2) + "`"
    else columnName.dropRight(suffix.length + 1)
  
  def tripleBackticks(columnName: String): String = columnName.replace("`", "```")
  
  def withKeyFields(columns: Seq[String], keyFields: Seq[String]): Seq[String] =
    keyFields ++ columns.filterNot(keyFields.contains)
}
