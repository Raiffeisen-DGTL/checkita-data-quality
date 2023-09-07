package ru.raiffeisen.checkita.configs

import org.apache.spark.sql.types.DataType

/**
 * Representation of columns for schema parsing
 */
sealed abstract class GenStructColumn {
  def getType: String
  def colName: String
  def colType: DataType
}

case class StructColumn(colName: String, colType: DataType) extends GenStructColumn {
  def getType = "StructColumn"
}

case class StructFixedColumn(colName: String, colType: DataType, length: Int) extends GenStructColumn {
  def getType = "StructFixedColumn"
}

case class MetricParameter(name: String, typ: String, isOptional: Boolean = false)

case class MetricConfig(name: String, parameters: Seq[MetricParameter] = Seq.empty) {
  override def toString: String = this.name
}

trait ConfigEnumRefl[T] {
  val values: List[T]
  def getValues(C: Object, exclude: Seq[String] = Seq.empty): List[T] = {
    val fields = C.getClass.getDeclaredFields.filter { f =>
      f.getName != "values" && f.getName != "getValues" && !f.getName.contains("$") && !exclude.contains(f.getName)
    }.toList
    fields.map{ f =>
      f.setAccessible(true)
      f.get(C).asInstanceOf[T]
    }
  }

  /**
   * Search string within values by type T string representation
   */
  def contains(elem: String): Boolean = this.values.map(_.toString).contains(elem)
}

object SchemaFormats extends ConfigEnumRefl[String] {
  val fixedFull: String = "fixedFull"
  val fixedShort: String = "fixedShort"
  val delimited: String = "delimited"
  val values: List[String] = getValues(this)
}

object SchemaParameters extends ConfigEnumRefl[String] {
  val colName: String = "name"
  val colType: String = "type"
  val colLength: String = "length"
  val values: List[String] = getValues(this)
}