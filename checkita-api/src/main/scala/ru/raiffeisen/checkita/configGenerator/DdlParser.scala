package ru.raiffeisen.checkita.configGenerator


import scala.collection.mutable.{Map => MutableMap}

object DdlParser {

  /**
    DDL parser
   @param ddl
   */
  def parseDDL(ddl: String): (Map[String, String], Map[String, List[Map[String, String]]]) = {
    val ddlLowerCase = ddl.toLowerCase
    val reg = "(?i)stored as ".r
    val location = "(?i) location".r
    val sources: MutableMap[String, String] = MutableMap()
    val notDataType: Seq[String] = List(")", "key", "constraint", "))", "")
    val columnDict: MutableMap[String, List[Map[String, String]]] = MutableMap()

    if (ddlLowerCase.contains("stored as")) {

      val pattern = if (ddlLowerCase.contains("external")) "create external table" else "create table"

      val source = ddlLowerCase.split(pattern, 2)(1).split("\\(", 2)(0).split("\\.")(1).trim
      val splits = reg.split(ddl.split("\\(", 2)(1))
      val columns = splits(0).toLowerCase.split(" \\) comment", 2)(0).split("',")

      val storedAs = location.split(splits(1))(0).trim
      val loc = location.split(splits(1).split(";")(0))(1).trim

      sources("stored_as") = storedAs.toLowerCase.replace(" ", "")
      sources("loc") = loc.replace("'", "")
        .replace("\"", "")
        .replace(" ", "") + "/dlk_cob_date="

      columns.foreach { column =>
        val parts = column.split("\\s+", 4).filter(_.nonEmpty)
        val columnName = if (parts(0).startsWith("`")) parts(0).split("`")(1) else parts(0)
        val dataType = parts(1).toLowerCase.split("\\(")(0)
        val comment = parts(2).split("'")(1)

        columnDict(dataType) = columnDict.getOrElse(dataType, List()) :+ Map(columnName -> comment)
      }
      sources("source") = source.replace("'", "")
        .replace("\"", "")
        .replace(" ", "")
    } else {
      val table = "(?i)table".r
      val splits = ddl.split("\\(", 2)(1).split("\\);")(0)
      val source = table.split(ddl.split("\\(")(0))(1).trim
      val columns = splits.split(",")

      columns.foreach { column =>
        val nullable = if (column.toLowerCase.contains("not null")) "not null" else "null"
        val parts = column.split("\\s+", 4).filter(_.nonEmpty)
        val columnName = parts(0)
        if (parts.length > 1) {
          val dataType = parts(1).toLowerCase.split("\\(")(0)
          if (!notDataType.contains(dataType) && !notDataType.contains(columnName.toLowerCase) && !columnName.contains(")")) {
            columnDict(dataType) = columnDict.getOrElse(dataType, List()) :+ Map(columnName -> nullable)
          }
        }
      }
      sources("source") = source.replace("'", "")
        .replace("\"", "")
        .replace(" ", "")
    }
    (sources.toMap, columnDict.toMap)
  }
}
