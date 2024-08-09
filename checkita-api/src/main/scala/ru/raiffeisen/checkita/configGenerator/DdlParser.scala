package ru.raiffeisen.checkita.configGenerator

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map => MutableMap}

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.CreateTable

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.create.table

import ru.raiffeisen.checkita.utils.Logging

object DdlParser extends Logging {

  /**
   * Parses DDL string to extract metadata about a table and its columns.
   *
   * @param ddl The DDL string to be parsed.
   * @return A tuple containing two maps:
   *         - The first map contains basic table information, including the table's source name, location, and storage format.
   *         - The second map categorizes columns by their data types, with each entry listing the columns and their associated attributes.
   */
  def parseDDL(ddl: String): (Map[String, String], Map[String, ListBuffer[Map[String, String]]]) = {
    var sources: MutableMap[String, String] = MutableMap()
    val columnDict: MutableMap[String, ListBuffer[Map[String, String]]] = MutableMap()

    if (ddl.toLowerCase.contains("stored as")) {
      log.debug("Parsing Hive DDL")

      val statements = ddl.split(";").map(_.trim).filter(_.nonEmpty)
      var tableName: Option[String] = None
      var tableLocation: Option[String] = None
      var tableStorageFormat: Option[String] = None
      val partitionCol: ListBuffer[String] = ListBuffer()

      statements.foreach { statement =>
        try {
          val logicalPlan = CatalystSqlParser.parsePlan(statement)
          logicalPlan match {
            case t: CreateTable =>
              val tableTree = t.name.treeString
              tableName = Some(
                if (tableTree.contains("UnresolvedIdentifier")) {
                  tableTree.split("\\[")(1).split(",")(1).split("]")(0).trim
                } else t.name.productIterator.next().asInstanceOf[List[Any]].mkString(".")
              )
              tableLocation = t.tableSpec.location
              tableStorageFormat = t.tableSpec.serde.head.storedAs
              t.partitioning.map { col =>
                partitionCol += col.references().head.toString
              }

              t.tableSchema.foreach { field =>
                val fieldType = field.dataType.simpleString
                val correctType = if (fieldType.contains("(")) fieldType.split("\\(")(0) else fieldType
                if (!partitionCol.contains(field.name)) {
                  val fInfo = Map(field.name -> field.getComment().getOrElse(""))
                  if (columnDict.contains(correctType)) {
                    columnDict(correctType) += fInfo
                  } else {
                      columnDict(correctType) = ListBuffer(fInfo)
                  }
                }
              }
          }
        } catch {
          case e: Exception => log.error(f"Error parsing $statement, error $e")
        }
      }
      sources = mutable.Map(
        "source" -> tableName.getOrElse(""),
        "loc" -> tableLocation.map(_ + "/dlk_cob_date=").getOrElse(""),
        "stored_as" -> tableStorageFormat.getOrElse("")
      )
    } else {
        log.debug("Parsing other DDL")
        try {
          val statement: Statement = CCJSqlParserUtil.parse(ddl)
          statement match {
            case t: table.CreateTable =>
              sources("source") = t.getTable.getName
              val colsInfo = t.getColumnDefinitions
              colsInfo.forEach { col =>
                val dataType = col.getColDataType.getDataType.toLowerCase
                val colSpec = col.getColumnSpecs.toString.toLowerCase
                val nullState = if (colSpec.contains("not") | colSpec.contains("primary")) "not null" else "null"
                val fInfo = Map(col.getColumnName -> nullState)

                if (columnDict.contains(dataType)) {
                  columnDict(dataType) += fInfo
                } else {
                    columnDict(dataType) = ListBuffer(fInfo)
                }
              }
          }
        } catch {
          case e: Exception => log.error(f"Error parsing, error $e")
        }
    }
    (sources.toMap, columnDict.toMap)
  }
}
