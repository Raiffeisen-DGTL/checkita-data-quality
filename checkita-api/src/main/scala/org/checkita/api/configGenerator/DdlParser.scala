package org.checkita.api.configGenerator

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.CreateTable

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.create.table

import org.checkita.dqf.utils.Logging

object DdlParser extends Logging {

  /**
   * Parses DDL string to extract metadata about a table and its columns.
   *
   * @param ddl The DDL string to be parsed.
   * @param connType The type of database connection (e.g., "oracle", "postgresql", "mysql").
   * @return A tuple containing two maps:
   *         - The first map contains basic table information, including the table's source name, location, and storage format.
   *         - The second map categorizes columns by their data types, with each entry listing the columns and their associated attributes.
   */
  def parseDDL(ddl: String, connType: String): (Map[String, String], Map[String, List[Map[String, String]]]) = {
    val (sources, columnDict) = if (connType == "hive") {
      log.debug("Parsing Hive DDL")

      val statements = ddl.split(";").map(_.trim).filter(_.nonEmpty)
      val initialResult = (Map.empty[String, String], Map.empty[String, List[Map[String, String]]])

      statements.foldLeft(initialResult) { case ((sourcesAcc, columnDictAcc), statement) =>
        try {
          val logicalPlan = CatalystSqlParser.parsePlan(statement)
          logicalPlan match {
            case t: CreateTable =>
              val tableTree = t.name.treeString
              val tableName = if (tableTree.contains("UnresolvedIdentifier")) {
                tableTree.split("\\[")(1).split(",")(1).split("]")(0).trim
              } else {
                t.name.productIterator.next().asInstanceOf[List[Any]].mkString(".")
              }
              val tableLocation = t.tableSpec.location.getOrElse("")
              val tableStorageFormat = t.tableSpec.serde.head.storedAs.getOrElse("")

              val partitionCol = t.partitioning.map { col =>
                col.references().head.toString
              }

              val updatedColumnDict = t.tableSchema.foldLeft(columnDictAcc) { (dict, field) =>
                val fieldType = field.dataType.simpleString
                val correctType = if (fieldType.contains("(")) fieldType.split("\\(")(0) else fieldType
                if (!partitionCol.contains(field.name)) {
                  val fInfo = Map(field.name -> field.getComment().getOrElse(""))
                  val updatedList = dict.getOrElse(correctType, Nil) :+ fInfo
                  dict + (correctType -> updatedList)
                } else dict
              }

              val updatedSources = sourcesAcc ++ Map(
                "source" -> tableName,
                "loc" -> tableLocation,
                "stored_as" -> tableStorageFormat
              )

              (updatedSources, updatedColumnDict)

            case _ => (sourcesAcc, columnDictAcc)
          }
        } catch {
          case e: Exception =>
            log.error(f"Error parsing $statement, error $e")
            (sourcesAcc, columnDictAcc)
        }
      }
    } else {
      log.debug(f"Parsing $connType DDL")
      try {
        val statement: Statement = CCJSqlParserUtil.parse(ddl)
        statement match {
          case t: table.CreateTable =>
            val tableName = t.getTable.getName
            val colsInfo = t.getColumnDefinitions

            val updatedColumnDict = colsInfo.asScala.foldLeft(Map.empty[String, List[Map[String, String]]]) {
              (dict, col) =>
                val dataType = col.getColDataType.getDataType.toLowerCase
                val colSpec = col.getColumnSpecs.toString.toLowerCase
                val nullState = if (colSpec.contains("not") || colSpec.contains("primary")) "not null" else "null"
                val fInfo = Map(col.getColumnName -> nullState)
                val updatedList = dict.getOrElse(dataType, Nil) :+ fInfo
                dict + (dataType -> updatedList)
            }

            val updatedSources = Map("source" -> tableName)
            (updatedSources, updatedColumnDict)

          case _ => (Map.empty[String, String], Map.empty[String, List[Map[String, String]]])
        }
      } catch {
        case e: Exception =>
          log.error(f"Error parsing, error $e")
          (Map.empty[String, String], Map.empty[String, List[Map[String, String]]])
      }
    }

    (sources, columnDict)
  }
}
