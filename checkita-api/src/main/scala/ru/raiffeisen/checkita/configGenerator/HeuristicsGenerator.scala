package ru.raiffeisen.checkita.configGenerator

import scala.collection.mutable.ListBuffer
import scala.util.Try

import ru.raiffeisen.checkita.configGenerator.DdlParser.parseDDL
import ru.raiffeisen.checkita.configGenerator.HeuristicsConstants._
import ru.raiffeisen.checkita.utils.ResultUtils._

/**  */
object HeuristicsGenerator {
  def heuristics(ddl: String, connType: String): Result[String] = {
    Try {
      val (sourcesMap, parsedDdl) = parseDDL(ddl)
      val notNullableCols: ListBuffer[String] = ListBuffer()
      val duplicatesCols: ListBuffer[String] = ListBuffer()
      val completenessCols: ListBuffer[String] = ListBuffer()
      val source = sourcesMap("source")

      val result: ListBuffer[String] = ListBuffer(rowCount.replaceAll("%s", source))
      val greaterThan = greaterThanConst.replaceAll("%s", source)
      val equalTo: ListBuffer[String] = ListBuffer()
      val lessThan: ListBuffer[String] = ListBuffer()
      val colCnt: ListBuffer[Int] = ListBuffer()
      val metricsForTarget: ListBuffer[String] = ListBuffer("check_row_cnt")
      val metrics: ListBuffer[String] = ListBuffer()
      val equalToResult: ListBuffer[String] = ListBuffer()

      parsedDdl.values.foreach { columns =>
        columns.foreach { values =>
          val key = Seq(values.keys).head.mkString
          val value = Seq(values.values).head.toString
          colCnt += 1
          if (distinctCols.contains(key)) {
            duplicatesCols += key
          }
          if (key == "cnum") {
            result += cnumRegex.replaceAll("%s", source)
            equalTo += equalToCnum.replace("%s", source)
          }
          if (value == "not null" || distinctCols.contains(key)) {
            notNullableCols += key
          } else {
            completenessCols += key
          }
        }
      }

      val colNum = colNumConst.replaceAll("%s", source).replace("%a", colCnt.length.toString)

      if (duplicatesCols.nonEmpty) {
        result += duplicateVal.replaceAll("%s", source).replace("%a", duplicatesCols.mkString(", "))
        equalTo += equalToDuplicates.replaceAll("%s", source)
        metricsForTarget += s"check_duplicates_$source"
      }
      if (completenessCols.nonEmpty) {
        result += completeness.replaceAll("%s", source).replace("%a", completenessCols.mkString(", "))
        equalTo += equalToCompleteness.replaceAll("%s", source)
        metricsForTarget += s"check_completeness_$source"
      }
      if (notNullableCols.nonEmpty) {
        result += nullVal.replaceAll("%s", source).replace("%a", notNullableCols.mkString(", "))
        val composed = composedConstants.replaceAll("%s", source)
        lessThan += lessThanPct.replace("%s", source)
        metricsForTarget += "check_pct_of_null"
        equalTo += equalToNulls.replaceAll("%s", source)
        metricsForTarget += s"check_nulls_$source"
        metrics += s"metrics : {regular: {${result.mkString(",")}}}, {$composed}"
      } else {
        metrics += s"metrics : {regular: {${result.mkString(",")}}}"
      }

      val connections = connType match {
        case "sqlite" => f"""connections:{{sqlite: [{{id: "$connType%s_id", url: "CHANGE"}}]}},"""
        case "kafka" => f"""connections: {kafka: [{id: "$connType%s_id", servers: ["CHANGE"]}]},"""
        case _ if tableType.contains(connType) =>
          f"""connections:{$connType: [{id: "$connType%s_id", url: "CHANGE", username: "CHANGE", password: "CHANGE"}]},"""
        case _ => ""
      }

      val sources = if (tableType.contains(connType) || connType == "sqlite") {
        f"""table: [{id: $source, connection: "$connType%s_id", table: "$source"}]"""
      } else {
        f"""file: [{id: "$source", kind: "${sourcesMap("stored_as")}", path: "${sourcesMap("loc")}"}]"""
      }

      equalToResult += equalToConst.replace("%s", equalTo.mkString(", "))

      val snapshots = if (lessThan.nonEmpty) {
        equalTo + ", " + greaterThan + ", " + lessThan
      } else {
        equalToResult.mkString(", ") + ", " + greaterThan
      }

      val targets = targetsEmail.replace("%s", metricsForTarget.mkString(", "))
      jobConfigConst.replace("%a", source)
        .replace("%b", connections)
        .replace("%c", sources)
        .replace("%d", colNum)
        .replace("%e", metrics.mkString(", "))
        .replace("%f", snapshots)
        .replace("%g", targets)
    }.toResult(preMsg = "Unable to get Job Configuration due to following error:")
  }
}