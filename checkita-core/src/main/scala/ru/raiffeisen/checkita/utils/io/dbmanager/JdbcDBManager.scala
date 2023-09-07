package ru.raiffeisen.checkita.utils.io.dbmanager

import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.metrics.{
  ColumnMetricResult, ColumnMetricResultFormatted, ComposedMetricResult, FileMetricResult
}
import ru.raiffeisen.checkita.sources.DatabaseConfig
import ru.raiffeisen.checkita.utils.{DQSettings, Logging, camelToUnderscores, makeTableName}

import java.sql.{Array, Connection, ResultSet, Timestamp}
import java.time.ZoneId


/**
 * JDBC Manager to do operations with history database.
 */
final class JdbcDBManager(settings: DQSettings) extends DBManager with Logging {
  type R = ResultSet

  private val dbConfig: Option[DatabaseConfig] = settings.resStorage.map(d => DatabaseConfig(
    d.id,
    d.subtype,
    d.host.get,
    d.port,
    d.service,
    d.user,
    d.password,
    d.schema
  ))

  private val connection: Option[Connection] = if (dbConfig.isDefined) Some(dbConfig.get.getConnection) else None

  /**
   * Saves metric results to a specific table
   * Main idea is that method is class independent. It's automatically get the class fields and creates
   * JDBC statement to fill and execute.
   * @param metrics Sequence of metric results
   * @param tb Target table
   */
  def saveResultsToDB(metrics: Seq[AnyRef], tb: String): Unit = {
    if (dbConfig.isDefined && connection.isDefined) {
      val table = makeTableName(dbConfig.get.schema, tb)
      log.info(s"Saving '$table'")
      try {
        val fieldNames = metrics.head.getClass.getDeclaredFields
        val fieldStructString: String = fieldNames
          .map(field => camelToUnderscores(field.getName))
          .mkString(" (", ", ", ") ")
        val paramString: String =
          List.fill(fieldNames.length)("?").mkString(" (", ", ", ")")

        val insertSql = "INSERT INTO " + table + fieldStructString + "VALUES" + paramString

        val statement = connection.get.prepareStatement(insertSql)

        metrics.foreach(res => {
          fieldNames.zip(Stream from 1).foreach{f =>
            f._1.setAccessible(true)
            val value: Any = f._1.get(res)
            value match {
              case x: Seq[_] => // the only type of sequence supported is the sequence of string.
                val xs: Seq[String] = x
                  .filter(_ match {
                    case _: String => true
                    case _         => false
                  })
                  .asInstanceOf[Seq[String]]
                val array: Array = connection.get.createArrayOf("text", xs.toArray)
                statement.setArray(f._2, array)
              case x: String => statement.setString(f._2, x)
              case x: Int => statement.setInt(f._2, x)
              case x: Double => statement.setDouble(f._2, x)
              case x: Boolean => statement.setBoolean(f._2, x)
              case x: Timestamp => statement.setTimestamp(f._2, x)
            }
          }
          statement.addBatch()
        })

        statement.executeBatch()
        log.info("Success!")
      } catch {
        case _: NoSuchElementException =>
          log.warn("Nothing to save!")
        case e: Exception =>
          log.error("Failed with error:")
          log.error(e.toString)
      }
    } else log.warn("History database is not connected. Avoiding saving the results...")

  }

  /**
   * Loads metric results of previous runs from the history database. Used in trend check processing.
   * @param metricSet Set of requested metrics (set of their ids)
   * @param jobId Job ID of metrics
   * @param rule Rule for result selection. Should be "date" or "record"
   * @param tw Requested time window
   *           For "date" selection rule will select from a window [startDate - tw * days, startDate]
   *           For "record" selection rule will select tw records from before starting date
   * @param tb History database table to load results from
   * @param startDate Start date to selection. Keep in mind that all the time windows are retrospective,
   *                  so the start date is actually a latest one
   * @param f Mapping function from ResultSet to sequence of metric results.
   * @tparam T Requested result type
   * @return lazy sequence of Metric results
   */
  def loadResults[T](metricSet: List[String],
                     jobId: String,
                     rule: String,
                     tw: Int,
                     startDate: Timestamp,
                     tb: String)(f: R => Seq[T]): Seq[T] = {
    if (dbConfig.isDefined && connection.isDefined) {
      val table = makeTableName(dbConfig.get.schema, tb)

      log.info(s"Loading results from $table...")

      val metricIdString =
        List.fill(metricSet.length)("?").mkString("(", ", ", ")")

      val selectSQL = rule match {
        case "record" =>
          // looks a bit bulky and, probably, there is a way to do the same completely with JDBC statements
          s"SELECT * FROM $table WHERE metric_id IN $metricIdString AND reference_date <= '$startDate' AND job_id = '$jobId' ORDER BY reference_date DESC LIMIT ${tw * metricSet.length}"
        case "date" =>
          val lastDate =Timestamp.from(startDate.toInstant.atZone(ZoneId.of("UTC")).minusDays(tw).toInstant)
          s"SELECT * FROM $table WHERE metric_id IN $metricIdString AND reference_date >= '$lastDate' AND reference_date <= '$startDate' AND job_id = '$jobId'"
        case x => throw IllegalParameterException(x)
      }

      val statement = connection.get.prepareStatement(selectSQL)

      metricSet.zip(Stream from 1).foreach(x => statement.setString(x._2, x._1))
      val results: ResultSet = statement.executeQuery()

      val resSet = f(results)
      log.info(s"Results found: ${resSet.length}")
      resSet
    } else {
      log.warn("History database is not connected. Providing empty historical results...")
      Seq.empty[T]
    }
  }

  /**
   * Closes connection with the local database (obviously)
   * Defined since the connection is private
   */
  def closeConnection(): Unit = {
    if (connection.isDefined) connection.get.close()
  }

  def colMetConverter(r: R): Seq[ColumnMetricResult] = {
    new Iterator[ColumnMetricResult] {
      def hasNext: Boolean = r.next()
      def next(): ColumnMetricResult = {
        val columIds = r.getString(6).filterNot("{}".toSet).split(", ").toSeq
        ColumnMetricResult(
          r.getString(1),
          r.getString(2),
          r.getString(3),
          r.getString(4),
          r.getString(5),
          columIds,
          r.getString(7),
          r.getDouble(8),
          r.getString(9)
        )
      }
    }.toSeq
  }

  def colMetFmtConverter(r: R): Seq[ColumnMetricResultFormatted] = {
    new Iterator[ColumnMetricResultFormatted] {
      def hasNext: Boolean = r.next()
      def next(): ColumnMetricResultFormatted = ColumnMetricResultFormatted(
        r.getString(1),
        r.getString(2),
        r.getString(3),
        r.getString(4),
        r.getString(5),
        r.getString(6),
        r.getString(7),
        r.getDouble(8),
        r.getString(9),
        r.getTimestamp(10),
        r.getTimestamp(11)
      )
    }.toSeq
  }

  def fileMetConverter(r: R): Seq[FileMetricResult] = {
    new Iterator[FileMetricResult] {
      def hasNext: Boolean = r.next()
      def next(): FileMetricResult = FileMetricResult(
        r.getString(1),
        r.getString(2),
        r.getString(3),
        r.getString(4),
        r.getString(5),
        r.getDouble(6),
        r.getString(7)
      )
    }.toSeq
  }

  def compMetConverter(r: R): Seq[ComposedMetricResult] = {
    new Iterator[ComposedMetricResult] {
      def hasNext: Boolean = r.next()
      def next(): ComposedMetricResult = ComposedMetricResult(
        r.getString(1),
        r.getString(2),
        r.getString(3),
        r.getString(4),
        r.getString(5),
        r.getString(6),
        r.getDouble(7),
        r.getString(8)
      )
    }.toSeq
  }
}
