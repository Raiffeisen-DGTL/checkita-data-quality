package ru.raiffeisen.checkita.utils.io.dbreaders

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import java.sql.{Connection, ResultSet}
import java.util.Properties
import scala.collection.JavaConverters._

trait TableReader {

  protected val connectionUrl: String
  protected val jdbcDriver: String

  /**
   * Runs query and maps result to the desired type
   * Used in SQL checks
   * @param query query to run
   * @param transformOutput transformation function
   * @tparam T desired type
   * @return object of desired type
   */
  def runQuery[T](query: String, transformOutput: ResultSet => T): T

  def getConnection: Connection

  def getUrl: String = connectionUrl

  /**
   * Loads database table to dataframe (using basic jdbc functions)
   * Used in table as a source
   * @param sourceId Id of the source to load
   * @param table Table to load
   * @param query Query to load
   * @param username user name
   * @param password password
   * @param sparkSes spark session
   * @note Either table or query to load must be defined but not both.
   * @return dataframe
   */
  def loadData(sourceId: String,
               table: Option[String],
               query: Option[String],
               username: Option[String],
               password: Option[String])(implicit sparkSes: SparkSession): DataFrame = {
    val connectionProperties = new Properties()

    (username, password) match {
      case (Some(u), Some(p)) =>
        connectionProperties.put("user", u)
        connectionProperties.put("password", p)
      case _ => ()
    }
    connectionProperties.put("driver", jdbcDriver)
    val df = (table, query) match {
      case (Some(t), None) => sparkSes.read.jdbc(connectionUrl, t, connectionProperties)
      case (None, Some(q)) =>
        connectionProperties.put("url", connectionUrl)
        connectionProperties.put("dbtable", s"($q) as $sourceId")
        sparkSes.read.format("jdbc").options(connectionProperties.asScala).load()
      case (t, q) => throw new IllegalArgumentException(
        s"Error loading table source $sourceId: either table or query to load must be defined but not both. " +
          s"Got following: table = $t, query = $q."
      )
    }
    df.select(df.columns.map(c => col(c).as(c.toLowerCase)) : _*)
  }
}
