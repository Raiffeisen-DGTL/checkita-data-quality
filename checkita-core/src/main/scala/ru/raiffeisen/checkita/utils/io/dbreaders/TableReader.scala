package ru.raiffeisen.checkita.utils.io.dbreaders

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import java.sql.{Connection, ResultSet}
import java.util.Properties

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
   * @param table target table
   * @param username user name
   * @param password password
   * @param sparkSes spark session
   * @return dataframe
   */
  def loadData(table: String,
               username: Option[String],
               password: Option[String])(implicit sparkSes: SparkSession): DataFrame = {
    val connectionProperties = new Properties()

    (username, password) match {
      case (Some(u), Some(p)) =>
        connectionProperties.put("user", u)
        connectionProperties.put("password", p)
      case _ =>
    }
    connectionProperties.put("driver", jdbcDriver)
    val df = sparkSes.read.jdbc(connectionUrl, table, connectionProperties)
    df.select(df.columns.map(c => col(c).as(c.toLowerCase)) : _*)
  }
}
