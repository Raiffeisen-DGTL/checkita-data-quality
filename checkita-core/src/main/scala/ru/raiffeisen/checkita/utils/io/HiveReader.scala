package ru.raiffeisen.checkita.utils.io

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import ru.raiffeisen.checkita.sources.HiveTableConfig
import ru.raiffeisen.checkita.utils.{Logging, log}

import scala.util.Try


/**
 * Hive manager
 * Hive context was deprecated since Spark 2.0 and is replaced with SparkSession (with enabled hive support).
 */
object HiveReader extends Logging {
  /**
   * Runs datasource query in Hive
   * @param inputConf Hive datasource configuration
   * @param spark Spark session
   * @return result of the query
   */
  def loadHiveTable(inputConf: HiveTableConfig)(
    implicit spark: SparkSession): Seq[DataFrame] = {

    // You can specify a template for queries here. Currently it's just an input query as it is
    val full_query = inputConf.query
    Try {
      val df = spark.sql(full_query)
      Seq(
        df.select(df.columns.map(c => col(c).as(c.toLowerCase)) : _*)
      )
    }.getOrElse({
      log.warn("Failed to load HIVE table")
      Seq.empty
    })
  }
}

