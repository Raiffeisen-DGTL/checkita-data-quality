package ru.raiffeisen.checkita.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait DQSparkContext {
  protected def makeSparkSession(settings: DQSettings): SparkSession = {
    SparkSession.builder().config(settings.sparkConf).enableHiveSupport().getOrCreate()
  }
  protected def makeSparkContext(sparkSession: SparkSession): SparkContext = sparkSession.sparkContext
}
