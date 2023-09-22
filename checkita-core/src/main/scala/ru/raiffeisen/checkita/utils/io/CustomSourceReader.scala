package ru.raiffeisen.checkita.utils.io

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.raiffeisen.checkita.sources.CustomSourceConfig
import ru.raiffeisen.checkita.utils.{Logging, optionSeqToMap}

import scala.util.{Failure, Success, Try}

object CustomSourceReader extends Logging {

  /**
   * Loads Custom Source
   * @param inputConf input configuration
   * @param spark spark session
   * @return sequence of dataframes
   */
  def load(inputConf: CustomSourceConfig)(implicit spark: SparkSession): Seq[DataFrame] = {
    val tryToDf = Try {
      log.info(s"Reading custom source ${inputConf.id}...")
      val readOptions = optionSeqToMap(inputConf.options)
      val preDf = spark.read.format(inputConf.format).options(readOptions)
      inputConf.path match {
        case Some(loadPath) => preDf.load(loadPath)
        case None => preDf.load()
      }
    }

    tryToDf match {
      case Success(df) =>
        log.info(s"Successfully read custom source ${inputConf.id}.")
        Seq(df)
      case Failure(err) =>
        log.error(s"Unable to load custom source ${inputConf.id} due to following error:\n${err.getMessage}")
        Seq.empty[DataFrame]
    }
  }
}

