package ru.raiffeisen.checkita.targets.builders

import ru.raiffeisen.checkita.storage.Models.ResultMetricErrors

import scala.util.Try

trait BuildHelpers {

  /**
   * Filters errors to be send in target.
   * @param errors Sequence of all collected metric errors
   * @param requested Sequence of metric IDs for which metric errors are requested
   * @param dumpSize Maximum number of errors to be collected per each metric
   * @return Filtered sequence of metric errors
   * @note If requested metric IDs are not provided (sequence is empty) then errors for all metrics are returned.
   */
  def filterErrors(errors: Seq[ResultMetricErrors],
                   requested: Seq[String],
                   dumpSize: Int): Seq[ResultMetricErrors] =
    errors.filter(r => if (requested.isEmpty) true else requested.contains(r.metricId)) // retain only requested metrics
      .groupBy(_.metricId).toSeq.zipWithIndex.filter(t => t._2 < dumpSize) // retain no more than 'dumpSize' errors per metric
      .flatMap(_._1._2)

  /**
   * Safely reads template from file
   * @param uri Path to a template file
   * @return Template string read from path or None if read operation wasn't successful.
   */
  def readTemplate(uri: String): Option[String] = Try {
    val src = scala.io.Source.fromFile(uri)
    val text = src.mkString
    src.close()
    text
  }.toOption
}
