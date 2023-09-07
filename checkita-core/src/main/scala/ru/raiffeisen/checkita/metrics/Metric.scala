package ru.raiffeisen.checkita.metrics

sealed trait Metric {
  def id: String
  def name: String
  def description: String
  def paramMap: Map[String, Any]
}

case class ColumnMetric(
                         id: String,
                         name: String,
                         description: String,
                         source: String,
                         columns: Seq[String],
                         paramMap: Map[String, Any],
                         positions: Seq[Int] = Seq.empty
                       ) extends Metric {
  if (positions.nonEmpty && positions.size != columns.size) throw new IllegalArgumentException("paramMap.size != columns.size")
}

case class FileMetric(
                       id: String,
                       name: String,
                       description: String,
                       source: String,
                       paramMap: Map[String, Any]
                     ) extends Metric

case class ComposedMetric(
                           id: String,
                           name: String,
                           description: String,
                           formula: String,
                           paramMap: Map[String, Any]
                         ) extends Metric

case class ConfigMetric(
                         id: String,
                         name: String,
                         description: String,
                         paramMap: Map[String, Any]
                       ) extends Metric
