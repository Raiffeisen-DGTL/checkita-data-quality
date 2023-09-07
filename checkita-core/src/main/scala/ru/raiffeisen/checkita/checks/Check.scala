package ru.raiffeisen.checkita.checks

import ru.raiffeisen.checkita.metrics.MetricResult

trait Check {

  def id: String
  def description: String
  def metricsList: Seq[MetricResult]
  def getDescription: String = description
  def getMetrics = s"METRICS[${metricsList.map(_.metricId).mkString(", ")}]"
  def getNumMetrics: Int = metricsList.size

  def addMetricList(metrics: Seq[MetricResult]): Check

  /**
   * Run checks and then report to the reporters.
   */
  def run(): CheckResult
}
