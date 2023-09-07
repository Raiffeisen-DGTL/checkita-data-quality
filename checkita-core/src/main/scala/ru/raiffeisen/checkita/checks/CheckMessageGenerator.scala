package ru.raiffeisen.checkita.checks

import ru.raiffeisen.checkita.metrics._


case class CheckMessageGenerator(metricRes: MetricResult,
                                 threshold: Double,
                                 status: CheckStatus,
                                 statusString: String,
                                 checkId: String,
                                 checkSubtype: String,
                                 rule: Option[String] = None,
                                 timewindow: Option[Int] = None) extends CheckMessage {
  val message: String = {

    val metricName = metricRes.metricName

    val onFile = metricRes.getType match {
      case DQResultTypes.column =>
        val mm = metricRes.asInstanceOf[ColumnMetricResult]
        s"on column ${mm.sourceId}[${mm.columnNames}]"
      case DQResultTypes.file =>
        s"on file ${metricRes.asInstanceOf[FileMetricResult].sourceId}"
      case DQResultTypes.composed =>
        s"on file ${metricRes.asInstanceOf[ComposedMetricResult].sourceId}"
    }

    val timeOptionalSting = (rule, timewindow) match {
      case (Some("record"), Some(x)) => s" over $x records back "
      case (Some("date"), Some(x))   => s" over $x days back "
      case _                         => " "
    }

    s"Check $checkId for metric $metricName$timeOptionalSting$onFile check if (MetricResult) ${metricRes.result} is $checkSubtype $threshold (compareMetric/threshold). Result: ${status.stringValue}. CheckStatus: $statusString."
  }
}
