package org.checkita.targets

object TargetTypes {
  
  trait ResultTargetType { val targetType: String = "results" }
  trait MetricErrorsTargetType { val targetType: String = "metricErrors" }
  trait SummaryTargetType { val targetType: String = "summary" }
  trait CheckAlertTargetType { val targetType: String = "checkAlert" }

}
