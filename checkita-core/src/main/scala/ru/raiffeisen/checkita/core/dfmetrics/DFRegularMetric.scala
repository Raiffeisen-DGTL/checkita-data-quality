package ru.raiffeisen.checkita.core.dfmetrics

import ru.raiffeisen.checkita.core.metrics.RegularMetric

trait DFRegularMetric extends RegularMetric {
  def initDFMetricCalculator: DFMetricCalculator
}
