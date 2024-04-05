package ru.raiffeisen.checkita.core.dfmetrics

import ru.raiffeisen.checkita.core.metrics.RegularMetric
import ru.raiffeisen.checkita.core.metrics.df.DFMetricCalculator

trait DFRegularMetric extends RegularMetric {
  def initDFMetricCalculator: DFMetricCalculator
}
