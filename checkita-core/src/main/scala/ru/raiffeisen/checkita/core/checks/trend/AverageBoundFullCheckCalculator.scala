package ru.raiffeisen.checkita.core.checks.trend
import ru.raiffeisen.checkita.config.Enums.TrendCheckRule
import ru.raiffeisen.checkita.core.Results.MetricCalculatorResult
import ru.raiffeisen.checkita.core.checks.CheckName

/**
 * `Average bound FULL` check calculator: verifies if metric value differs from its 
 * historical average value by less than a given threshold
 * @param checkId Check ID
 * @param baseMetric Base metric ID
 * @param compareThreshold Difference threshold
 * @param rule Rule to build time window (either record or duration)
 * @param windowSize Size of the window to pull historical results
 * @param windowOffset Offset current date/record
 */
case class AverageBoundFullCheckCalculator(checkId: String,
                                           baseMetric: String,
                                           compareThreshold: Double,
                                           rule: TrendCheckRule,
                                           windowSize: String,
                                           windowOffset: Option[String]
                                          ) extends AverageBoundCheckCalculator {

  override val lThreshold: Option[Double] = Some(compareThreshold)
  override val uThreshold: Option[Double] = Some(compareThreshold)
  override val checkName: CheckName = CheckName.AverageBoundFull

  override protected val compareFuncSuccessRepr: (Double, Double) => String = (base, avg) => 
    s"Metric value of $base is within interval [${lBound(avg).getOrElse("Nan")}, ${uBound(avg).getOrElse("NaN")}]" +
      s" obtained from average metric result of $avg and threshold of $compareThreshold."
  
  override protected val compareFuncFailureRepr: (Double, Double) => String = (base, avg) =>
    s"Metric value of $base is outside of interval [${lBound(avg).getOrElse("Nan")}, ${uBound(avg).getOrElse("NaN")}]" +
      s" obtained from average metric result of $avg and threshold of $compareThreshold."

  /**
   * Gets check details message to insert into final check message.
   *
   * @param compareMetricResult Compare metric result
   * @return Check details message
   */
  protected def getDetailsMsg(compareMetricResult: Option[MetricCalculatorResult]): Option[String] = for {
    lt <- lThreshold
    _ <- uThreshold
  } yield s"differs from average metric value over a given window by less than (threshold) $lt."
  
}