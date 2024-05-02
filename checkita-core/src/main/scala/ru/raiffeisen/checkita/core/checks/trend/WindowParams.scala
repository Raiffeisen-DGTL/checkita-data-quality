package ru.raiffeisen.checkita.core.checks.trend

import ru.raiffeisen.checkita.config.Enums.TrendCheckRule

/**
 * Trait with time window definitions for trend metrics
 */
trait WindowParams {

  val rule: TrendCheckRule
  val windowSize: String
  val windowOffset: Option[String]

  def windowString: Option[String] = rule match {
    case TrendCheckRule.Record =>
      val windowMsg = s" over $windowSize records back"
      val offsetMsg = windowOffset.map(n => s" with offset of $n records back").getOrElse("")
      Some(windowMsg + offsetMsg + " from last record")
    case TrendCheckRule.Datetime =>
      val windowMsg = s" over duration of $windowSize back"
      val offsetMsg = windowOffset.map(d => s" with duration offset of $d back").getOrElse("")
      Some(windowMsg + offsetMsg + " from referenceDate")
  }
}
