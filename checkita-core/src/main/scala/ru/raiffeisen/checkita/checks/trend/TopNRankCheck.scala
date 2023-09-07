package ru.raiffeisen.checkita.checks.trend

import ru.raiffeisen.checkita.checks._
import ru.raiffeisen.checkita.exceptions.IllegalConstraintResultException
import ru.raiffeisen.checkita.metrics.{ColumnMetricResultFormatted, MetricResult}
import ru.raiffeisen.checkita.utils.{DQSettings, toUtcTimestamp}
import ru.raiffeisen.checkita.utils.io.dbmanager.DBManager

import java.sql.Timestamp
import scala.util.Try

case class TopNRankCheck(id: String,
                         description: String,
                         metrics: Seq[MetricResult],
                         rule: String,
                         threshold: Double,
                         timeWindow: Int)(
                          implicit resultsWriter: DBManager,
                          settings: DQSettings)
  extends Check {

  def calculateJaccardDistance(set1: Set[String], set2: Set[String]): Double = {
    1 - set1.intersect(set2).size.toFloat / set1.union(set2).size
  }

  override def metricsList: Seq[MetricResult] = metrics

  override def addMetricList(metrics: Seq[MetricResult]): TopNRankCheck =
    TopNRankCheck(id,
      description,
      metrics,
      rule,
      threshold,
      timeWindow)

  override def run(): CheckResult = {

    val baseMetricResult: MetricResult = metrics.head

    val jobId: String = baseMetricResult.jobId

    val metricIds: List[String] = metrics.map(x => x.metricId).toList

    val currMetResult = metrics.map(x => x.additionalResult).toSet

    val startTS: Timestamp = toUtcTimestamp(settings.referenceDate)

    val dbMetResults: Seq[ColumnMetricResultFormatted] =
      resultsWriter.loadColMetFmtResults(
        metricIds,
        jobId,
        rule,
        timeWindow,
        startTS)
    val grouped: Iterable[Set[String]] = dbMetResults
      .map(res => Map(res.referenceDate -> Set(res.additionalResult)))
      .reduce((x, y) =>
        x ++ y.map { case (k, v) => k -> (v ++ x.getOrElse(k, Set.empty)) })
      .values

    // todo Longer window calculation
    val dist = calculateJaccardDistance(currMetResult, grouped.head)
    val checkStatus =
      CheckUtil.tryToStatus[Double](Try(dist), d => d <= threshold)

    val statusString = checkStatus match {
      case CheckSuccess =>
        s"$dist <= $threshold"
      case CheckFailure =>
        s"$dist > $threshold (failed: Difference is ${threshold - dist})"
      case CheckError(throwable) =>
        s"Checking ${baseMetricResult.result} = $threshold error: $throwable"
      case _ => throw IllegalConstraintResultException(id)
    }

    val checkMessage = CheckMessageGenerator(
      baseMetricResult,
      threshold,
      checkStatus,
      statusString,
      id,
      subType,
      Some(rule),
      Some(timeWindow)
    )

    val cr = CheckResult(
      this.id,
      subType,
      this.description,
      baseMetricResult.sourceId,
      baseMetricResult.metricId,
      None,
      threshold,
      None,
      None,
      checkStatus.stringValue,
      checkMessage.message
    )

    cr
  }

  val subType = "TOP_N_RANK_CHECK"
}

