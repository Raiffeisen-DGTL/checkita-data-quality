package org.checkita.dqf.storage

import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.IO.readJobConfig
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.utils.Common.camelToSnakeCase
import shapeless.{::, HList, HNil}

import java.sql.Timestamp

// store arrays as string: '[val1, val2, val3]'

object Models {

  type GeneralUniqueResult = String :: String :: Timestamp :: HNil
  type RegularMetricUniqueResult = String :: String :: String :: Timestamp :: HNil
  type JobUniqueResult = String :: Timestamp :: HNil

  sealed abstract class DQEntity extends Product {
    val jobId: String // mandatory for all entities
    val entityType: String // required for serialization
    val uniqueFields: HList
    val uniqueFieldNames: Seq[String]
  }

  trait DescriptiveFields { this: DQEntity =>
    val description: Option[String]  // optional description that eny result entity can have
    val metadata: Option[String]  // optional user-defined metadata that any result entity can have
  }

  sealed abstract class MetricResult extends DQEntity with DescriptiveFields {
    val jobId: String
    val metricId: String
    val metricName: String
    val sourceId: String
    val result: Double
    val additionalResult: Option[String]
    val referenceDate: Timestamp
    val executionDate: Timestamp
  }

  sealed abstract class CheckResult extends DQEntity with DescriptiveFields {
    val jobId: String
    val checkId: String
    val checkName: String
    val sourceId: String
    val status: String
    val message: Option[String]
    val isCritical: Boolean
    val referenceDate: Timestamp
    val executionDate: Timestamp

    val uniqueFields: GeneralUniqueResult = jobId :: checkId :: referenceDate :: HNil
    val uniqueFieldNames: Seq[String] = Seq("jobId", "checkId", "referenceDate").map(camelToSnakeCase)
  }

  final case class ResultMetricRegular(jobId: String,
                                       metricId: String,
                                       metricName: String,
                                       description: Option[String],
                                       metadata: Option[String],
                                       sourceId: String,
                                       columnNames: Option[String],
                                       params: Option[String],
                                       result: Double,
                                       additionalResult: Option[String],
                                       referenceDate: Timestamp,
                                       executionDate: Timestamp) extends MetricResult {
    val entityType: String = "regularMetricResult"
    val uniqueFields: RegularMetricUniqueResult = jobId :: metricId :: metricName :: referenceDate :: HNil
    val uniqueFieldNames: Seq[String] = Seq("jobId", "metricId", "metricName", "referenceDate").map(camelToSnakeCase)
  }

  final case class ResultMetricComposed(jobId: String,
                                        metricId: String,
                                        metricName: String,
                                        description: Option[String],
                                        metadata: Option[String],
                                        sourceId: String,
                                        formula: String,
                                        result: Double,
                                        additionalResult: Option[String],
                                        referenceDate: Timestamp,
                                        executionDate: Timestamp) extends MetricResult {
    val entityType: String = "composedMetricResult"
    val uniqueFields: GeneralUniqueResult = jobId :: metricId :: referenceDate :: HNil
    val uniqueFieldNames: Seq[String] = Seq("jobId", "metricId", "referenceDate").map(camelToSnakeCase)
  }

  final case class ResultMetricTrend(jobId: String,
                                     metricId: String,
                                     metricName: String,
                                     description: Option[String],
                                     metadata: Option[String],
                                     sourceId: String,
                                     params: Option[String],
                                     result: Double,
                                     additionalResult: Option[String],
                                     referenceDate: Timestamp,
                                     executionDate: Timestamp) extends MetricResult {
    val entityType: String = "trendMetricResult"
    val uniqueFields: GeneralUniqueResult = jobId :: metricId :: referenceDate :: HNil
    val uniqueFieldNames: Seq[String] = Seq("jobId", "metricId", "referenceDate").map(camelToSnakeCase)
  }

  final case class ResultCheck(jobId: String,
                               checkId: String,
                               checkName: String,
                               description: Option[String],
                               metadata: Option[String],
                               sourceId: String,
                               baseMetric: String,
                               comparedMetric: Option[String],
                               comparedThreshold: Option[Double],
                               lowerBound: Option[Double],
                               upperBound: Option[Double],
                               status: String,
                               message: Option[String],
                               isCritical: Boolean,
                               referenceDate: Timestamp,
                               executionDate: Timestamp) extends CheckResult {
    val entityType: String = "checkResult"
  }

  final case class ResultCheckLoad(jobId: String,
                                   checkId: String,
                                   checkName: String,
                                   description: Option[String],
                                   metadata: Option[String],
                                   sourceId: String,
                                   expected: String,
                                   status: String,
                                   message: Option[String],
                                   isCritical: Boolean,
                                   referenceDate: Timestamp,
                                   executionDate: Timestamp) extends CheckResult {
    val entityType: String = "loadCheckResult"
  }

  final case class JobState(jobId: String,
                            config: String,
                            versionInfo: String,
                            referenceDate: Timestamp,
                            executionDate: Timestamp
                           ) extends DQEntity {
    val uniqueFields: JobUniqueResult = jobId :: referenceDate :: HNil
    val uniqueFieldNames: Seq[String] = Seq("jobId", "referenceDate").map(camelToSnakeCase)
    override val entityType: String = "jobState"
  }

  final case class ResultMetricError(jobId: String,
                                     metricId: String,
                                     sourceId: String,
                                     sourceKeyFields: String,
                                     metricColumns: String,
                                     status: String,
                                     message: String,
                                     rowData: String,
                                     errorHash: String,
                                     referenceDate: Timestamp,
                                     executionDate: Timestamp) extends DQEntity {
    override val uniqueFields: HList = jobId :: errorHash :: referenceDate :: HNil
    override val uniqueFieldNames: Seq[String] = Seq("jobId", "errorHash", "referenceDate").map(camelToSnakeCase)
    val entityType: String = "metricError"
  }

  final case class ResultSummaryMetrics(
                                         jobId: String,
                                         jobStatus: String,
                                         referenceDate: Timestamp,
                                         executionDate: Timestamp,
                                         numSources: Int,
                                         numMetrics: Int,
                                         numChecks: Int,
                                         numLoadChecks: Int,
                                         numMetricsWithErrors: Int,
                                         numFailedChecks: Int,
                                         numFailedLoadChecks: Int,
                                         listMetricsWithErrors: Seq[String],
                                         listFailedChecks: Seq[String],
                                         listFailedLoadChecks: Seq[String],
                                       ) extends DQEntity {
    override val entityType: String = "summaryReport"
    override val uniqueFields: HList = jobId :: referenceDate :: executionDate :: HNil
    override val uniqueFieldNames: Seq[String] = Seq("jobId", "referenceDate", "executionDate").map(camelToSnakeCase)
  }
  
  /**
   * Set of all results
   * @param regularMetrics Sequence of regular metric results
   * @param composedMetrics Sequence of composed metric results
   * @param trendMetrics Sequence of trend metric results
   * @param checks Sequence of check results
   * @param loadChecks Sequence of load check results
   * @param metricErrors Sequence of metric errors
   */
  final case class ResultSet(
                              regularMetrics: Seq[ResultMetricRegular],
                              composedMetrics: Seq[ResultMetricComposed],
                              trendMetrics: Seq[ResultMetricTrend],
                              checks: Seq[ResultCheck],
                              loadChecks: Seq[ResultCheckLoad],
                              metricErrors: Seq[ResultMetricError],
                              jobConfig: JobState,
                              summaryMetrics: ResultSummaryMetrics,
                              failureToleranceViolationChecks: Seq[String]
                            )
  
  object ResultSet{
    /**
     * Factory method to build result set along with Summary metrics from job results.
     * @param numSources Number of sources that was processed.
     * @param regularMetrics Regular metric results
     * @param composedMetrics Compose metric results
     * @param trendMetrics Trend metric results
     * @param checks Check results
     * @param loadChecks Load check results
     * @param metricErrors Metric errors
     * @param jobId Implicit job ID string
     * @param settings Implicit application settings object
     * @return Job results set
     */
    def apply(numSources: Int,
              regularMetrics: Seq[ResultMetricRegular],
              composedMetrics: Seq[ResultMetricComposed],
              trendMetrics: Seq[ResultMetricTrend],
              checks: Seq[ResultCheck],
              loadChecks: Seq[ResultCheckLoad],
              jobConfig: JobState,
              metricErrors: Seq[ResultMetricError]
             )(implicit jobId: String, settings: AppSettings): ResultSet = {
      val criticalChecks = (checks ++ loadChecks).filter(_.isCritical).map(_.checkId)
      val failedChecks = checks.filter(_.status != CalculatorStatus.Success.toString).map(_.checkId)
      val failedLoadChecks = loadChecks.filter(_.status != CalculatorStatus.Success.toString).map(_.checkId)
      val metricsWithErrors = metricErrors.map(_.metricId).distinct
      val status = if (failedChecks.length + failedLoadChecks.length == 0) "SUCCESS" else "FAILURE"
      val failedChk = failedChecks ++ failedLoadChecks

      val failureToleranceViolationChecks: Seq[String] = settings.checkFailureTolerance.entryName match {
        case "Critical" =>
          val failedCritical = (checks ++ loadChecks)
            .filter(r => criticalChecks.contains(r.checkId) && r.status == "Failure")
            .map(_.checkId)

          if (failedCritical.nonEmpty) {
            failedCritical
          } else Seq.empty

        case "All" if failedChk.nonEmpty =>
          failedChk
        case _ => Seq.empty
      }
      val summary = ResultSummaryMetrics(
        jobId,
        status,
        settings.referenceDateTime.getUtcTS,
        settings.executionDateTime.getUtcTS,
        numSources,
        regularMetrics.size + composedMetrics.size + trendMetrics.size,
        checks.size,
        loadChecks.size,
        metricsWithErrors.size,
        failedChecks.size,
        failedLoadChecks.size,
        metricsWithErrors,
        failedChecks,
        failedLoadChecks
      )
      ResultSet(regularMetrics, composedMetrics, trendMetrics, checks, loadChecks, metricErrors, jobConfig, summary, failureToleranceViolationChecks)
    }
  }
}
