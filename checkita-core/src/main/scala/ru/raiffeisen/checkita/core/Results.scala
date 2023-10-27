package ru.raiffeisen.checkita.core

import enumeratum.{Enum, EnumEntry}
import org.json4s._
import org.json4s.jackson.Serialization.write
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.MetricErrors
import ru.raiffeisen.checkita.storage.Models._

import scala.collection.immutable

object Results {

  implicit val formats: DefaultFormats.type = DefaultFormats
  
  /**
   * Enumeration holding all possible result types:
   *   - source metric results
   *   - composed metric results
   *   - load check results
   *   - check results
   */
  sealed trait ResultType extends EnumEntry

  object ResultType extends Enum[ResultType] {
    case object RegularMetric extends ResultType
    case object ComposedMetric extends ResultType
    case object LoadCheck extends ResultType
    case object Check extends ResultType

    override val values: immutable.IndexedSeq[ResultType] = findValues
  }

  sealed abstract class TypedResult {
    val resultType: ResultType
  }

  /**
   * Metric calculator result.
   *
   * @param metricId          Metric ID
   * @param metricName        Metric calculator name
   * @param result            Metric calculation results
   * @param additionalResult  Additional metric calculation result
   * @param sourceIds         Source IDs on which metric was calculated
   * @param columns           Sequence of metric columns
   * @param errors            Metric errors
   * @param resultType        Type of result
   */
  final case class MetricCalculatorResult(
                                           metricId: String,
                                           metricName: String,
                                           result: Double,
                                           additionalResult: Option[String],
                                           sourceIds: Seq[String],
                                           sourceKeyFields: Seq[String],
                                           columns: Seq[String],
                                           errors: Option[MetricErrors],
                                           resultType: ResultType
                                         ) extends TypedResult {

    /**
     * Converts regular metric calculator result to final regular metric result representation suitable
     * for storing into results storage and sending via targets.
     * @param description Regular metric description
     * @param params Regular metric parameters (JSON string)
     * @param jobId Implicit Job ID
     * @param settings Implicit application settings object
     * @return Finalized regular metric result
     */
    def finalizeAsRegular(description: Option[String],
                          params: Option[String])(implicit jobId: String,
                                                  settings: AppSettings): ResultMetricRegular = ResultMetricRegular(
      jobId,
      metricId,
      metricName,
      description,
      write(sourceIds),
      Some(write(columns)),
      params,
      result,
      additionalResult,
      settings.referenceDateTime.getUtcTS,
      settings.executionDateTime.getUtcTS
    )

    /**
     * Converts composed metric calculator result to final composed metric result representation suitable
     * for storing into results storage and sending via targets.
     * @param description Composed metric description
     * @param formula Composed metric formula
     * @param jobId Implicit Job ID
     * @param settings Implicit application settings object
     * @return Finalized composed metric result
     */
    def finalizeAsComposed(description: Option[String],
                           formula: String)(implicit jobId: String,
                                            settings: AppSettings): ResultMetricComposed = ResultMetricComposed(
      jobId,
      metricId,
      metricName,
      description,
      write(sourceIds),
      formula,
      result,
      additionalResult,
      settings.referenceDateTime.getUtcTS,
      settings.executionDateTime.getUtcTS
    )
    
    //  case class MetricErrors(columns: Seq[String], errors: Seq[ErrorRow])
    // case class ErrorRow(status: CalculatorStatus, message: String, rowData: Seq[String])
    def finalizeMetricErrors(implicit jobId: String,
                             settings: AppSettings): Seq[ResultMetricErrors] = 
      errors.toSeq.flatMap{ err => 
        err.errors.map(e => ResultMetricErrors(
          jobId,
          metricId,
          sourceIds,
          sourceKeyFields,
          columns,
          e.status.toString,
          e.message,
          err.columns.zip(e.rowData).toMap,
          settings.referenceDateTime.getUtcTS,
          settings.executionDateTime.getUtcTS
        ))
      }
  }

  /**
   * Check calculator result.
   * @param checkId Check ID
   * @param checkName Check calculator name
   * @param baseMetric Base metric used to build check
   * @param comparedMetric Metric to compare with
   * @param comparedThreshold Threshold to compare with
   * @param lowerBound Allowed lower bound for base metric value
   * @param upperBound Allowed upper bound for base metric value
   * @param status Check status
   * @param message Check message
   * @param resultType Type of result
   */
  final case class CheckCalculatorResult(
                                          checkId: String,
                                          checkName: String,
                                          sourceIds: Seq[String],
                                          baseMetric: String,
                                          comparedMetric: Option[String],
                                          comparedThreshold: Option[Double],
                                          lowerBound: Option[Double],
                                          upperBound: Option[Double],
                                          status: CalculatorStatus,
                                          message: String,
                                          resultType: ResultType = ResultType.Check
                                        ) extends TypedResult {

    /**
     * Converts check calculator result to final check result representation suitable
     * for storing into results storage and sending via targets.
     * @param description Check description
     * @param jobId Implicit Job ID
     * @param settings Implicit application settings object
     * @return Finalized check result
     */
    def finalize(description: Option[String])(implicit jobId: String, settings: AppSettings): ResultCheck = ResultCheck(
      jobId,
      checkId,
      checkName,
      description,
      write(sourceIds),
      baseMetric,
      comparedMetric,
      comparedThreshold,
      lowerBound,
      upperBound,
      status.toString,
      Some(message),
      settings.referenceDateTime.getUtcTS,
      settings.executionDateTime.getUtcTS
    )
  }

  /**
   * Load check calculator result
   * @param checkId Check ID
   * @param checkName Load check calculator name
   * @param sourceId Source ID
   * @param expected Expected value
   * @param status Check status
   * @param message Check message
   * @param resultType Type of result
   */
  final case class LoadCheckCalculatorResult(
                                              checkId: String,
                                              checkName: String,
                                              sourceId: String,
                                              expected: String,
                                              status: CalculatorStatus,
                                              message: String,
                                              resultType: ResultType = ResultType.LoadCheck
                                            ) {
    /**
     * Converts load check calculator result to final load check result representation suitable
     * for storing into results storage and sending via targets.
     * @param jobId Implicit Job ID
     * @param settings Implicit application settings object
     * @return Finalized load check result
     */
    def finalize(implicit jobId: String, settings: AppSettings): ResultCheckLoad = ResultCheckLoad(
      jobId,
      checkId,
      checkName,
      sourceId,
      expected,
      status.toString,
      Some(message),
      settings.referenceDateTime.getUtcTS,
      settings.executionDateTime.getUtcTS
    )
  }
}
