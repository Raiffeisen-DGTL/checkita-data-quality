package ru.raiffeisen.checkita.storage

import ru.raiffeisen.checkita.storage.Models._
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

import java.sql.Timestamp

class Tables(val profile: JdbcProfile) {
  
  import profile.api._
  
  sealed abstract class DQTable[R <: DQEntity](tag: Tag, schema: Option[String], table: String)
      extends Table[R](tag, schema, table) {

    // columns that are mandatory for all entities:
    def jobId: Rep[String] = column[String]("job_id")
    def referenceDate: Rep[Timestamp] = column[Timestamp]("reference_date")
    def executionDate: Rep[Timestamp] = column[Timestamp]("execution_date")

    def getUniqueCond(r: R): Rep[Boolean]
  }

  trait DescriptiveColumns[R <: DQEntity] { this: DQTable[R] =>
    // optional description that eny result entity can have
    def description: Rep[Option[String]] = column[Option[String]]("description")
    // optional user-defined metadata that any result entity can have
    def metadata: Rep[Option[String]] = column[Option[String]]("metadata")
  }

  trait DQTableOps[R <: DQEntity] {
    type T <: DQTable[R]
    def getTableQuery(schema: Option[String]): TableQuery[T]
    def getTableName(t: TableQuery[T]): String = t.baseTableRow.tableName
  }
  
  sealed abstract class MetricResultTable[R <: MetricResult](tag: Tag, schema: Option[String], table: String)
      extends DQTable[R](tag, schema, table) with DescriptiveColumns[R] {
    
    def metricId: Rep[String] = column[String]("metric_id")
    def metricName: Rep[String] = column[String]("metric_name")
    def sourceId: Rep[String] = column[String]("source_id")
    def result: Rep[Double] = column[Double]("result")
    def additionalResult: Rep[Option[String]] = column[Option[String]]("additional_result")
  }
  
  sealed abstract class CheckResultTable[R <: CheckResult](tag: Tag, schema: Option[String], table: String)
      extends DQTable[R](tag, schema, table) with DescriptiveColumns[R] {
    
    def checkId: Rep[String] = column[String]("check_id")
    def checkName: Rep[String] = column[String]("check_name")
    def sourceId: Rep[String] = column[String]("source_id")
    def status: Rep[String] = column[String]("status")
    def message: Rep[Option[String]] = column[Option[String]]("message")

    def getUniqueCond(r: R): Rep[Boolean] =
      jobId === r.jobId && checkId === r.checkId && referenceDate === r.referenceDate
  }
  
  class ResultMetricRegularTable(tag: Tag, schema: Option[String]) 
    extends MetricResultTable[ResultMetricRegular](tag, schema, "results_metric_regular") {
    
    def columnNames: Rep[Option[String]] = column[Option[String]]("column_names")
    def params: Rep[Option[String]] = column[Option[String]]("params")
    
    override def * : ProvenShape[ResultMetricRegular] = (
      jobId,
      metricId,
      metricName,
      description,
      metadata,
      sourceId,
      columnNames,
      params,
      result,
      additionalResult,
      referenceDate,
      executionDate
    ) <> (ResultMetricRegular.tupled, ResultMetricRegular.unapply)

    def getUniqueCond(r: ResultMetricRegular): Rep[Boolean] =
      jobId === r.jobId && metricId === r.metricId && metricName === r.metricName && referenceDate === r.referenceDate
  }
  
  class ResultMetricComposedTable(tag: Tag,schema: Option[String])
    extends MetricResultTable[ResultMetricComposed](tag, schema, "results_metric_composed") {

    def formula: Rep[String] = column[String]("formula")

    override def * : ProvenShape[ResultMetricComposed] = (
      jobId,
      metricId,
      metricName,
      description,
      metadata,
      sourceId,
      formula,
      result,
      additionalResult,
      referenceDate,
      executionDate
    ) <> (ResultMetricComposed.tupled, ResultMetricComposed.unapply)

    def getUniqueCond(r: ResultMetricComposed): Rep[Boolean] =
      jobId === r.jobId && metricId === r.metricId && referenceDate === r.referenceDate
  }

  class ResultCheckTable(tag: Tag,schema: Option[String])
    extends CheckResultTable[ResultCheck](tag, schema, "results_check") {

    def baseMetric: Rep[String] = column[String]("base_metric")
    def comparedMetric: Rep[Option[String]] = column[Option[String]]("compared_metric")
    def comparedThreshold: Rep[Option[Double]] = column[Option[Double]]("compared_threshold")
    def lowerBound: Rep[Option[Double]] = column[Option[Double]]("lower_bound")
    def upperBound: Rep[Option[Double]] = column[Option[Double]]("upper_bound")

    override def * : ProvenShape[ResultCheck] = (
      jobId,
      checkId,
      checkName,
      description,
      metadata,
      sourceId,
      baseMetric,
      comparedMetric,
      comparedThreshold,
      lowerBound,
      upperBound,
      status,
      message,
      referenceDate,
      executionDate
    ) <> (ResultCheck.tupled, ResultCheck.unapply)
  }

  class ResultCheckLoadTable(tag: Tag,schema: Option[String])
    extends CheckResultTable[ResultCheckLoad](tag, schema, "results_check_load") {

    def expected: Rep[String] = column[String]("expected")

    override def * : ProvenShape[ResultCheckLoad] = (
      jobId,
      checkId,
      checkName,
      description,
      metadata,
      sourceId,
      expected,
      status,
      message,
      referenceDate,
      executionDate
    ) <> (ResultCheckLoad.tupled, ResultCheckLoad.unapply)
  }

  class ResultMetricErrorTable(tag: Tag, schema: Option[String])
    extends DQTable[ResultMetricError](tag, schema, "results_metric_error") {


    def metricId: Rep[String] = column[String]("metric_id")
    def sourceId: Rep[String] = column[String]("source_id")
    def sourceKeyFields: Rep[String] = column[String]("source_key_fields")
    def metricColumns: Rep[String] = column[String]("metric_columns")
    def status: Rep[String] = column[String]("status")
    def message: Rep[String] = column[String]("message")
    def rowData: Rep[String] = column[String]("row_data")
    def errorHash: Rep[String] = column[String]("error_hash")

    def getUniqueCond(r: ResultMetricError): Rep[Boolean] =
      jobId === r.jobId && errorHash === r.errorHash && referenceDate === r.referenceDate

    def * : ProvenShape[ResultMetricError] = (
      jobId,
      metricId,
      sourceId,
      sourceKeyFields,
      metricColumns,
      status,
      message,
      rowData,
      errorHash,
      referenceDate,
      executionDate
    ) <> (ResultMetricError.tupled, ResultMetricError.unapply)
  }

  class JobStateTable(tag: Tag, schema: Option[String])
    extends DQTable[JobState](tag, schema, "job_state") {

    def config: Rep[String] = column[String]("config")
    def versionInfo: Rep[String] = column[String]("version_info")

    def getUniqueCond(r: JobState): Rep[Boolean] =
      jobId === r.jobId && referenceDate === r.referenceDate

    def * : ProvenShape[JobState] = (
      jobId,
      config,
      versionInfo,
      referenceDate,
      executionDate
    ) <> (JobState.tupled, JobState.unapply)
  }
    
  object TableImplicits {
    implicit object ResultMetricColumnarTableOps extends DQTableOps[ResultMetricRegular] {
      type T = ResultMetricRegularTable
      def getTableQuery(schema: Option[String]): TableQuery[ResultMetricRegularTable] =
        TableQuery[ResultMetricRegularTable]((t: Tag) => new ResultMetricRegularTable(t, schema))
    }

    implicit object ResultMetricComposedTableOps extends DQTableOps[ResultMetricComposed] {
      type T = ResultMetricComposedTable
      def getTableQuery(schema: Option[String]): TableQuery[ResultMetricComposedTable] =
        TableQuery[ResultMetricComposedTable]((t: Tag) => new ResultMetricComposedTable(t, schema))
    }

    implicit object ResultCheckTableOps extends DQTableOps[ResultCheck] {
      type T = ResultCheckTable
      def getTableQuery(schema: Option[String]): TableQuery[ResultCheckTable] =
        TableQuery[ResultCheckTable]((t: Tag) => new ResultCheckTable(t, schema))
    }

    implicit object ResultCheckLoadTableOps extends DQTableOps[ResultCheckLoad] {
      type T = ResultCheckLoadTable
      def getTableQuery(schema: Option[String]): TableQuery[ResultCheckLoadTable] =
        TableQuery[ResultCheckLoadTable]((t: Tag) => new ResultCheckLoadTable(t, schema))
    }

    implicit object ResultMetricErrorTableOps extends DQTableOps[ResultMetricError] {
      type T = ResultMetricErrorTable

      def getTableQuery(schema: Option[String]): TableQuery[ResultMetricErrorTable] =
        TableQuery[ResultMetricErrorTable]((t: Tag) => new ResultMetricErrorTable(t, schema))
    }

    implicit object JobStateTableOps extends DQTableOps[JobState] {
      type T = JobStateTable

      def getTableQuery(schema: Option[String]): TableQuery[JobStateTable] =
        TableQuery[JobStateTable]((t: Tag) => new JobStateTable(t, schema))
    }
  }
}
