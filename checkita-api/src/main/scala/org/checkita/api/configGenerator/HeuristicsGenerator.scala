package org.checkita.api.configGenerator

import scala.collection.mutable.ListBuffer

import com.typesafe.config.Config
import eu.timepit.refined.api.Refined
import eu.timepit.refined.types.string.NonEmptyString

import org.checkita.dqf.config.IO.writeJobConfig
import org.checkita.dqf.config.RefinedTypes.{Email, ID}
import org.checkita.dqf.config.jobconf.Checks._
import org.checkita.dqf.config.jobconf.Connections._
import org.checkita.dqf.config.jobconf.JobConfig
import org.checkita.dqf.config.jobconf.LoadChecks.{ExactColNumLoadCheckConfig, LoadChecksConfig}
import org.checkita.dqf.config.jobconf.Metrics._
import org.checkita.dqf.config.jobconf.Sources._
import org.checkita.dqf.config.jobconf.Targets._
import org.checkita.dqf.utils.ResultUtils._
import org.checkita.dqf.utils.Logging
import org.checkita.api.configGenerator.DdlParser.parseDDL

/**  */
object HeuristicsGenerator extends Logging{

  /**
   * Generates a configuration based on the provided DDL string and connection type.
   *
   * @param ddl      The DDL string representing the table schema.
   * @param connType The type of database connection (e.g., "oracle", "postgresql", "mysql").
   * @return A `Result[Config]` object representing the generated configuration.
   *
   */
  def heuristics(ddl: String, connType: String): Result[Config] = {

    val tableType = Seq("oracle", "postgresql", "mysql", "mssql", "h2", "clickhouse")
    val distinctCols = Seq("login", "inn", "id")
    val (sourcesMap, parsedDdl) = parseDDL(ddl, connType)
    val source = sourcesMap("source")
    val metricsForTarget: ListBuffer[NonEmptyString] = ListBuffer(Refined.unsafeApply("check_row_cnt"))
    val initialState = (0, List.empty[String], List.empty[String], List.empty[String])

    val rowCnt = RowCountMetricConfig(
      id = ID(f"$source%s_row_cnt"),
      source = Refined.unsafeApply(source),
      description = Option(Refined.unsafeApply("Row count in %s"))
    )
    val greaterThan = GreaterThanCheckConfig(
      id = ID(f"check_row_cnt"),
      description = Option(Refined.unsafeApply(f"Completness check for $source")),
      metric = Refined.unsafeApply(f"$source%s_row_cnt"),
      threshold = Option(0.0),
      compareMetric = None
    )

    val (colCnt, notNullableCols, duplicatesCols, completenessCols) = parsedDdl.values.foldLeft(initialState) {
      case ((colCntAcc, notNullableAcc, duplicatesAcc, completenessAcc), columns) =>
        columns.foldLeft((colCntAcc, notNullableAcc, duplicatesAcc, completenessAcc)) {
          case ((colCntInner, notNullableInner, duplicatesInner, completenessInner), values) =>
            val key = values.keys.head
            val value = values.values.head
            val updatedColCnt = colCntInner + 1

            val updatedDuplicates = if (distinctCols.contains(key)) duplicatesInner :+ key else duplicatesInner

            val updatedNotNullable = if (value == "not null" || distinctCols.contains(key)) {
              notNullableInner :+ key
            } else notNullableInner

            val updatedCompleteness = if (value != "not null" && !distinctCols.contains(key)) {
              completenessInner :+ key
            } else completenessInner

            (updatedColCnt, updatedNotNullable, updatedDuplicates, updatedCompleteness)
        }
    }

    val colNum = ExactColNumLoadCheckConfig(
      id = ID(f"$source%s_cols_num"),
      source = Refined.unsafeApply(source),
      option = Refined.unsafeApply(colCnt),
      description = None
    )

    val (duplicateVal: Option[DuplicateValuesMetricConfig], equalToDuplicates: Option[EqualToCheckConfig]) =
      if (duplicatesCols.nonEmpty) {
        log.debug(f"Found ${duplicatesCols.length} columns with duplicate values")
        val duplicateVal = DuplicateValuesMetricConfig(
          id = ID(f"$source%s_duplicate_val"),
          description = Option(Refined.unsafeApply(f"Count of duplicate values in $source")),
          source = Refined.unsafeApply(source),
          columns = Refined.unsafeApply(duplicatesCols)
        )
        val equalToDuplicates = EqualToCheckConfig(
          id = ID(f"check_duplicates_$source"),
          description = Option(Refined.unsafeApply(f"$source mustnt contain nulls")),
          metric = Refined.unsafeApply(f"$source%s_duplicate_val"),
          threshold = Option(0.0),
          compareMetric = None
        )
        metricsForTarget += Refined.unsafeApply(s"check_duplicates_$source")
        (Some(duplicateVal), Some(equalToDuplicates))
      } else (None, None)

    val (completeness: Option[CompletenessMetricConfig], equalToCompleteness: Option[EqualToCheckConfig]) =
      if (completenessCols.nonEmpty) {
        log.debug(f"Found ${completenessCols.length} columns with completeness")
        val completeness = CompletenessMetricConfig(
          id = ID(f"$source%s_completeness"),
          description = Option(Refined.unsafeApply(f"Completeness check for $source")),
          source = Refined.unsafeApply(source),
          columns = Refined.unsafeApply(completenessCols)
        )
        val equalToCompleteness = EqualToCheckConfig(
          id = ID(f"check_completeness_$source"),
          description = Option(Refined.unsafeApply(f"Completness check for $source")),
          metric = Refined.unsafeApply(f"$source%s_completeness"),
          threshold = Option(1.0),
          compareMetric = None
        )
        metricsForTarget += Refined.unsafeApply(s"check_completeness_$source")
        (Some(completeness), Some(equalToCompleteness))
      } else (None, None)

    val (
      regular: Option[RegularMetricsConfig],
      composed: Option[ComposedMetricConfig],
      equalToNulls: Option[EqualToCheckConfig],
      lessThanPct: Option[LessThanCheckConfig]
      ) =
      if (notNullableCols.nonEmpty) {
        log.debug(f"Found ${notNullableCols.length} nullable columns")
        val nullVal = NullValuesMetricConfig(
          id = ID(f"$source%s_nulls"),
          description = Option(Refined.unsafeApply(f"Null values in $source")),
          source = Refined.unsafeApply(source),
          columns = Refined.unsafeApply(notNullableCols)
        )
        val composed = ComposedMetricConfig(
          id = ID(f"$source%s_pct_of_null"),
          description = Option(Refined.unsafeApply(f"Percent of null values in $source")),
          formula = Refined.unsafeApply(f"$source%s_nulls   / (  $source%s_row_cnt  )")
        )
        val lessThanPct = LessThanCheckConfig(
          id = ID(f"check_pct_of_null"),
          description = None,
          metric = Refined.unsafeApply(f"$source%s_pct_of_null"),
          compareMetric = None,
          threshold = Option(0.1)
        )
        metricsForTarget += Refined.unsafeApply("check_pct_of_null")
        val equalToNulls = EqualToCheckConfig(
          id = ID(f"check_nulls_$source"),
          description = Option(Refined.unsafeApply(f"$source mustnt contain duplicates")),
          metric = Refined.unsafeApply(f"$source%s_duplicate_val"),
          threshold = Option(0.0),
          compareMetric = None
        )
        metricsForTarget += Refined.unsafeApply(s"check_nulls_$source")
        val regular = RegularMetricsConfig(
          rowCount = Seq(rowCnt),
          duplicateValues = duplicateVal match {
            case Some(value) => Seq(value)
            case None => Seq.empty
          },
          completeness = completeness match {
            case Some(value) => Seq(value)
            case None => Seq.empty
          },
          nullValues = Seq(nullVal)
        )
        (Some(regular), Some(composed), Some(equalToNulls), Some(lessThanPct))
      } else {
        (None, None, None, None)
      }

    val metrics = MetricsConfig(
      regular = regular,
      composed = composed match {
        case Some(comp) => Seq(comp)
        case None => Seq.empty
      }
    )

    val connections: Option[ConnectionsConfig] = connType match {
      case "sqlite" => Some(ConnectionsConfig(
        sqlite = Seq(
          SQLiteConnectionConfig(
            id = ID(f"$connType%s_id"),
            description = None,
            url = Refined.unsafeApply("CHANGE")
          )
        )
      ))
      case "kafka" => Some(ConnectionsConfig(
        kafka = Seq(
          KafkaConnectionConfig
          (
            id = ID(f"$connType%s_id"),
            description = None,
            servers = Refined.unsafeApply(Seq(Refined.unsafeApply("CHANGE")))
          )
        )
      ))
      case "postgresql" => Some(ConnectionsConfig(
        postgres = Seq(
          PostgresConnectionConfig
          (
            id = ID(f"$connType%s_id"),
            description = None,
            url = Refined.unsafeApply("CHANGE"),
            username = Option(Refined.unsafeApply("CHANGE")),
            password = Option(Refined.unsafeApply("CHANGE")),
            schema = None
          )
        )
      ))
      case "oracle" => Some(ConnectionsConfig(
        oracle = Seq(
          OracleConnectionConfig
          (
            id = ID(f"$connType%s_id"),
            description = None,
            url = Refined.unsafeApply("CHANGE"),
            username = Option(Refined.unsafeApply("CHANGE")),
            password = Option(Refined.unsafeApply("CHANGE")),
            schema = None
          )
        )
      ))
      case "mysql" => Some(ConnectionsConfig(
        mysql = Seq(
          MySQLConnectionConfig
          (
            id = ID(f"$connType%s_id"),
            description = None,
            url = Refined.unsafeApply("CHANGE"),
            username = Option(Refined.unsafeApply("CHANGE")),
            password = Option(Refined.unsafeApply("CHANGE")),
            schema = None
          )
        )
      ))
      case "mssql" => Some(ConnectionsConfig(
        mssql = Seq(
          MSSQLConnectionConfig
          (
            id = ID(f"$connType%s_id"),
            description = None,
            url = Refined.unsafeApply("CHANGE"),
            username = Option(Refined.unsafeApply("CHANGE")),
            password = Option(Refined.unsafeApply("CHANGE")),
            schema = None
          )
        )
      ))
      case "h2" => Some(ConnectionsConfig(
        h2 = Seq(
          H2ConnectionConfig
          (
            id = ID(f"$connType%s_id"),
            description = None,
            url = Refined.unsafeApply("CHANGE")
          )
        )
      ))
      case "greenplum" => Some(ConnectionsConfig(
        greenplum = Seq(
          GreenplumConnectionConfig
          (
            id = ID(f"$connType%s_id"),
            description = None,
            url = Refined.unsafeApply("CHANGE"),
            username = Option(Refined.unsafeApply("CHANGE")),
            password = Option(Refined.unsafeApply("CHANGE")),
            schema = None
          )
        )
      ))
      case "clickhouse" => Some(ConnectionsConfig(
        clickhouse = Seq(
          ClickHouseConnectionConfig
          (
            id = ID(f"$connType%s_id"),
            description = None,
            url = Refined.unsafeApply("CHANGE"),
            username = Option(Refined.unsafeApply("CHANGE")),
            password = Option(Refined.unsafeApply("CHANGE")),
            schema = None
          )
        )
      ))
      case _ => None
    }

    val sources = SourcesConfig(
      table = if (tableType.contains(connType) || connType == "sqlite") {
        Seq(
          TableSourceConfig(
            id = ID(f"$source"),
            description = None,
            connection = ID(f"$connType%s_id"),
            table = Option(Refined.unsafeApply(f"$source")),
            query = None
          )
        )
      } else Seq.empty,
      hive = connType match {
        case "hive" => Seq(
          HiveSourceConfig(
            id = ID(f"$source"),
            description = None,
            table = Refined.unsafeApply(f"$source"),
            schema = Refined.unsafeApply("CHANGE")
          )
        )
        case _ => Seq.empty
      },
      kafka = connType match {
        case "kafka" => Seq(
          KafkaSourceConfig(
            id = ID(f"$source"),
            description = None,
            connection = ID(f"$connType%s_id"),
            topics = Seq(Refined.unsafeApply("CHANGE")),
            topicPattern = None,
            startingOffsets = None,
            endingOffsets = None
          )
        )
        case _ => Seq.empty
      },
      greenplum = connType match {
        case "greenplum" => Seq(
          GreenplumSourceConfig(
            id = ID(f"$source"),
            description = None,
            connection = ID(f"$connType%s_id"),
            table = Option(Refined.unsafeApply(f"$source"))
          )
        )
        case _ => Seq.empty
      },
      file = sourcesMap.get("stored_as") match {
        case Some(stored) => stored match {
          case "avro" => Seq(
            AvroFileSourceConfig(
              id = ID(f"$source"),
              description = None,
              path = Refined.unsafeApply(sourcesMap("loc")),
              schema = None
            )
          )
          case "parquet" => Seq(
            ParquetFileSourceConfig(
              id = ID(f"$source"),
              description = None,
              path = Refined.unsafeApply(sourcesMap("loc")),
              schema = None
            )
          )
          case "orc" => Seq(
            OrcFileSourceConfig(
              id = ID(f"$source"),
              description = None,
              path = Refined.unsafeApply(sourcesMap("loc")),
              schema = None
            )
          )
          case _ => connType match {
            case "fixed" => Seq(
              FixedFileSourceConfig(
                id = ID(f"$source"),
                description = None,
                path = Refined.unsafeApply("CHANGE"),
                schema = None
              )
            )
            case "delimited" => Seq(
              DelimitedFileSourceConfig(
                id = ID(f"$source"),
                description = None,
                path = Refined.unsafeApply("CHANGE"),
                schema = None
              )
            )
            case _ => Seq.empty
          }
        }
        case None => Seq.empty
      },
      custom = connType match {
        case "custom" => Seq(
          CustomSource(
            id = ID(f"$source"),
            description = None,
            path = Option(Refined.unsafeApply("CHANGE")),
            schema = None,
            format = Refined.unsafeApply("CHANGE")
          )
        )
        case _ => Seq.empty
      }
    )

    val snapshots = SnapshotChecks(
      equalTo = Seq(
        equalToNulls,
        equalToDuplicates,
        equalToCompleteness
      ).flatten,
      lessThan = lessThanPct match {
        case Some(l) => Seq(l)
        case None => Seq.empty
      },
      greaterThan = Seq(greaterThan)
    )

    val targets = TargetsConfig(
      summary = Some(
        SummaryTargetsConfig(
          email = Some(
            SummaryEmailTargetConfig(
              recipients = Refined.unsafeApply(Seq(Email("change@change.com"))),
              dumpSize = Some(Refined.unsafeApply(10)),
              attachMetricErrors = true,
              subjectTemplate = None,
              template = None,
              templateFile = None
            )
          ),
          mattermost = None,
          kafka = None
        )
      ),
      checkAlerts = Some(
        CheckAlertTargetsConfig(
          email = Seq(
            CheckAlertEmailTargetConfig(
              id = ID("alert"),
              checks = metricsForTarget,
              recipients = Refined.unsafeApply(Seq(Email("change@change.com"))),
              subjectTemplate = None,
              template = None,
              templateFile = None
            )
          )
        )
      ),
      results = None,
      errorCollection = None
    )

    writeJobConfig(
      JobConfig(
        jobId = ID(f"job_$source"),
        connections = connections,
        sources = Some(sources),
        loadChecks = Some(
          LoadChecksConfig(
            exactColumnNum = Seq(colNum)
          )
        ),
        metrics = Some(metrics),
        checks = Some(
          ChecksConfig(
            snapshot = Some(snapshots),
            trend = None
          )
        ),
        targets = Some(targets),
        jobDescription = None,
        streams = None
      )
    )
  }
}