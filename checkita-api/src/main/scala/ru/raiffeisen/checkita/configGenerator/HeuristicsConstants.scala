package ru.raiffeisen.checkita.configGenerator


/**  */
object HeuristicsConstants {
  val notDataType: Seq[String] = List(")", "key", "constraint", "))", "")
  val tableType: Seq[String] = Seq("oracle", "postgresql", "mysql", "mssql", "h2", "clickhouse")
  val rowCount: String = "rowCount: [{id: %s_row_cnt, description: Row count in %s, source: %s}]"
  val greaterThanConst: String = "greaterThan: [{id: check_row_cnt, metric: %s_row_cnt, threshold: 0}]"
  val lessThanPct = "lessThan: [{id: check_pct_of_null, metric: %s_pct_of_null, threshold: 0.1}]"
  val equalToConst = "equalTo: [ %s ]"
  val distinctCols: Seq[String] = Seq("login", "cnum", "inn", "cocunut")
  val jobConfigConst =
    """
    "jobConfig":
        {
            "jobId": "job_%a",
            %b
            "sources": {%c},
            "loadChecks": {%d},
            %e,
            "checks": {
                "snapshot": {
                    %f
                }
            },
            "targets": {%g}
        }
    """
  val cnumRegex =
    """
    regexMismatch: [
        {
            id: %s_cnum_regex,
            description: Count cnum of wrong format in %s,
            source: %s,
            columns: ["cnum"],
            params:
                {regex: ^[A-Z0-9]{6}$}
        }
    ]
    """
  val equalToCnum =
    """
    {
        "id": "%s_cnum_regex_check",
        "description": "Check that %s do not have wrong cnum.",
        "metric": "%s_cnum_regex",
        "threshold": 0
    }
    """
  val colNumConst =
    """
    "exactColumnNum": [
        {"id": "%s_cols_num", "source": "%s", "option": %a}
    ]
    """
  val duplicateVal =
    """
    "duplicateValues": [
        {"id": "%s_duplicate_val", "description": "Count of duplicate values in %s", "source": "%s", "columns": %a}
    ]
    """
  val equalToDuplicates =
    """
    {
        "id": "check_duplicates_%s",
        "description": "%s mustnt contain duplicates",
        "metric": "%s_duplicate_val", "threshold": 0
    }
    """
  val completeness =
    """
    "completeness": [
        {"id": "%s_completeness", "description": "Completeness check for %s", "source": "%s", "columns": [%a]}
    ]
    """
  val equalToCompleteness =
    """
    {
        "id": "check_completeness_%s",
        "description": "Completness check for %s",
        "metric": "%s_completeness",
        "threshold": 1.0
    }
    """
  val nullVal =
    """
    "nullValues": [
        {"id": "%s_nulls", "description": "Null values in %s", "source": "%s", "columns": %s}
    ]
    """
  val composedConstants =
    """
    "composed": [
        {"id": "%s_pct_of_null",
        "description": "Percent of null values in %s",
        "formula": %s_nulls   / (  %s_row_cnt  )"
        }
    ]
    """
  val equalToNulls =
    """
    {
        "id": "check_nulls_%s",
        "description": "%s mustnt contain nulls",
        "metric": "%s_nulls",
        "threshold": 0
    }
    """
  val targetsEmail =
    """
    "summary": {
        "email": {
            "attachMetricErrors": true,
            "dumpSize": 10,
            "recipients": ["change@change.com"]
        }
    },
    checkAlerts: {
        email: [
          {
                id: "alert",
                checks: [%s],
                recipients: ["change@change.com"]
          }
       ]
    }
    """
}