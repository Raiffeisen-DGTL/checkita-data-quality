# Job Configuration Example

Below example represents abstract but fully filled Data Quality job configuration with most of the features of 
Checkita framework configured.

```hocon
jobConfig: {
  jobId: "job_id_for_this_configuration"

  connections: {
    oracle: [
      {id: "oracle_db1", url: "oracle.db.com:1521/public", username: "db-user", password: "dq-password"}
    ]
    sqlite: [
      {id: "sqlite_db", url: "some/path/to/db.sqlite"}
    ],
    kafka: [
      {id: "kafka_broker", servers: ["server1:9092", "server2:9092"]}
    ]
  }

  schemas: [
    {
      id: "schema1"
      kind: "delimited"
      schema: [
        {name: "colA", type: "string"},
        {name: "colB", type: "timestamp"},
        {name: "colC", type: "decimal(10, 3)"}
      ]
    },
    {
      id: "schema2"
      kind: "fixedFull",
      schema: [
        {name: "col1", type: "integer", width: 5},
        {name: "col2", type: "double", width: 6},
        {name: "col3", type: "boolean", width: 4}
      ]
    },
    {id: "schema3", kind: "fixedShort", schema: ["colOne:5", "colTwo:7", "colThree:9"]}
    {id: "hive_schema", kind: "hive", schema: "some_schema", table: "some_table"}
    {id: "avro_schema", kind: "avro", schema: "some/path/to/avro_schema.avsc"}

  ]

  sources: {
    table: [
      {id: "table_source_1", connection: "oracle_db1", table: "some_table", keyFields: ["id", "name"]}
      {id: "table_source_2", connection: "sqlite_db", table: "other_table"}
    ]
    hive: [
      {
        id: "hive_source_1", schema: "some_schema", table: "some_table",
        partitions: [{name: "dlk_cob_date", values: ["2023-06-30", "2023-07-01"]}],
        keyFields: ["id", "name"]
      }
    ]
    file: [
      {id: "hdfs_avro_source", kind: "avro", path: "path/to/avro/file.avro", schema: "avro_schema"},
      {id: "hdfs_orc_source", kind: "orc", path: "path/to/orc/file.orc"},
      {
        id: "hdfs_delimited_source",
        kind: "delimited",
        path: "path/to/csv/file.csv"
        schema: "schema1"
      },
      {id: "hdfs_fixed_file", kind: "fixed", path: "path/to/fixed/file.txt", schema: "schema2"},
    ],
    kafka: [
      {
        id: "kafka_source",
        connection: "kafka_broker",
        topics: ["topic1.pub", "topic2.pub"]
        format: "json"
      }
    ]
  }

  virtualSources: [
    {
      id: "sqlVS"
      kind: "sql"
      parentSources: ["hive_source_1"]
      persist: "disk_only"
      save: {
        kind: "orc"
        path: ${basePath}"/sqlVs"
      }
      query: "select id, name, entity, description from hive_source_1 where dlk_cob_date == '2023-06-30'"
    }
    {
      id: "joinVS"
      kind: "join"
      parentSources: ["hdfs_avro_source", "hdfs_orc_source"]
      joinBy: ["id"]
      joinType: "leftouter"
      persist: "memory_only"
      keyFields: ["id", "order_id"]
    }
    {
      id: "filterVS"
      kind: "filter"
      parentSources: ["kafka_source"]
      expr: ["key is not null"]
      keyFields: ["batchId", "dttm"]
    }
    {
      id: "selectVS"
      kind: "select"
      parentSources: ["table_source_1"]
      expr: [
        "count(id) as id_cnt",
        "count(name) as name_cnt"
      ]
    }
    {
      id: "aggVS"
      kind: "aggregate"
      parentSources: ["hdfs_fixed_file"]
      groupBy: ["col1"]
      expr: [
        "avg(col2) as avg_col2",
        "sum(col3) as sum_col3"
      ],
      keyFields: ["col1", "avg_col2", "sum_col3"]
    }
  ]

  loadChecks: {
    exactColumnNum: [
      {id: "loadCheck1", source: "hdfs_delimited_source", option: 3}
    ]
    minColumnNum: [
      {id: "loadCheck2", source: "kafka_source", option: 2}
    ]
    columnsExist: [
      {id: "loadCheck3", source: "sqlVS", columns: ["id", "name", "entity", "description"]},
      {id: "load_check_4", source: "hdfs_delimited_source", columns: ["id", "name", "value"]}
    ]
    schemaMatch: [
      {id: "load_check_5", source: "kafka_source", schema: "hive_schema"}
    ]
  }

  metrics: {
    regular: {
      rowCount: [
        {id: "hive_table_row_cnt", description: "Row count in hive_source_1", source: "hive_source_1"},
        {id: "csv_file_row_cnt", description: "Row count in hdfs_delimited_source", source: "hdfs_delimited_source"}
      ]
      distinctValues: [
        {
          id: "fixed_file_dist_name", description: "Distinct values in hdfs_fixed_file",
          source: "hdfs_fixed_file", columns: ["colA"]
        }
      ]
      nullValues: [
        {id: "hive_table_nulls", description: "Null values in columns id and name", source: "hive_source_1", columns: ["id", "name"]}
      ]
      completeness: [
        {id: "orc_data_compl", description: "Completness of column id", source: "hdfs_orc_source", columns: ["id"]}
      ]
      avgNumber: [
        {id: "avro_file1_avg_bal", description: "Avg number of column balance", source: "hdfs_avro_source", columns: ["balance"]}
      ]
      regexMatch: [
        {
          id: "table_source1_inn_regex", description: "Regex match for inn column", source: "table_source_1",
          columns: ["inn"], params: {regex: """^\d{10}$"""}
        }
      ]
      stringInDomain: [
        {
          id: "orc_data_segment_domain", source: "hdfs_orc_source",
          columns: ["segment"], params: {domain: ["FI", "MID", "SME", "INTL", "CIB"]}
        }
      ]
      topN: [
        {
          id: "filterVS_top3_currency", description: "Top 3 currency in filterVS", source: "filterVS",
          columns: ["id"], params: {targetNumber: 3, maxCapacity: 10}
        }
      ],
      levenshteinDistance: [
        {
          id: "lvnstDist", source: "table_source_2", columns: ["col1", "col2"],
          params: {normalize: true, threshold: 0.3}
        }
      ]
    }
    composed: [
      {
        id: "pct_of_null", description: "Percent of null values in hive_table1",
        formula: "100 * {{ hive_table_nulls }} ^ 2 / ( {{ hive_table_row_cnt }} + 1)"
      }
    ]
  }

  checks: {
    trend: {
      averageBoundFull: [
        {
          id: "avg_bal_check",
          description: "Check that average balance stays within +/-25% of the week average"
          metric: "avro_file1_avg_bal",
          rule: "datetime"
          windowSize: "8d"
          threshold: 0.25
        }
      ]
      averageBoundUpper: [
        {id: "avg_pct_null", metric: "pct_of_null", rule: "datetime", windowSize: "15d", threshold: 0.5}
      ]
      averageBoundLower: [
        {id: "avg_distinct", metric: "fixed_file_dist_name", rule: "record", windowSize: 31, threshold: 0.3}
      ]
      averageBoundRange: [
        {
          id: "avg_inn_match",
          metric: "table_source1_inn_regex",
          rule: "datetime",
          windowSize: "8d",
          thresholdLower: 0.2
          thresholdUpper: 0.4
        }
      ]
      topNRank: [
        {id: "top2_curr_match", metric: "filterVS_top3_currency", targetNumber: 2, threshold: 0.1}
      ]
    }
    snapshot: {
      differByLT: [
        {
          id: "row_cnt_diff",
          description: "Number of rows in two tables should not differ on more than 5%.",
          metric: "hive_table_row_cnt"
          compareMetric: "csv_file_row_cnt"
          threshold: 0.05
        }
      ]
      equalTo: [
        {id: "zero_nulls", description: "Hive Table1 mustn't contain nulls", metric: "hive_table_nulls", threshold: 0}
      ]
      greaterThan: [
        {id: "completeness_check", metric: "orc_data_compl", threshold: 0.99}
      ]
      lessThan: [
        {id: "null_threshold", metric: "pct_of_null", threshold: 0.01}
      ]
    }
  }

  targets: {
    results: {
      file: {
        resultTypes: ["checks", "loadChecks"]
        save: {
          kind: "delimited"
          path: ${basePath}"/results/"${referenceDate}
          header: true
        }
      }
      hive: {
        resultTypes: ["regularMetrics", "composedMetrics", "loadChecks", "checks"],
        schema: "DQ_SCHEMA",
        table: "DQ_TARGETS"
      }
      kafka: {
        resultTypes: ["regularMetrics", "composedMetrics", "loadChecks", "checks"],
        connection: "kafka_broker"
        topic: "some.topic"
      }
    }
    errorCollection: {
      file: {
        metrics: ["pct_of_null", "hive_table_row_cnt", "hive_table_nulls"]
        dumpSize: 50
        save: {
          kind: "orc"
          path: ${basePath}"/errors/"${referenceDate}
        }
      }
      kafka: {
        metrics: ["hive_table_nulls", "fixed_file_dist_name", "table_source1_inn_regex"]
        dumpSize: 25
        connection: "kafka_broker"
        topic: "some.topic"
        options: ["addParam=true"]
      }
    }
    summary: {
      email: {
        attachMetricErrors: true
        metrics: ["hive_table_nulls", "fixed_file_dist_name", "table_source1_inn_regex"]
        dumpSize: 10
        recipients: ["some.person@some.domain"]
      }
      mattermost: {
        attachMetricErrors: true
        metrics: ["hive_table_nulls", "fixed_file_dist_name", "table_source1_inn_regex"]
        dumpSize: 10
        recipients: ["@someUser", "#someChannel"]
      }
      kafka: {
        connection: "kafka_broker"
        topic: "dev.dq_results.topic"
      }
    }
    checkAlerts: {
      email: [
        {
          id: "alert1"
          checks: ["avg_bal_check", "zero_nulls"]
          recipients: ["some.peron@some.domain"]
        }
        {
          id: "alert2"
          checks: ["top2_curr_match", "completeness_check"]
          recipients: ["another.peron@some.domain"]
        }
      ]
      mattermost: [
        {
          id: "alert3"
          checks: ["avg_bal_check", "zero_nulls"]
          recipients: ["@someUser"]
        }
        {
          id: "alert4"
          checks: ["top2_curr_match", "completeness_check"]
          recipients: ["#someChannel"]
        }
      ]
    }
  }
}
```