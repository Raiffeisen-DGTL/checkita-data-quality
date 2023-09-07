-- REPLACE <schema_name> and <schema_dir> with actual name and path:
set hivevar:schema_name=<schema_name>;
set hivevar:schema_dir=<schema_path>;

CREATE SCHEMA IF NOT EXISTS ${schema_name};

DROP TABLE IF EXISTS ${schema_name}.results_metric_columnar;
CREATE EXTERNAL TABLE ${schema_name}.results_metric_columnar
(
    metric_id         STRING COMMENT '',
    metric_name       STRING COMMENT '',
    description       STRING COMMENT '',
    source_id         STRING COMMENT '',
    column_names      STRING COMMENT '',
    params            STRING COMMENT '',
    result            DOUBLE,
    additional_result STRING COMMENT '',
    reference_date    TIMESTAMP COMMENT '',
    execution_date    TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Column Metrics Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_metric_columnar';
MSCK REPAIR TABLE ${schema_name}.results_metric_columnar;

DROP TABLE IF EXISTS ${schema_name}.results_metric_file;
CREATE EXTERNAL TABLE ${schema_name}.results_metric_file
(
    metric_id         STRING COMMENT '',
    metric_name       STRING COMMENT '',
    description       STRING COMMENT '',
    source_id         STRING COMMENT '',
    result            DOUBLE COMMENT '',
    additional_result STRING COMMENT '',
    reference_date    TIMESTAMP COMMENT '',
    execution_date    TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality File Metrics Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_metric_file';
MSCK REPAIR TABLE ${schema_name}.results_metric_file;

DROP TABLE IF EXISTS ${schema_name}.results_metric_composed;
CREATE EXTERNAL TABLE ${schema_name}.results_metric_composed
(
    metric_id         STRING COMMENT '',
    metric_name       STRING COMMENT '',
    description       STRING COMMENT '',
    source_id         STRING COMMENT '',
    formula           STRING COMMENT '',
    result            DOUBLE COMMENT '',
    additional_result STRING COMMENT '',
    reference_date    TIMESTAMP COMMENT '',
    execution_date    TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Composed Metrics Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_metric_composed';
MSCK REPAIR TABLE ${schema_name}.results_metric_composed;

DROP TABLE IF EXISTS ${schema_name}.results_check;
CREATE EXTERNAL TABLE ${schema_name}.results_check
(
    check_id           STRING COMMENT '',
    check_name         STRING COMMENT '',
    description        STRING COMMENT '',
    source_id          STRING COMMENT '',
    base_metric        STRING COMMENT '',
    compared_metric    STRING COMMENT '',
    compared_threshold DOUBLE COMMENT '',
    lower_bound        STRING COMMENT '',
    upper_bound        STRING COMMENT '',
    status             STRING COMMENT '',
    message            STRING COMMENT '',
    reference_date     TIMESTAMP COMMENT '',
    execution_date     TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Checks Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_check';
MSCK REPAIR TABLE ${schema_name}.results_check;

DROP TABLE IF EXISTS ${schema_name}.results_check_load;
CREATE EXTERNAL TABLE ${schema_name}.results_check_load
(
    check_id       STRING COMMENT '',
    check_name     STRING COMMENT '',
    source_id      STRING COMMENT '',
    expected       STRING COMMENT '',
    status         STRING COMMENT '',
    message        STRING COMMENT '',
    reference_date TIMESTAMP COMMENT '',
    execution_date TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Load Checks Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_check_load';
MSCK REPAIR TABLE ${schema_name}.results_check_load;