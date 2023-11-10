# Data Quality Results Storage

In order to use all features of the framework, it is required to set up a results storage. Checkita can use various
RDBMS as a results storage. Also, Hive can be used as a results storage and even a simple file storage is supported.

The full list of various storage types is following:

* `PostgreSQL` (v.9.3 and higher) - recommended database to be used as resutls storage.
* `Oracle`
* `MySQL`
* `Microsoft SQL Server`
* `SQLite`
* `H2`
* `Hive`
* `File` (directory in local file system or remote one (HDFS, S3))

Checkita framework support results storage schema evolution. [Flyway](https://flywaydb.org/) is run under the hood to
support schema migrations. Therefore, if one of the supported RDBMS is chosen for results storage then it is possible
to set it up during the first run of the Data Quality job providing `-m` application argument on startup.
For more details on how to run Data Quality applications refer to 
[Submitting Data Quality Application](02-ApplicationSubmit.md) chapter.

> **IMPORTANT**: Flyway migrations usually run either in empty database/schema or in one that was initiated with 
> Flyway. In Checkita framework it is also possible to run migration in non-empty database/schema. In this case
> it is up to user to ensure that there are no conflicting table names in database/schema.

If `File` type of storage is used then it is only required to provide a path to a directory/bucket, where results
will be stored. Results are stored as parquet files with the same schema as for RDMS storage. No schema evolution
mechanisms are provided for `File` type of storage. Therefore, if results schemas would evolve later,
it will be up to user to update existing results to a new structure.

> **IMPORTANT**: There is no partitioning used for storing results as parquet files.
> Every job will read entire results history and overwrite it adding new ones. Therefore, using `File` type of storage
> is not recommended for production use. 

For `Hive` type of storage the schema evolution mechanisms are also not available. Therefore, it is up to user to
create corresponding hive tables. DDL scripts from [Hive Storage Setup Scripts](#hive-storage-setup-scripts) chapter
below can be used for that.

> **IMPORTANT**: Results hive table must be partitioned by `job_id`. Job ID is chosen as partition column to support
> faster results fetching during computation of trend checks (used for anomaly detection in data).
> `Hive` type of results storage works faster that `File` one, since only partition for current `job_id` is read and 
> overwritten. Nevertheless, this type of storage is also not recommended for use in production where large number of
> jobs will be run.

## Results Types and Schemas

There are for types of result are written in storage:

* regular metrics results
* composed metrics results
* load checks results
* checks results

Schemas for all results types are given below.

Primary keys denotes how we keep track if unique records: 
generally results for the same Data Quality job that is run for the same reference date are overwritten.
History of various attempts of the same job for the same reference date is not stored. It is done in order trend checks
work correctly. As these checks read historical results from Data Quality storage, it is required that there will be
only one set of results per Data Quality job and given reference date.

### Regular Metrics Results Schema

* Primary key: `(job_id, metric_id, reference_date)`
* `source_id` & `column_names` contain string representation of lists in format `'[val1,val2,val3]'`.
* `params` is a JSON string.

| Column Name       | Column Type | Constraint |
|-------------------|-------------|------------|
| job_id            | STRING      | NOT NULL   |
| metric_id         | STRING      | NOT NULL   |
| metric_name       | STRING      | NOT NULL   |
| description       | STRING      |            |
| source_id         | STRING      | NOT NULL   |
| column_names      | STRING      |            |
| params            | STRING      |            |
| result            | DOUBLE      | NOT NULL   |
| additional_result | STRING      |            |
| reference_date    | TIMESTAMP   | NOT NULL   |
| execution_date    | TIMESTAMP   | NOT NULL   |

### Composed Metrics Results Schema

* Primary key: `(job_id, metric_id, reference_date)`
* `source_id` contains string representation of lists in format `'[val1,val2,val3]'`.

| Column Name       | Column Type | Constraint |
|-------------------|-------------|------------|
| job_id            | STRING      | NOT NULL   |
| metric_id         | STRING      | NOT NULL   |
| metric_name       | STRING      | NOT NULL   |
| description       | STRING      |            |
| source_id         | STRING      | NOT NULL   |
| formula           | STRING      | NOT NULL   |
| result            | DOUBLE      | NOT NULL   |
| additional_result | STRING      |            |
| reference_date    | TIMESTAMP   | NOT NULL   |
| execution_date    | TIMESTAMP   | NOT NULL   |

### Load Checks Results Schema

* Primary key: `(job_id, check_id, reference_date)`
* `source_id` contains string representation of lists in format `'[val1,val2,val3]'`.

| Column Name    | Column Type | Constraint |
|----------------|-------------|------------|
| job_id         | STRING      | NOT NULL   |
| check_id       | STRING      | NOT NULL   |
| check_name     | STRING      | NOT NULL   |
| source_id      | STRING      | NOT NULL   |
| expected       | STRING      | NOT NULL   |
| status         | STRING      | NOT NULL   |
| message        | STRING      |            |
| reference_date | TIMESTAMP   | NOT NULL   |
| execution_date | TIMESTAMP   | NOT NULL   |

### Checks Results Schema

* Primary key: `(job_id, check_id, reference_date)`
* `source_id` contains string representation of lists in format `'[val1,val2,val3]'`.

| Column Name        | Column Type | Constraint |
|--------------------|-------------|------------|
| job_id             | STRING      | NOT NULL   |
| check_id           | STRING      | NOT NULL   |
| check_name         | STRING      | NOT NULL   |
| description        | STRING      |            |
| source_id          | STRING      | NOT NULL   |
| base_metric        | STRING      | NOT NULL   |
| compared_metric    | STRING      |            |
| compared_threshold | DOUBLE      |            |
| lower_bound        | DOUBLE      |            |
| upper_bound        | DOUBLE      |            |
| status             | STRING      | NOT NULL   |
| message            | STRING      |            |
| reference_date     | TIMESTAMP   | NOT NULL   |
| execution_date     | TIMESTAMP   | NOT NULL   |

## Hive Storage Setup Scripts

Below is a HiveQL script that can be used to set up Hive results storage:

```hiveql
-- REPLACE <schema_name> and <schema_dir> with actual name and path:
set hivevar:schema_name=<schema_name>;
set hivevar:schema_dir=<schema_path>;

CREATE SCHEMA IF NOT EXISTS ${schema_name};

DROP TABLE IF EXISTS ${schema_name}.results_metric_regular;
CREATE EXTERNAL TABLE ${schema_name}.results_metric_regular
(
    job_id            STRING COMMENT '',
    metric_id         STRING COMMENT '',
    metric_name       STRING COMMENT '',
    description       STRING COMMENT '',
    source_id         STRING COMMENT '',
    column_names      STRING COMMENT '',
    params            STRING COMMENT '',
    result            DOUBLE COMMENT '',
    additional_result STRING COMMENT '',
    reference_date    TIMESTAMP COMMENT '',
    execution_date    TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Regular Metrics Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_metric_regular';

DROP TABLE IF EXISTS ${schema_name}.results_metric_composed;
CREATE EXTERNAL TABLE ${schema_name}.results_metric_composed
(
    job_id            STRING COMMENT '',
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

DROP TABLE IF EXISTS ${schema_name}.results_check_load;
CREATE EXTERNAL TABLE ${schema_name}.results_check_load
(
    job_id         STRING COMMENT '',
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

DROP TABLE IF EXISTS ${schema_name}.results_check;
CREATE EXTERNAL TABLE ${schema_name}.results_check
(
    job_id             STRING COMMENT '',
    check_id           STRING COMMENT '',
    check_name         STRING COMMENT '',
    description        STRING COMMENT '',
    source_id          STRING COMMENT '',
    base_metric        STRING COMMENT '',
    compared_metric    STRING COMMENT '',
    compared_threshold DOUBLE COMMENT '',
    lower_bound        DOUBLE COMMENT '',
    upper_bound        DOUBLE COMMENT '',
    status             STRING COMMENT '',
    message            STRING COMMENT '',
    reference_date     TIMESTAMP COMMENT '',
    execution_date     TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Checks Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_check';
```