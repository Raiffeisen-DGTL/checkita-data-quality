# Targets

The **Targets** section in the configuration file allows you to save a backup of the results,
as well as set up alerts in case any checks were not passed.

The following **Targets** types are supported:

* ***Result Targets***: allow you to save calculation results in Hive or HDFS in one of the supported formats.
  All major types of metrics and checks are supported, through the following Results Targets subtypes:
    * `columnMetrics` - saves the results of calculating column metrics;
    * `fileMetrics` - saves the results of calculating file metrics;
    * `composedMetrics` - saves the results of calculating compositional metrics;
    * `checks` - saves the results of checks.
    * `loadChecks` - stores the results of Load Checks.
* ***Error Collection***: allow you to set a list of metrics to collect their errors.
  Error collection is implemented only for `statusable` metrics (see section [Metrics](./Metrics.md)).
  Also, in order to collect errors, `keyFields` must be defined for sources - a list of columns that form
  the business key for this source. If the metric for any row returned the status "error", the values
  in the `keyFields` columns, as well as in the columns by which the metric is calculated, will be written to the report.
* ***Check Alerts***: Allows you to configure notifications if any of the specified checks fail.
* ***Summary***: Allows you to customize how reports are sent when a DQ calculation is completed.
  You can also attach error collection report to the email.

## Save Targets to Hive

In order to save Targets in Hive, you must first create a schema
with the corresponding tables.

Required parameters for saving Targets in Hive:

* `schema` - schema for Targets in Hive
* `table` - the table corresponding to the given Targets subtype.
* `partitionColumn [optional]` - name of the date column (in `string` format) by which the table is partitioned.
  Default: `execDate`.
* `date [optional]` - date for which Targets are written, if different from `executionDate`.

## Save Targets to HDFS

Targets can be saved to HDFS in one of the following formats: ***csv***, ***txt***, ***parquet***, ***orc***.

The options for storing Targets in HDFS are similar to those used to describe HDFS data sources
(See [Sources](Sources.md)).

Required parameters:

* `fileFormat` - file format (*csv*, *txt*, *parquet*, *orc*).
* `path` - the path to the directory where the Target will be saved. The file name will be generated automatically and
  will match the Target subtype.

For text formats (csv, txt), you can specify the following optional parameters:

* `delimiter [optional]` - delimiter (default: `,`)
* `quote [optional]` - enclosing quotes (default `"`)
* `quoted [optional]` - boolean parameter indicating whether all values in the file should be quoted.
  Default: `false`.
* `escape [optional]` - escape character (default ` \ `)

## Save Targets to Kafka

Result Targets can be sent as messages to a Kafka topic in `json` format.
To do this, you need to fill in the `results` section, `kafka` subsection, with the following parameters:

* `results` - list of Result Targets to send to Kafka.
* `brokerId` - id of the Kafka broker described in the [MessageBrokers](MessageBrokers.md) section,
* `topic` is the name of the Kafka topic to send messages to.
* `options [optional]` - additional options for sending messages to Kafka

## Error Collection

### Save Errors to HDFS

To configure saving reports in HDFS, you must specify the following parameters:

* `metrics [optional]` - list of metrics by which errors should be saved. If omitted, errors will be saved
  for all Statusable metrics.
* `dumpSize [optional]` - the maximum number of rows where the metric was calculated with an error that will be written to the report.
  The recommended value is <= `100` lines. The maximum allowed value is `10000`. The default value is `100`.
* `fileFormat` - file format for writing error reports: `csv`, `orc` or `parquet`.
* `path` - directory in HDFS where the report will be written.

For text format (csv), you can specify the following optional parameters:
* `delimiter [optional]` - delimiter (default: `,`)
* `quote [optional]` - enclosing quotes (default `"`)
* `quoted [optional]` - boolean parameter indicating whether all values in the file should be quoted.
  Default: `false`.
* `escape [optional]` - escape character (default `` \ ``)

### Sent Errors to Kafka

To configure sending messages on metric errors to Kafka, you must specify the following parameters:

* `metrics [optional]` - list of metrics by which errors should be saved. If omitted, errors will be saved
  across all Statusable metrics.
* `dumpSize [optional]` - the maximum number of rows where the metric was calculated with an error that will be written to the report.
  The recommended value is <= `100` lines. The maximum allowed value is `10000`. The default value is `100`.
* `brokerId` - id of the Kafka broker described in the [MessageBrokers](MessageBrokers.md) section.
* `topic` is the name of the Kafka topic to send messages to.
* `options [optional]` - additional options for sending messages to Kafka.

## Check Alerts

Sending notifications about checks that have received the `Failure` status

### Send Alerts to Email

To set up email notifications about checks, it is required to specify the following parameters:

* `id` - notification identifier (you can configure several types of notifications with different checks and different recipients).
* `checks [optional]` - list of check identifiers for which notifications will be sent.
  By default - all checks described in the configuration file.
* `mailingList` - list of recipients for sending notifications.

### Send Alerts to Mattermost

To configure notifications for reviews in Mattermost, you must specify the following settings:

* `id` - notification identifier (you can configure several types of notifications with different checks and different recipients).
* `checks [optional]` - list of check identifiers for which notifications will be sent.
  By default - all checks described in the configuration file.
* `recipients` - list of recipients for sending notifications. Notifications can be sent both to channels and direct users.
  > **Important:** Channel names must start with `#`, and to send notifications to a direct user, you need to
  > specify the name of his account, prefixed with `@`, for example: `@someUser`

### Send Alerts to Kafka

To set up sending messages about checks with a `Failure` status to Kafka, it is required to specify the following parameters:

* `id` - notification identifier (you can configure several types of notifications with different checks and different recipients).
* `checks [optional]` - list of check identifiers for which notifications will be sent.
  By default - all checks described in the configuration file.
* `brokerId` - id of the Kafka broker described in the [MessageBrokers](MessageBrokers.md) section.
* `topic` is the name of the Kafka topic to send messages to.
* `options [optional]` - additional options for sending messages to Kafka.

## Summary

### Send Summary Report to Email

To configure Email distribution of reports upon completion of each DQ calculation, it is required to specify the following parameters:

* `mailingList` - list of recipients for sending notifications.
* `attachMetricErrors [optional]` - a boolean parameter indicating whether to attach a file with a metric error report to the letter.
  Default is `false`
* `dumpSize [optional]` - the maximum number of rows where the metric was calculated with an error that will be written to the report.
  The recommended value is <= `100` lines. The maximum allowed value is `10000`. The default value is `100`.
  ***Needed only if `attachMetricErrors = true`.***

### Send Summary Report to Mattermost

To configure the distribution of reports upon completion of each DQ calculation in Mattermost, it is required to specify the following parameters:

* `recipients` - list of recipients for sending reports. Notifications can be sent both to channels and direct users.
  > **Important:** Channel names must start with `#`, and to send notifications to a direct user, you need to
  > specify the name of his account, prefixed with `@`, for example: `@someUser`
* `attachMetricErrors [optional]` - a boolean parameter indicating whether to attach a file with a metric error report to the letter.
  Default is `false`
* `dumpSize [optional]` - the maximum number of rows where the metric was calculated with an error that will be written to the report.
  The recommended value is <= `100` lines. The maximum allowed value is `10000`. The default value is `100`.
  ***Needed only if `attachMetricErrors = true`.***

### Send Summary Report to Mattermost

To set up sending reports at the end of each DQ calculation in Kafka, you must specify the following parameters:

> **Important** If a report is sent as a message to Kafka, naturally, the report cannot be attached to it
> with metric errors. To do this, in the `errorCollection` section, you must also configure the sending of an error message
> by metrics to Kafka.

* `brokerId` - id of the Kafka broker described in the [MessageBrokers](MessageBrokers.md) section.
* `topic` - the name of the Kafka topic to send messages to.
* `options [optional]` - additional options for sending messages to Kafka.

The following is an example of a `targets` section in a configuration file:

```hocon
targets: {
  hive: {
    columnMetrics: {schema: "some_schema", table: "DQ_COLUMNAR_METRICS", partitionColumn: "load_date"},
    fileMetrics: {schema: "some_schema", table: "DQ_FILE_METRICS", partitionColumn: "load_date"},
    composedMetrics: {schema: "some_schema", table: "DQ_COMPOSED_METRICS", partitionColumn: "load_date"}
  }
  hdfs: {
    checks: {
      fileFormat: "csv"
      path: "/tmp/dataquality/results"
      delimiter: ":",
      quote: "'",
      escape: "|",
    },
    loadChecks: {fileFormat: "orc", path: "/tmp/dataquality/results"}
  }
  results: {
    kafka: {
      results: ["columnMetrics", "fileMetrics", "composedMetrics", "loadChecks", "checks"],
      brokerId: "kafka1"
      topic: "some.topic"
    }
  }
  errorCollection: {
    hdfs: {
      metrics: ["metric_1", "metric_2", "metric_n"]
      dumpSize: 50
      fileFormat: "csv"
      path: "tmp/DQ/ERRORS"
      // metric errors report contains JSON string in column ROW_DATA, therefore,
      // it is reccomended to use semicolon or tab separator for csv file.
      delimiter: ";" 
      quote: "\""
      escape: "\\"
      quoted: false
    }
    kafka: {
      metrics: ["hive_table1_nulls", "fiexed_file1_dist_name", "parquet_file_id_regex"]
      dumpSize: 25
      brokerId: "kafka1"
      topic: "some.topic"
    }
  }
  summary: {
    email: {
      attachMetricErrors: true
      metrics: ["metric_1", "metric_2", "metric_n"]
      dumpSize: 10
      mailingList: ["some.user@some.domain"]
    }
    mattermost: {
      attachMetricErrors: true
      metrics: ["hive_table1_nulls", "fiexed_file1_dist_name", "parquet_file_id_regex"]
      dumpSize: 10
      recipients: ["@someUser", "#someChannel"]
    }
    kafka: {
      brokerId: "kafka1"
      topic: "some.topic"
    }
  }
  checkAlerts: {
    email: [
      {
        id: "alert1"
        checks: ["check_1", "check_2"]
        mailingList: ["preson1@some.domain", "person2@some.domain"]
      }
      {
        id: "alert2"
        checks: ["check_3", "check_n"]
        mailingList: ["preson3@some.domain"]
      }
    ]
    mattermost: [
      {
        id: "alert1"
        checks: ["avg_bal_check", "zero_nulls"]
        recipients: ["@someUser"]
      }
      {
        id: "alert2"
        checks: ["top2_curr_match", "completeness_check"]
        recipients: ["#someChannel"]
      }
    ]
    kafka: [
      {
        id: "alert1"
        checks: ["avg_bal_check", "zero_nulls"]
        brokerId: "kafka1"
        topic: "some.topic"
      }
      {
        id: "alert2"
        checks: ["top2_curr_match", "completeness_check"]
        brokerId: "kafka1"
        topic: "some.topic"
      }
    ]
  }
}
```