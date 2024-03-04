# Targets Configuration

Targets are designed to provide alternative channels for sending results. First of all, targets can be used to send
notifications to users about problems in their data or just send summary of Data Quality job. In addition, targets
provide different ways for saving results, e.g. write them to file in HDFS or send to Kafka topic.

All targets are configured in `targets` section of the job configuration.
There are four general types of targets that can be configured depending on what information is being sent or saved:

* [Result Targets](#result-targets) - used to save results as file or send them to Kafka in addition to storing into
  Data Quality storage.
* [Error Collection Targets](#error-collection-targets) - used to save collected metric errors as file or to send them
  to Kafka topic. Note that metric errors are not saved in the Data Quality storage as they contain some excerpts from 
  data being checked. For more information, see [Metric Error Collection](../02-general-concepts/04-ErrorCollection.md)
  chapter.
* [Summary Targets](#summary-targets) - used to send notifications with summary report for Data Quality job.
  Summary report may also be sent to Kafka topic.
* [Check Alert Targets](#check-alert-targets) - used to watch over some checks and send notification to users in case if
  some watched checks have failed.

## Result Targets

Results targets are configured in the `results` subsection and can be one of the following type depending on where
they are sent or saved:

* `file` - Save results as file in local or remote (HDFS, S3, etc.) file system.
* `hive` - Save results in HDFS as Hive table. Note that Hive table with required schema must be created prior
  results saving.
* `kafka` - Send results to Kafka topic in JSON format.

For result target of any type it is required to configure list of result to be saved or sent:

* `resultTypes` - *Required*. List of result types to save or sent. May include following:
  `regularMetrics`, `composedMetrics`, `loadChecks`, `checks`, `jobState`. Note that all results types are reduced 
   to [Unified Targets Schema](#unified-targets-schema) and saved together.

### Save Results to File

In order to save results to file, it is required to configure result target of `file` type. In addition to list of 
saved results, it is required to configure file output.

* `save` - *Required*. File output configuration used to save results.
  For more information on configuring file outputs, see [File Output Configuration](11-FileOutputs.md) chapter.

File with results will have [Unified Targets Schema](#unified-targets-schema).

### Save Results to Hive

In order to save results to Hive table, it is required to configure result target of `hive` type. Hive table to which 
results will be saved must be created in advance with [Unified Targets Schema](#unified-targets-schema).

Thus, in addition to list of saved results, it is required to indicate Hive schema and table:

* `schema` - *Required*. Hive schema.
* `table` - *Required*. Hive table.

Note that results will be appended to Hive table.

### Send Results to Kafka

In order to send results to Kafka topic, it is required to configure result target of `kafka` type. 
Connection to Kafka cluster must be configured in `connections` section of job configuration as described in 
[Kafka Connection Configuration](01-Connections.md#kafka-connection-configuration). 

Thus, in addition to list of saved results, it is required provide following parameters:

* `connection` - *Required*. Kafka connection ID.
* `topic` - *Required*. Kafka topic to send results to.
* `options` - *Optional*. Additional list of Kafka parameters for sending messages to topic. 
  Parameters are provided as a strings in format of `parameterName=parameterValue`.

Results will be saved as JSON messages. In addition, `aggregatedKafkaOutput` parameter configured in application 
settings controls how results will be sent (see [Enablers](../01-application-setup/01-ApplicationSettings.md#enablers) chapter):

* One message per each result.
* One large message with list of all results.

## Error Collection Targets

Error collection targets are configured in `errorCollection` subsection and can be one of the following type depending 
on where metric errors are sent or saved:

* `file` - Save metric errors as file in local or remote (HDFS, S3, etc.) file system.
* `hive` - Save metric errors in HDFS as Hive table. Note that Hive table with required schema must be created prior
  metric errors saving.
* `kafka` - Send metric errors to Kafka topic in JSON format.

Note that metric errors are transformed to [Unified Targets Schema](#unified-targets-schema) when send or saved.

For error collection target of any type the following parameters can be supplied:

* `metrics` - *Optional*. List of metric for which errors will be saved. If omitted, then errors are saved for all
  metrics defined in Data Quality job.
* `dumpSize` - *Optional, default is `100`*. Allows additionally limit number of errors saved per metric in order to 
  make reports more compact. Could not be larger, than application-level limitation as described in 
  [Enablers](../01-application-setup/01-ApplicationSettings.md#enablers) chapter.

### Save Metric Errors to File

In order to save metric errors to file, it is required to configure error collection target of `file` type. 
In addition to common error collection target parameters, it is required to configure file output:

* `save` - *Required*. File output configuration used to save results.
  For more information on configuring file outputs, see [File Output Configuration](11-FileOutputs.md) chapter.

File with metric errors will have [Unified Targets Schema](#unified-targets-schema).

### Save Metric Errors to Hive

In order to save metric errors to Hive table, it is required to configure result error collection target of `hive` type.
Hive table to which metric errors will be saved must be created in advance with [Unified Targets Schema](#unified-targets-schema).

Thus, in addition to common error collection target parameters, it is required to indicate Hive schema and table:

* `schema` - *Required*. Hive schema.
* `table` - *Required*. Hive table.

Note that metric errors will be appended to Hive table.

### Send Metric Errors to Kafka

In order to send metric errors to Kafka topic, it is required to configure error collection target of `kafka` type.
Connection to Kafka cluster must be configured in `connections` section of job configuration as described in
[Kafka Connection Configuration](01-Connections.md#kafka-connection-configuration).

Thus, in addition to common error collection target parameters, it is required provide following ones:

* `connection` - *Required*. Kafka connection ID.
* `topic` - *Required*. Kafka topic to send results to.
* `options` - *Optional*. Additional list of Kafka parameters for sending messages to topic.
  Parameters are provided as a strings in format of `parameterName=parameterValue`.

Metric errors will be saved as JSON messages. In addition, `aggregatedKafkaOutput` parameter configured in application
settings controls how metric errors will be sent 
(see [Enablers](../01-application-setup/01-ApplicationSettings.md#enablers) chapter):

* One message per each result.
* One large message with list of all results. 

> **IMPORTANT**. Be careful, when using this option for saving metric errors as there could
> be a significant number of them. In order to fit into Kafka message size limits it is recommended to limit number
> of errors sent per each metric by setting `dumpSize` parameter to a reasonably low number.

## Summary Targets

Checkita framework collects summary upon completion of each Data Quality job. Summary targets are designed accordingly,
to enable sending summary reports to users. Thus, summary targets are configured in `summary` subsection and can be 
one of the following type depending on where summary reports are sent or saved:

* `email` - Send summary report to user(s) via email.
* `mattermost` - Send summary report to mattermost either to channel or to user's direct messages.
* `kafka` - Send summary report to Kafka topic in JSON format. When sending summary report to Kafka, it is transformed
  to [Unified Targets Schema](#unified-targets-schema).

For summary target of `email` or `mattermost` type the following parameters can be supplied:

* `attachMetricErrors` - *Optional, default is `false`*. Boolean parameter indicating whether report with collected
  metric errors should be attached to email or message with summary report.
* `attachFailedChecks` - *Optional, default is `false`*. Boolean parameter indicating whether report with failed checks
  should be attached to email or message with summary report.
* `metrics` - *Optional*. If `attachMetricErrors` is set to `true`, then this parameter can be used to specify list of 
  metric for which errors will be saved. If omitted, then errors are saved for all metrics defined in Data Quality job.
* `dumpSize` - *Optional, default is `100`*. If `attachMetricErrors` is set to `true`, then this parameter allows 
  additionally limit number of errors saved per metric in order to make report more compact. 
  Could not be larger, than application-level limitation as described in
  [Enablers](../01-application-setup/01-ApplicationSettings.md#enablers) chapter.

### Send Summary to Email

In order to send summary report via email, it is required to configure summary target of `email` type.
In addition to common summary target parameters, it is required to configure following ones:

* `recipients` - *Required*. List of recipients' emails to which summary report will be sent.
* `template` - *Optional*. HTML template to build email body.
* `templateFile` - *Optional*. Location of the file with HTML template to build email body.

HTML template is optional. If HTML template is not provided then the default summary report body is 
compiled. Moreover, it should be noted, that `template` parameter has higher priority than `templateFile` one. 
Therefore, if both of them are set then explicitly defined then HTML template from `template` parameter is used.

In addition, HTML templates support parameter substitution using 
[Mustache Template](https://mustache.github.io/mustache.5.html) notation, e.g.:
`This {{ parameterName }} has a value of {{ parameterValue }}`. List of available parameters that can be used for
substitution in HTML templates is given in 
[Job Summary Parameters Available for Templates](#job-summary-parameters-available-for-templates) chapter below.

### Send Summary to Mattermost

In order to send summary report to mattermost, it is required to configure summary target of `mattermost` type.
In addition to common summary target parameters, it is required to configure following ones:

* `recipients` - *Required*. List of recipients' to which summary report will be sent. Message can be sent either to
  a channel or to a user's direct messages:
    * When sending message to a channel, it is required to specify channel name prefixed with `#` sign: `#someChannel`.
    * When sending message to a user's direct messages, it is required to specify username with `@` prefix: `@someUser`.
* `template` - *Optional*. Markdown template to build message body.
* `templateFile` - *Optional*. Location of the file with Markdown template to build message body.

Markdown template is optional. If Markdown template is not provided then the default summary report body is
compiled. Moreover, it should be noted, that `template` parameter has higher priority than `templateFile` one.
Therefore, if both of them are set then explicitly defined then Markdown template from `template` parameter is used.

In addition, Markdown templates support parameter substitution using
[Mustache Template](https://mustache.github.io/mustache.5.html) notation, e.g.:
`This {{ parameterName }} has a value of {{ parameterValue }}`. List of available parameters that can be used for
substitution in Markdown templates is given in
[Job Summary Parameters Available for Templates](#job-summary-parameters-available-for-templates) chapter below.

### Send Summary to Kafka

In order to send summary report to Kafka topic, it is required to configure summary target of `kafka` type.
Connection to Kafka cluster must be configured in `connections` section of job configuration as described in
[Kafka Connection Configuration](01-Connections.md#kafka-connection-configuration).

Kafka messages do not support any from of attachments, therefore, only summary report itself can be sent to Kafka topic.
Summary report is sent in form of JSON string that will contain all the parameters defined in
[Job Summary Parameters Available for Templates](#job-summary-parameters-available-for-templates) chapter below.
JSON string format will conform to [Unified Targets Schema](#unified-targets-schema).

Thus, in order to configure `kafka` summary target it is required to specify following parameters:

* `connection` - *Required*. Kafka connection ID.
* `topic` - *Required*. Kafka topic to send results to.
* `options` - *Optional*. Additional list of Kafka parameters for sending messages to topic.
  Parameters are provided as a strings in format of `parameterName=parameterValue`.

## Check Alert Targets

Check alert targets are developed specifically to enable notification sending in case if some of watched checks 
have failed. These targets are configured in `checkAlert` subsection and can be one of the following type depending 
on where alerts are sent:

* `email` - Send check alert to user(s) via email.
* `mattermost` - Send check alert to mattermost either to channel or to user's direct messages.

For check alert target of any type the following parameters can be supplied:

* `id` - *Required*. ID of check alert. There could be different check alert configurations for different sets of checks.
  Therefore, check alerts should have an ID, in order to distinguish them.
* `checks` - *Optional*. List of watched checks. If any of watched checks fails then alert notification is sent.
  If omitted, then all checks defined in the Data Quality job are being watched.

### Send Check Alerts to Email

In order to send check alert via email, it is required to configure check alert target of `email` type.
In addition to common check alert target parameters, it is required to configure following ones:

* `recipients` - *Required*. List of recipients' emails to which check alert will be sent.
* `template` - *Optional*. HTML template to build email body.
* `templateFile` - *Optional*. Location of the file with HTML template to build email body.

HTML template is optional. If HTML template is not provided then the default check alert body is
compiled. Moreover, it should be noted, that `template` parameter has higher priority than `templateFile` one.
Therefore, if both of them are set then explicitly defined then HTML template from `template` parameter is used.

In addition, HTML templates support parameter substitution using
[Mustache Template](https://mustache.github.io/mustache.5.html) notation, e.g.:
`This {{ parameterName }} has a value of {{ parameterValue }}`. List of available parameters that can be used for
substitution in HTML templates is given in
[Job Summary Parameters Available for Templates](#job-summary-parameters-available-for-templates) chapter below.

### Send Check Alerts to Mattermost

In order to check alert to mattermost, it is required to configure check alert target of `mattermost` type.
In addition to common check alert target parameters, it is required to configure following ones:

* `recipients` - *Required*. List of recipients' to which check alert will be sent. Message can be sent either to
  a channel or to a user's direct messages:
  * When sending message to a channel, it is required to specify channel name prefixed with `#` sign: `#someChannel`.
  * When sending message to a user's direct messages, it is required to specify username with `@` prefix: `@someUser`.
* `template` - *Optional*. Markdown template to build message body.
* `templateFile` - *Optional*. Location of the file with Markdown template to build message body.

Markdown template is optional. If Markdown template is not provided then the default check alert body is
compiled. Moreover, it should be noted, that `template` parameter has higher priority than `templateFile` one.
Therefore, if both of them are set then explicitly defined then Markdown template from `template` parameter is used.

In addition, Markdown templates support parameter substitution using
[Mustache Template](https://mustache.github.io/mustache.5.html) notation, e.g.:
`This {{ parameterName }} has a value of {{ parameterValue }}`. List of available parameters that can be used for
substitution in Markdown templates is given in
[Job Summary Parameters Available for Templates](#job-summary-parameters-available-for-templates) chapter below.

## Unified Targets Schema

All targets that are saved to five or sent to Kafka are reduced to unified schema. Such approach have some advantages:

* Results of various types can be sent all together as single file or a large Kafka message.
* Saved targets from different Data Quality jobs can be merged into a larger file. This allows to avoid "small files"
  problem when saving targets in HDFS or S3.
* As all targets sent to Kafka topic conform to unified schema then it is easier to parse message with different type
  of results.

Thus, unified schema is following:

| Column Name       | Column Type | Comment                                             |
|-------------------|-------------|-----------------------------------------------------|
| jobId             | STRING      | ID of Data Quality Job                              |
| referenceDate     | STRING      | Reference datetime for which job is run             |
| executionDate     | STRING      | Datetime of actual job start                        |
| entityType        | STRING      | Type of result                                      |
| data              | STRING      | JSON string. Content varies depending in entityType |

From the schema above it is seen that all data that is specific to a results of each type is stored as JSON string.
When sending results to Kafka, the schema would be the same but `data` will become a nested JSON object.

## Job Summary Parameters Available for Templates

It is already noted that HTML or Markdown templates used to build body of notifications support parameter substitution 
using [Mustache Template](https://mustache.github.io/mustache.5.html) notation. List of available parameters that can 
be used for substitution is shown below.

For example, Markdown template with check alert notification could look like:

```markdown
# Checkita Data Quality Notification - Failed Check Alert

You requested notifications on failed checks in Data Quality Job: `{{ jobId}}`.

Inform you that some watched checks have failed for job started for:

* Reference date: `{{ referenceDate }}`
* Execution date: `{{ executionDate }}`

Attached files contain information about failed checks. Please, review them.
```

* `jobId` - ID of the current Data Quality job.
* `jobStatus` - Job status: `Success` if all checks are passed, `Failure` otherwise.
* `referenceDate` - Reference datetime for which job is run.
* `executionDate` - Datetime of actual job start.
* `numSources` - Total number of sources in the job.
* `numMetrics` - Total number of metric in the job.
* `numChecks` - Total number of checks in the job.
* `numLoadChecks` - Total number of load checks in the job.
* `numMetricsWithErrors` - Number of metrics that yielded errors during their computation.
* `numFailedChecks` - Number of failed checks.
* `numFailedLoadChecks` - Number of failed load checks.
* `listMetricsWithErrors` - List of all metrics that yielded errors during their computation.
* `listFailedChecks` - List of failed checks.
* `listFailedLoadChecks` - List of failed load checks.

## Targets Configuration Example

As it is shown in the example below, targets are grouped into subsections named after their type.
These subsections may contain various target configuration depending on the channel where targets are saved or sent.
Due to multiple check alert configurations are allowed then they are grouped as list of check alerts sent to a specific
channel (email or mattermost).

```hocon
jobConfig: {
  targets: {
    results: {
      file: {
        resultTypes: ["checks", "loadChecks"]
        save: {
          kind: "delimited"
          path: "/tmp/dataquality/results"
          header: true
        }
      }
      hive: {
        resultTypes: ["regularMetrics", "composedMetrics", "loadChecks", "checks", "jobState"],
        schema: "WORKSPACE_CIBAA",
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
          path: "tmp/DQ/ERRORS"
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