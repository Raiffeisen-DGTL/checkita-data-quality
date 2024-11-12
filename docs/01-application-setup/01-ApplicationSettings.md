# Application Settings

General Checkita Data Quality settings are configured in [Hocon](https://github.com/lightbend/config/blob/main/HOCON.md)
file `application.conf` which is supplied to the application on the startup.
All configurations are set within `appConfig` section.

There is only one parameter that is set at the top level and this is `applicationName` - name of the Spark application.
This parameter is optional and if not set, then `Checkita Data Quality` application name is used by default.

The rest of the parameters are defined in the subsections that are described below.

## DateTime Settings

DateTime configurations are set in the `dateTimeOptions` section. 
Please, see [Working with Date and Time](../02-general-information/01-WorkingWithDateTime.md) 
section for more details on working with date and time in Checkita Framework.

DateTime settings include following:

* `timeZone` - Time zone in which string representation of reference date and execution date are parsed and rendered.
    *Optional, default is `"UTC"`*.
* `referenceDateFormat` - datetime format used to parse and render reference date.
    *Optional, default is `"yyyy-MM-dd'T'HH:mm:ss.SSS"`*.
* `executionDateFormat` - datetime format used to parse and render execution date.
    *Optional, default is `"yyyy-MM-dd'T'HH:mm:ss.SSS"`*
 
If `dateTimeOptions` section is missing then default values are used for all parameters above.

## Streaming Settings

These settings are only applicable to streaming applications and define various aspects of running data quality checks
for streaming sources. Please, see [Data Quality Checks over Streaming Sources](../02-general-information/05-StreamingMode.md)
section for more details on runnig data quality checks over streaming sources.

* `trigger` - Trigger interval: defines time interval for which micro-batches are collected. *Optional, default is `10s`*.
* `window` - Window interval: defines tabbing window size used to accumulate metrics. 
  All metrics results and checks are evaluated per each window once it finalised. *Optional, default is `10m`*.
* `watermark` - Watermark level: defines time interval after which late records are no longer processed.
    *Optional, default is `5m`*.
* `allowEmptyWindows` - Boolean flag indicating whether empty windows are allowed. Thus, in situation when window is 
  below watermark and for some of the processed streams there are no results then all related checks will be skipped 
  if this flag is set to `true`. Otherwise, checks will be processed and will return error status with 
  `... metric results were not found ...` type of message. *Optional, default is `false`*.
* `checkpointDir` - Checkpoint directory location. *Optional, if empty, then checkpoints will not be written.*

> **IMPORTANT** All intervals must be defined as a duration string which should conform to 
> [Scala Duration](https://www.scala-lang.org/api/2.12.4/scala/concurrent/duration/Duration.html) format.

## Enablers

Section `enablers` of application configuration file defines various boolean switchers is single-value parameters
that controls various aspects of data quality job execution:

* `allowSqlQueries` - Enables usage arbitrary SQL queries in data quality job configuration.
    *Optional, default is `false`*
* `allowNotifications` - Enables notifications to be sent from DQ application. 
    *Optional, default is `false`*
* `aggregatedKafkaOutput` - Enables sending aggregates messages for Kafka Targets (one per each target type).
  By default, kafka messages are sent per each result entity.
    *Optional, default is `false`*
* `enableCaseSensitivity` - Enable columns case sensitivity. Controls column names comparison and lookup.
    *Optional, default is `false`*
* `errorDumpSize` - Maximum number of errors to be collected per single metric. Framework is able to collect source 
  data rows where metric evaluation yielded some errors. But in order to prevent OOM the number of collected errors
  have to be limited to a reasonable value. Thus, maximum allowable number of errors per metric is `10000`.
  It is possible to lower this number by setting this parameter. *Optional, default is `10000`*
* `outputRepartition` - Sets the number of partitions when writing outputs. By default, writes single file.
    *Optional, default is `1`*
* `metricEngineAPI` - Sets engine to be used for regular metric processing: `rdd` (RDD-engine) or `df` (DF-engine) are
  available. It is recommended to use DF-engine for batch applications while streaming applications support only
  RDD-engine. *Optional, default is `rdd`*.
* `checkFailureTolerance` - Sets check failure tolerance for the application, i.e. whether the application should 
  return non-zero exit code when some the checks have failed. 
  For more info, see [Check Failure Tolerance](../02-general-information/08-CheckFailureTolerance.md).
    *Optional, default is `none`*

If `enablers` section is missing then default values are used for all parameters above.

## Storage Configuration

Parameters for connecting to Data Quality results storage are defined in `storage` section of application configuration.

For more information on results storage refer to [Data Quality Results Storage](03-ResultsStorage.md) chapter
of the documentation.

Thus, connection to storage is configured using following parameters:

* `dbType` - Type of database used to store Data Quality results. *Required*.
* `url` - Database connection URL (without protocol identifiers). *Required*.
* `username` - Username to connect to database with (if required). *Optional*.
* `password` - Password to connect to database with (if required). *Optional*.
* `schema` - Schema where data quality tables are located (if required). *Optional*.
* `saveErrorsToStorage` - Enables metric errors to be stored in storage database. *Optional, default is `false`*.

> **IMPORTANT** If `storage` section is missing then application will run without usage of results storage:
> 
> * results won't be saved (only targets can be sent);
> * trend checks (used for anomaly detection in data) won't be performed as they require historical data.
> 
> In addition, be mindful when storing metric errors to storage database. Depending on `errorDumpSize` settings, 
> the number of collected errors could be quite large. This will load to overloading DQ storage as well as increase
> database write operations execution time. Another concern is related to the fact that metric errors contain 
> data excerpts from sources being checked. These excerpts might contain some sensitive information that is rather 
> not to be stored in DQ storage database. Alternatively, these excerpts can be encrypted before storing. 
> See [Encryption](#encryption) configuration for more details.

## Email Configuration

In order to send notification via email it is necessary to configure connection to SMTP server which should be defined
in `email` section of application configuration with following parameters:

* `host` - SMTP server host. *Required*.
* `port` - SMTP server port. *Required*.
* `address` - Email address to sent notification from. *Required*.
* `name` - Name of the sender. *Required*.
* `sslOnConnect` - Boolean parameter indicating whether to use SSL on connect. *Optional, default is `false`*.
* `tlsEnabled` - Boolean parameter indicating whether to enable TLS. *Optional, default is `false`*.
* `username` - Username for connection to SMTP server (if required). *Optional*.
* `password` - Password for connection to SMTP server (if required). *Optional*.

If `email` section is missing then email notifications cannot be sent. If ones were configured in job configuration,
then exception would be thrown at runtime.

## Mattermost Configuration

In order to send notification to Mattermost it is necessary to configure connection to Mattermost API which
should be defined in `mattermost` section of application configuration with following parameters:

* `host` - Mattermost API host.
* `token` - Mattermost API token (using Bot accounts for notifications is preferable).

If `mattermost` section is missing then corresponding notifications cannot be sent. If ones were configured in job 
configuration, then exception would be thrown at runtime.

## Default Spark Parameters

It is also possible to provide list of default Spark configuration parameters used across multiple jobs.
These parameters should be provided as `defaultSparkOptions` list where each parameter is a string in format:
`spark.param.name=spark.param.value`.

## Encryption

When `storage` section is defined, it is also recommended to use `encryption` section in order to protect sensitive 
information in job config. This should be done by defining the parameters within the application configuration file:

* `secret` -  Secret string used to encrypt/decrypt sensitive fields. This string should contain at least 32 characters. 
*Required*. 
* `keyFields` - List of key fields used to identify fields that requires encryption/decryption. 
*Optional, default is `[password, secret]`*.
* `encryptErrorData` - Boolean flag indicating whether it is necessary tp encrypt data excerpts within collected 
  metric errors. *Optional, default is `false`*

If `encryption` section is missing then any sensitive information will not be encrypted.

> **IMPORTANT** Both keys of job configuration and data excerpts that metric errors contain might 
> contain some sensitive information. Storing raw sensitive information  in DQ storage database 
> might not satisfy security requirements. Therefore, DQ framework offers functionality to encrypt 
> sensitive data with AES256 encryption algorithm. As AES25 is a symmetric algorithm then encrypted 
> data can be decrypted with use secret key if needed.

## Example of Application Configuration File

Hocon configuration format supports variable substitution and Checkita Data Quality framework has a mechanism to 
feed configuration files with extra variables at runtime. For more information,
see [Usage of Environment Variables and Extra Variables](../02-general-information/02-EnvironmentAndExtraVariables.md) 
chapter of the documentation.

```hocon
appConfig: {

  applicationName: "Custom Data Quality Application Name"
  
  dateTimeOptions: {
    timeZone: "GMT+3"
    referenceDateFormat: "yyyy-MM-dd"
    executionDateFormat: "yyyy-MM-dd-HH-mm-ss"
  }

  enablers: {
    allowSqlQueries: false
    allowNotifications: true
    aggregatedKafkaOutput: true
  }

  defaultSparkOptions: [
    "spark.sql.orc.enabled=true"
    "spark.sql.parquet.compression.codec=snappy"
    "spark.sql.autoBroadcastJoinThreshold=-1"
  ]

  storage: {
    dbType: "postgres"
    url: "localhost:5432/public"
    username: "postgres"
    password: "postgres"
    schema: "dqdb"
    saveErrorsToStorage: true
  }

  email: {
    host: "smtp.some-company.domain"
    port: "25"
    username: "emailUser"
    password: "emailPassword"
    address: "some.service@some-company.domain"
    name: "Data Quality Service"
    sslOnConnect: true
  }

  mattermost: {
    host: "https://some-team.mattermost.com"
    token: ${dqMattermostToken}
  }

  encryption: {
    secret: "secretmustbeatleastthirtytwocharacters"
    keyFields: ["password", "username", "url"]
    encryptErrorData: true
  }
}
```

