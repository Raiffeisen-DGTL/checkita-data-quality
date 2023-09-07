# Application Settings

Framework settings are stored in two files:

* `application.conf` - file with basic application settings.
* `log4j.properties` - file with logging parameters. _**Changing this file is not recommended!**_

The `application.conf` file specifies the following application parameters:

* `application_name`: default Spark Application name (if not specified, **Checkita Data Quality** is used);
* `run_configuration_version`: version of the configuration file (currently supported versions are 0.9 and 1.0,
  it is preferable to use a newer one, which is described in the documentation);
* `referenceDateFormat` date format which is used to render the date for which the calculation is performed.
  The actual date is passed in the `-d` flag at startup and must have the appropriate format;
* `executionDateFormat` date format which is used to render the actual date when the application was started;
  The data model contains both the reference date (for which the calculation is performed)
  and the application started date;
* `timeZone` is the time zone in which the application is started and in which the string representations of
  the date and time are given.
* `aggregatedKafkaOutput` Boolean parameter that allows you to aggregate messages that are sent to Kafka
  topic: one large message per target type (default - `false`);
* `enableSharedSparkContext` Boolean parameter that allows the application to run in the cluster with
  Shared SparkContext (default - `false`).
* `s3_bucket`: url to connect to a bucket in S3 storage (not specified by default);
* `sql_warehouse_path`: absolute path to hive warehouse (not specified by default);
* `hbase_host`: host to connect to HBase (not specified by default, **HBase support is temporarily unavailable**);
* `tmp_files_management`: path to save temporary files when the application is running:
    * `local_fs_path`: path in the local file system;
    * `hdfsPpath`: path in HDFS;
* `metric_error_management`: a group of parameters for setting error collection during metrics calculation:
    * `dump_directory_path`: absolute path where logs with metric calculations errors will be saved;
    * `dump_size`: the maximum number of errors that will be logged for each metric and each partition
      (default: 10000).
    * `empty_file`: write an empty file even if there were no errors (default: false);
    * `file_config`: settings for the format in which error messages are written:
        * `format`: file format (default **csv**);
        * `delimiter`: delimiter (default ` , `);
        * `quote`: enclosing quotes (default ` " `);
        * `escape`: escape character (default ` \ `);
        * `quote_mode`: mode of quoting values (default **"ALL"**);
* `virtual_sources_management`: a group of options for configuring the storage of virtual sources:
    * `dump_directory_path`: absolute path where virtual sources will be saved;
    * `file_format`: file format in which sources will be saved;
    * `delimiter`: delimiter (default ` , `);
* `storage`: a group of parameters for setting up a connection to the framework database.
  **Specifying these parameters is critical for the successful operation of the Data Quality application!**
    * `type`: a way to specify parameters for connecting to the database. At the moment connection parameters can be specified
      either directly in the application.conf file (`type: "APP_CONFIG"`) or in the parameters to the Spark Application
      (`type: "SPARK_CONFIG"`).
    * `config`: database connection parameters (specified only for `type: "APP_CONFIG"`);
        * `subtype`: the following database types are supported: **"POSTGRES"**, **"SQLITE"**, **"HIVE"**, **"FILE"**;
        * `host`: URL to connect to;
        * `user`: user login;
        * `password`: user password;
        * `schema`: schema in which the results of metrics and checks calculations are stored;
    * If `type: "APP_CONFIG"` then similar settings must be specified in the Spark Application options:
        * `spark.jdbc.db_type`
        * `spark.jdbc.host`
        * `spark.jdbc.login`
        * `spark.jdbc.password`
        * `spark.jdbc.db_schema`
* `mailing`: a group of options for setting up email notifications.
    * `notifications`: true/false - whether notifications should be sent (false by default);
    * `mode`: notification sending mode (not specified by default - notifications are disabled).
        * `external`: external SMTP Server is used;
        * `internal`: use internal SMTP to send messages via bash;
    * `mailing_script_path`: path to the bash script for sending messages;
    * `config`:
        * `address`: sender's e-mail;
        * `hostname`: SMTP server host;
        * `smtpPort`: SMTP server port;
        * `username`: login;
        * `password`: password;
        * `sslOnConnect`: true/false to enable/disable SSL secure connection.
        * `tlsEnables`: true/false to enable/disable TLS.
* `mattermost`: a group of options to configure how notifications are sent to Mattermost:
    * `host` - Mattermost server host;
    * `token` - access token for connection to Mattermost API.

Example of application configuration file can be found here: [application.conf](../examples/application.conf).