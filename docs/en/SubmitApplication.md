# Submit Application

Since **Checkita** is powered by Spark, the framework is launched using the usual `spark-submit` command.

Like any Spark application, application with **Checkita** framework can be run both locally and on a cluster
(in `client` or `cluster` mode).

The following arguments must be passed to the framework itself at startup:

* `-a`: path to the file with application settings `application.conf`;
* `-c`: path to the configuration file with descriptions of sources, metrics, checks, etc.;
* `-d [optional]`: date for which the calculation is performed in the format specified in `application.conf`
  (by default, the application start date).
* `-l [optional]`: a flag indicating that the application should be run in local mode.
* `-r [optional]`: flag to indicate that sources should be repartitioned after reading.
* `-e [optional]`: flag, within which you can pass additional variables when starting the application.
  These variables will be added to the beginning of the configuration file specified after the `-c` flag and thus may
  be used in the description of sources, metrics and checks.
  Variables are passed in key-value format: `"k1=v1,k2=v2,k3-v3,.."`
* `-v [optional]`: flag, within which you can also pass additional variables when starting the application.
  However, these variables will be added to the top of the application configuration file `application.conf`, which is specified
  after the `-a` flag. Thus, these variables can be used to pass, for example, secrets for
  connections to SMTP or Mattermost servers.
  Variables are also passed in key-value format: `"k1=v1,k2=v2,k3-v3,.."`

The following is an example of running an application in YARN in _**cluster mode**_. Framework database connection parameters
are specified either in application.conf or in the Spark application settings (see [Application Settings](./ApplicationSettings.md)).
This example shows the second way.

```bash
export DQ_APPLICATION=<local or HDFS path to uber-jar application>
export DQ_APP_CONFIG=<local or HDFS path to application.conf>
export DQ_LOG_CONFIG=<local or HDFS path to log4j.properties>
export DQ_METRICS_CONFIG=<local or HDFS path to configuration file>

export DQ_APP_CONFIG_FILE=$(basename $DQ_APP_CONFIG)
export DQ_METRICS_CONFIG_FILE=$(basename $DQ_METRICS_CONFIG)
export REFERENCE_DATE="2023-08-01"

# application entry point (executable class):
# en.raiffeisen.checkita.apps.DQMasterBatch

spark-submit\
   --class en.raiffeisen.checkita.apps.DQMasterBatch \
   --name "Checkita Data Quality" \
   --master yarn \
   --deploy-mode cluster \
   --num-executors 1\
   --executor-memory 2g\
   --executor-cores 4\
   --driver-memory 2g\
   --files $DQ_APP_CONFIG,$DQ_LOG_CONFIG,$DQ_METRICS_CONFIG \
   --conf "spark.yarn.queue=data_science" \
   --conf "spark.default.parallelism=10" \
   --conf "spark.executor.memoryOverhead=2g" \
   --conf "spark.driver.memoryOverhead=2g" \
   --conf "spark.driver.maxResultSize=4g" \
   --conf "spark.jdbc.db_type=<database type>" \
   --conf "spark.jdbc.host=<url to connect>" \
   --conf "spark.jdbc.login=<login>" \
   --conf "spark.jdbc.password=<password>" \
   --conf "spark.jdbc.db_schema=<schema>" \
   $DQ_APPLICATION\
   -a $DQ_APP_CONFIG_FILE -c $DQ_LOG_CONFIG_FILE -d $REFERENCE_DATE \
   -e "external_db_user=some_db_user,external_db_password=some_db_password" \
   -v "mattermostToken=someToken"
```