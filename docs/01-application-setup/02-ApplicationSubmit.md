# Submitting Data Quality Application

Since Checkita framework is based on Spark, it runs as an ordinary Spark application using `spark-submit` command.
And as any Spark application, Checkita applications can be run both locally and on a cluster
(in `client` or `cluster` mode).

However, Checkita applications require some command line arguments to be passed on startup. These are:

* `-a` - *Required*. Path to HOCON file with application settings: `application.conf`.
  Note, that name of the file may vary, but usually aforementioned name is used.
* `-j` - *Required*. List of paths to job configuration files. Paths must be separated by commas.
  Hocon format supports  configuration merging, therefore, it is possible to define different parts of
  job configuration in separate files and reuse some common configuration sections.
* `-d` - *Optional*. Datetime for which the Data Quality job is being run. Date string must conform to format specified 
  in `referenceDateFormat` parameter of the application settings. If date is not provided on startup, then it will be 
  set to application start date. *This parameter is ignored when running streaming application*.
* `-l` - *Optional*. Flag indicating that application should be run in local mode.
* `-s` - *Optional*. Flag indicating that application will be run using Shared Spark Context. In this case application
  will get existing context instead of creating a new one. It is also quite important not to stop it upon job completion.
* `-m` - *Optional*. Flag indicating that storage database migration must be performed prior results saving.
* `-e` - *Optional*. Extra variables to be added to configuration files during prior parsing. These variables can be
  used in configuration files, e.g. to pass secrets. Variables are provided in key-value format:
  `"k1=v1,k2=v2,k3=v3,...""`.
* `-v` - *Optional*. Application log verbosity. By default, log level is set to `INFO`.

There are two available applications to start:

* Batch application: main class path is `org.checkita.dqf.apps.batch.DataQualityBatchApp`
* Streaming application: main class path is `org.checkita.dqf.apps.stream.DataQualityStreamApp`


The following is an example of running an application in YARN in `cluster` mode.
Framework storage database connection parameters are specified in `application.conf` and secrets may be passed either
via environment variables or via extra variables argument. For more details see 
[Usage of Environment Variables and Extra Variables](../02-general-information/02-EnvironmentAndExtraVariables.md).

```bash
export DQ_APPLICATION="<local or remote (HDFS, S3) path to application jar>"
export DQ_DEPENDENCIES="<local or remote (HDFS, S3) path to uber-jar with framework dependencies>"
export DQ_APP_CONFIG="<local or remote (HDFS, S3) path to application configuration file>"
export DQ_JOB_CONFIGS="<local or remote (HDFS, S3) paths to job configuration files separated by commas>"

# As configuration files are uploaded to driver and executors they will be located in working directories.
# Therefore, in application arguments it is required to list just their file names:
export DQ_APP_CONFIG_FILE=$(basename $DQ_APP_CONFIG)
export DQ_JOB_CONFIG_FILES="<job configuration files separated by commas (only file names)>"
export REFERENCE_DATE="2023-08-01"

# application entry point (executable class): org.checkita.dqf.apps.batch.DataQualityBatchApp
# --name spark-submit argument has a higher priority over application name set in `application.conf`

spark-submit\
   --class org.checkita.dqf.apps.batch.DataQualityBatchApp \
   --name "Checkita Data Quality" \
   --master yarn \
   --deploy-mode cluster \
   --num-executors 1 \
   --executor-memory 2g \
   --executor-cores 4 \
   --driver-memory 2g \
   --jars $DQ_DEPENDENCIES \
   --files "$DQ_APP_CONFIG,$DQ_DQ_JOB_CONFIGS" \
   --conf "spark.executor.memoryOverhead=2g" \
   --conf "spark.driver.memoryOverhead=2g" \
   --conf "spark.driver.maxResultSize=4g" \
   $DQ_APPLICATION \
   -a $DQ_APP_CONFIG_FILE \
   -j $DQ_JOB_CONFIG_FILES \
   -d $REFERENCE_DATE \
   -e "storage_db_user=some_db_user,storage_db_password=some_db_password"
```