# Metric Error Collection

Metric calculation involves reading data row by row and incrementing metric value for each row. During increment step
there could be something wrong: either due to problems with data or due to some unexpected runtime errors. In addition,
some metrics have logical condition that needs to be met in order to increment the metric value. Failing to satisfy
this condition is also considered as failure.

Thus, in the situations, described above, there will be error collection mechanism triggered and following data 
error or failure data collected:

* Metric information: metric id and list of columns;
* Source information over which metric is calculated: source id and list of key fields.
* Error information: status (either `Failure` or `Success`) and message.
* Excerpt from row data: only values from metric columns and key fields are collected.

Since the processed source can be extremely large and, subsequently, can yield large amount of metric errors then
out-of-memory errors are likely to happen. In order to prevent that, the number of errors collected per each metric
is limited. Thus, maximum number of errors collected per metric cannot be more than `10000`. This number can be
additionally limited in the application settings by setting `errorDumpSize` parameter to a lower number.
See [Enablers](../01-application-setup/01-ApplicationSettings.md#enablers) chapter for more details.

Collected metric errors could be used to identify and debug problems in the data. In order to save or send metric error
reports, [Error Collection Targets](../03-job-configuration/08-Targets.md#error-collection-targets) can be configured in
`targets` section of job configuration. Note that error collection reports will contain excerpts from data and,
therefore, should be communicated with caution. For the same reason they are never saved in Data Quality storage.