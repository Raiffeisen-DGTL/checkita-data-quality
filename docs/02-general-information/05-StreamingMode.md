# Data Quality Checks over Streaming Sources

> **IMPORTANT** Functionality of performing data quality checks over streaming sources is currently in experimental 
> state and is subjected to changes.

As it has already been stated Checkita Data Quality framework has ability to calculate metrics and perform 
quality checks over streaming data sources. As Spark is used as a computation engine, then 
[Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
API is used to run metric calculations over streaming data sources.

The core idea of running data quality job in streaming mode is to retain the ability to process multiple data sources
at the same time. As metrics calculation is a stateful operation then all streaming sources are processed per tabbing 
windows. In order to process multiple sources simultaneously, their windows must be synchronized: (1) be of the same 
size and (2) starting at the same time. Therefore, window size is set at the application level and is used for all 
processed streaming source.

As streaming sources are processed per each window, then it is crucial to provide time value used to assign record to
a particular window. Following options are supported:

* `Processing time` - Spark builds time value for each record using `current_timestamp` function.
* `Event time` - Mostly applicable to kafka topics: time value is obtained from `timestamp` column which correspond to
  message creation time (a.k.a. event time).
* `Custom time` - Uses user-defined column of *timestamp* type that is used to provide time value for window assignment.

Another thing to care about is how to finalize windows state. In other words, it is required to establish rules on 
when we can consider window state is final and assume that no new records will arrive to this window. Common approach
to resolve this problem in streaming processing is to use "watermarks". Watermark holds a time value which sets a level
to accept new records for processing. If record's time is below the watermark level, then it is considered to be "late"
and is not processed. Watermark is defined as maximum observed record time minus predefined offset. For more details
see Spark documentation: 
[Handling Late Data and Watermarking](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking).
For purpose of synchronous processing of multiple streaming sources the watermark offset is the same for all 
sources and is set at the application level.

Finally, it should be noted that Spark Structured Steaming engine processes streaming sources in micro-batches. Thus,
records are collected for some short-termed interval and processed as a static dataframe (micro-batch). Spark allows
us to control time interval for which micro-batches are collected by setting `trigger` interval. 
This interval must also be the same for all streams and is set at application level. Adjusting trigger interval 
allows us to control size of micro-batches and thus to control executors load.

Thus, for more information on streaming configuration settings, please see 
[Streaming Settings](../01-application-setup/01-ApplicationSettings.md#streaming-settings) chapter. 
Summarizing, data quality streaming job processing routing consists of following stages:

* Start streaming queries from provided sources with `forEachBatch` sink.
* Start window processor in a separate thread.
* For each micro-batch (evaluated once per trigger interval) process data:
    * register metric error accumulator;
    * for each record increment metric calculators corresponding to the window to which record is assigned;
    * collect metric errors if any;
    * if record is late to current watermark, then it is skipped and metric calculators state is unchanged;
    * compute new watermark based on time values obtained from processed records;
    * update processor buffer state, which contains state of metric calculators for all windows as well as collected
      metric errors (also per each window). In addition, processor buffer tracks current watermark levels per each
      processed streaming source.
* Window processor checks processor buffer (also once per trigger interval) for windows that are completely below the
  watermark level. **IMPORTANT** In order to support synchronised processing of multiple streaming sources, the minimum
  watermark level is used (computed from current watermark levels of all the processed sources). This ensures that
  window is finalised for all processed sources.
* Once finalised window is obtained, then for this window all data quality routines are performed:
    * metric results are retrieved from calculators;
    * composed metrics are calculated;
    * checks are performed;
    * results are stored in the data quality storage;
    * all targets are processed and results (or notifications) are sent to required channels.
    * checkpoints are saved if checkpoint directory is configured. **This is new feature available since Checkita 2.0.**
    * processor buffer is cleared: state for processed window is removed. 
* Streaming queries and window processor run until application is stopped (`sigterm` signal received) or error occurs.

**Important note on results saving**: since set of results is generated per each processed window than for each set of 
results reference datetime and execution datetime is set to a corresponding window start time. For more details on
working with datetime in Checkita framework, please see [Working with Date and Time](01-WorkingWithDateTime.md) chapter.

> **TIP** Since data quality checks are performed for each window, then windows size should rather be large, in order 
> to produce results at such time interval which allows reviewing any occurred data quality issued and take some
> measures to resolve them. Thus, if your engineering team has a "reaction time" of 1 hour then it is quite 
> unreasonable to perform quality checks over streaming source with 10-minutes window. 

## Checkpointing

Since Checkita 2.0 release, streaming applications support checkpointing.
In order to enable that mechanism, it is just required to set up checkpoint directory location,
as described in [Streaming Settings](../01-application-setup/01-ApplicationSettings.md#streaming-settings)
of application configuration.

Note, that checkpoints are saved in Parquet format with processor buffer been binary serialized in advance.
Thus, default spark writer is used to store Parquet files and, therefore, checkpoints can be saved to any filesystem,
that is set up and supported during Spark runtime, e.g. HDFS, S3 or local file system.

Checkpoints are saved once streaming window is finalized and processed: all metric and checks are computed,
results are saved into DQ Storage and send as a targets. After that, we can ensure that window has been processed 
successfully and, therefore, update checkpoints in order this window won't be processed again later if application restarted.

Later, if application is failed and restarted then, latest checkpoint is read from checkpoint directory and
all required topics are read from the starting offsets written within checkpoint.

> **IMPORTANT**: Currently, checkpointing is only supported for Kafka streaming sources.
