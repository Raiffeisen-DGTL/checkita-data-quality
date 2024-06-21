# Streaming Sources Configurations

When running Data Quality checks over the streaming data sources it is required to define them in `streams` section
of job configuration. Thus, sources defined in this section are read as streaming dataframes using 
Spark Structured Streaming API. More details on running data quality checks over streaming sources are given in 
[Data Quality Checks over Streaming Sources](../02-general-information/05-StreamingMode.md) chapter.

The configuration of streaming sources is the same as for the static ones. 
See chapter [Sources Configuration](03-Sources.md) for more details.

It is important to note that not all supported sources can be read in streaming mode. Currently, only sources below
can be read as streams:

* [File sources](03-Sources.md#file-sources-configuration).
* [Kafka sources](03-Sources.md#kafka-sources-configuration).  Note, that there is a difference in default value for parameter `startingOffsets`. When defining streaming kafka
  source, the default value for this parameter is `latest`. Also, for streaming kafka sources parameter 
  `endingOffsets` is ignored (all new records will be processed until application is stopped).

The only additional parameter that is required to be defined for all streaming sources is following:

* `windowBy` - *Optional, default is `processingTime`*. Source of timestamp used to assign records to a particular
  streaming windows and also to skip "late" records. **Applicable only for streaming jobs!** There are following 
  options supported:
    * `processingTime` - Uses current timestamp at the moment when Spark processes record.
    * `eventTime` - Mostly applicable to kafka sources. Uses column with name `timestamp` to retrieve time value from.
      This column must be of *Timestamp* type.
    * `custom(columnName)` - Uses arbitrary user-defined column to retrieve time value from. Specified column
      must be of *Timestamp* type. In addition, an SQL expressions are supported. An expression should also evaluate 
      to value of Timestamp type. For example: `custom(value.createdAt)` - the time value for a record will be retrieved
      from message value's field with name `createdAt`.



