# Sources Configuration

Reading sources is one of the major part of Data Quality job. During job execution, Checkita will read all sources into 
a Spark DataFrames, that will be later processed to calculate metrics and perform quality checks. In addition, 
dataframes' metadata is used to perform all types of load checks in order to ensure that source has the structure 
as expected.

Generally, sources can be read from file systems or object storage that Spark is connected to such as HDFS or S3.
In additional, table-like source from Hive catalogue can be read. Apart from integrations natively supported by Spark,
Checkita can read sources from external systems such as RDBMS or Kafka. For this purpose it is required to define
connections to these systems in a first place. See [Connections Configuration](01-Connections.md) chapter for more 
details on connections configurations.

Thus, currently Checkita supports four general types of sources:

* File sources: read files from local or remote file systems (HDFS, S3, etc.);
* Hive sources: read hive table from Hive catalogue;
* Table sources: read tables from RDBMS via JDBC connection.
* Kafka sources: read topics from Kafka.

All sources must be defined in `sources` section of job configuration. 
More details on how to configure sources of each of these types are shown below. Example of `sources` section of 
job configuration is shown in [Sources Configuration Example](#sources-configuration-example) below.

## File Sources Configuration

Currently, there are five file types that Checkita can read as a source. These are:

* Fixed-width text files;
* Delimited text files (CSV, TSV);
* ORC files;
* Parquet files;
* Avro files.

When configuring file source, it is mandatory to indicate its type. Subsequently, configuration parameters may 
vary for files of different types.

Common parameters for sources of any file type are:

* `id` - *Required*. Source ID;
* `kind` - *Required*. File type. Can be one of the following: `fixed`, `delimited`, `orc`, `parquet`, `avro`;
* `path` - *Required*. File path. Can be a path to a directory or a S3-bucket. In this case all files from this
  directory/bucket will be read (assuming they all have the same schema). Note, that when reading from file system which
  is not spark default file system, it is required to add FS prefix to the path, e.g. `file://` to read from local FS, 
  or `s3a://` to read from S3.
* `keyFields` - *Optional*. List of columns that form a Primary Key or are used to identify row within a dataset.
  Key fields are primarily used in error collection reports. For more details on error collection, see 
  [Metric Error Collection](../02-general-concepts/04-ErrorCollection.md) chapter.

### Fixed Width File Sources

In order to read fixed-width file it is additionally required to provide ID of the schema used to parse file content.
Schema itself should be defined in `schemas` section of job configuration as described in 
[Schemas Configuration](02-Schemas.md) chapter. 

* `schema` - *Required*. Schema ID used to parse fixed-width file. 
  The schema definition type should be either `fixedFull` or `fixedShort`

### Delimited File Sources

When reading delimited text file, its schema may be inferred from file header if it is presented in the file or 
may be explicitly defined in `schemas` section of job configuration file  as described in
[Schemas Configuration](02-Schemas.md) chapter.

Thus, additional parameters for configuring delimited file source are:

* `schema` - *Optional*. Schema ID used to parse delimited file text file. It is possible to use schema of any
  definition type as long as it has flat structure (nested columns are not supported for delimited text files).
* `header` - *Optional, default is `false`*. Boolean parameter indicating whether schema should be inferred 
  from file header.
* `delimiter` - *Optional, default is `,`*. Column delimiter.
* `quote` - *Optional, default is `"`*. Column enclosing character.
* `escape` - *Optional, default is ``\``*. Escape character.

> **IMPORTANT**: If the `header` parameter is absent or set to`false`, then `schema` parameter must be set.
> And vice versa, if `header` parameter is set to `true`, then `schema` parameter must not be set.
> In other words, schema may be inferred from file header or be explicitly defined, but not both.

### Avro File Sources

Avro files can contain schema in its header. Therefore, there are two options to read avro files: either infer schema
from file or provide it explicitly. In the second case, schema must be defined in `schemas` section of job 
configuration file  as described in [Schemas Configuration](02-Schemas.md) chapter. Therefore, there is only one 
additional parameter for avro file source configuration:

* `schema` - *Optional*. Schema ID used to read avro file. It is possible to use schema of any
  definition type.

### ORC File Sources

As ORC format contains schema within itself, then there are no additional parameters required to read ORC files.

### Parquet File Sources

As Parquet format contains schema within itself, then there are no additional parameters required to read Parquet files.

## Hive Sources Configuration

In order to read data from Hive table it is required to provide following:

* `id` - *Required*. Source ID;
* `schema` - *Required*. Hive schema.
* `table` - *Required*. Hive table.
* `partitions` - *Optional*. List of partitions to read where each element is an object with following fields.
  If partitions are not set then entire table is read.
    * `name` - *Required*. Partition column name
    * `values` - *Required*. List of partition column name values to read.
* `keyFields` - *Optional*. List of columns that form a Primary Key or are used to identify row within a dataset.
  Key fields are primarily used in error collection reports. For more details on error collection, see
  [Metric Error Collection](../02-general-concepts/04-ErrorCollection.md) chapter.

## Table Sources Configuration

Table source are read from supported RDBMS via JDBC connection. There are two options to read data from RDBMS:

* read entire table content;
* execute query on the RDBMS side and read only query result.

In order to set up table source, it is required to
supply following parameters:

* `id` - *Required*. Source ID;
* `connection` - *Required*. Connection ID to use for table source. Connection ID must refer to connection configuration
  for one of the supported RDBMS. See [Connections Configuration](01-Connections.md) chapter for more information.
* `table` - *Optional*. Table to read.
* `query` - *Optional*. Query to execute. Query result is read as table source.
* `keyFields` - *Optional*. List of columns that form a Primary Key or are used to identify row within a dataset.
  Key fields are primarily used in error collection reports. For more details on error collection, see
  [Metric Error Collection](../02-general-concepts/04-ErrorCollection.md) chapter.

> **IMPORTANT**: Either `table` to read from must be specified or `query` to execute, but not both.
> In addition, using queries is only allowed when `allowSqlQueries` is set to true. Otherwise, any usage of arbitrary
> SQL queries will not be permitted. See [Enablers](../01-application-setup/01-ApplicationSettings.md#enablers) chapter
> for more information.

> **TIP**: HOCON format supports multiline string values. In order to define such a value, it is required to enclose
> string in triple quotes, e.g.:
> ```hocon
> multilineString: """
>   SELECT * from schema.table
>   WHERE load_date = '2023-08-23';
> """
> ```

## Kafka Sources Configuration

Despite, it is not common situation to read messages from Kafka topics in batch-mode, such feature is presented in 
Checkita framework. In order to set up source that reads from Kafka topic/s, it is required to provide following
parameters:

* `id` - *Required*. Source ID;
* `connection` - *Required*. Connection ID to use for kafka source. Connection ID must refer to Kafka connection 
  configuration. See [Connections Configuration](01-Connections.md) chapter for more information.
* `topics` - *Optional*. List of topics to read. Topics can be specified in either of two formats:
    * List of topics without indication of partitions to read (read all topic partitions): `["topic1", "topic2"]`;
    * List of topics with indication of partitions to read: `["topic1@[0, 1]", "topic2@[2, 4]"]`
    * *All topics must be defined using the same format.*
* `topicPattern` - *Optional*. Topic pattern name: read all topics that match pattern.
* `startingOffsets` - *Optional, default is `earliest`*. Json string setting starting offsets to read from topic.
  By default, all topic is read.
* `endingOffsets` - *Optional, default is `latest`*. Json string setting ending offset until which to read from topic.
  By default, read topic till the end.
* `keyFormat` - *Optional, default is `string`*. Format used to decode message key.
* `valueFormat` - *Optional, default is `string`*. Format used to decode message value.
* `keySchema` - Schema ID used to parse message key. If key format other than `string` then schema must be provided.
* `valueSchema` - Schema ID used to parse message value. If value format other than `string` then schema must be provided.
* `options` - *Optional*. Additional Spark parameters related to reading messages from Kafka topics such as:
  `failOnDataLoss, kafkaConsumer.pollTimeoutMs, fetchOffset.numRetries, fetchOffset.retryIntervalMs, maxOffsetsPerTrigger`.
  Parameters are provided as a strings in format of `parameterName=parameterValue`.
  For more information, see [Spark Kafka Integration Guide](https://spark.apache.org/docs/2.3.2/structured-streaming-kafka-integration.html).
* `keyFields` - *Optional*. List of columns that form a Primary Key or are used to identify row within a dataset.
  Key fields are primarily used in error collection reports. For more details on error collection, see
  [Metric Error Collection](../02-general-concepts/04-ErrorCollection.md) chapter.

Currently, `string`, `xml` and `json` formats are supported to decode message key and value.

> *TIP*: In order to define JSON strings, they must be enclosed in triple quotes:
> `"""{"name1": {"name2": "value2", "name3": "value3""}}"""`.

## Custom Sources Configuration

Custom sources can be used in cases when it is required to read data from the source type that is not explicitly
supported (by one of the configuration described above). In order to configure a custom source, it is required to
provide following parameters:

* `id` - *Required*. Source ID;
* `format` - *Required*. Spark DataFrame reader format that is used to read from the given source.
* `path` - *Optional*. Path to read data from (if required).
* `schema` - *Optional*. Explicit schema to be applied to data from the given source (if required).
* `options` - *Optional*. Additional Spark parameters used to read data from the given source.
* `keyFields` - *Optional*. List of columns that form a Primary Key or are used to identify row within a dataset.
  Key fields are primarily used in error collection reports. For more details on error collection, see
  [Metric Error Collection](../02-general-concepts/04-ErrorCollection.md) chapter.

After parameters above are defined then spark DataFrame reader is set up to read data from the source as follows:

```scala
val df = spark.read.format(format).schema(schema).options(options).load(path)
```

If any of the optional parameters is missing than corresponding Spark reader configuration is not set.

## Sources Configuration Example

As it is shown in the example below, sources of the same type are grouped within subsections named after the type
of the source. These subsections should contain a list of source configurations of the corresponding type.

```hocon
  sources: {
    file: [
      {id: "hdfs_fixed_file", kind: "fixed", path: "path/to/fixed/file.txt", schema: "schema2"}
      {
        id: "hdfs_delimited_source",
        kind: "delimited",
        path: "path/to/csv/file.csv"
        schema: "schema1"
      }
      {id: "hdfs_avro_source", kind: "avro", path: "path/to/avro/file.avro", schema: "avro_schema"}
      {id: "hdfs_orc_source", kind: "orc", path: "path/to/orc/file.orc"}
    ]
    hive: [
      {
        id: "hive_source_1", schema: "some_schema", table: "some_table",
        partitions: [{name: "load_date", values: ["2023-06-30", "2023-07-01"]}],
        keyFields: ["id", "name"]
      }
    ]
    table: [
      {id: "table_source_1", connection: "oracle_db1", table: "some_table", keyFields: ["id", "name"]}
      {id: "table_source_2", connection: "sqlite_db", table: "other_table"}
    ]
    kafka: [
      {
        id: "kafka_source_1",
        connection: "kafka_broker",
        topics: ["topic1.pub", "topic2.pub"]
        format: "json"
      }
      {
        id: "kafka_source_2",
        brokerId: "kafka_broker",
        topics: ["topic3.pub@[1,3]"]
        startingOffsets: """{"topic3.pub":{"1":1234,"3":2314}}"""
        options: ["kafkaConsumer.pollTimeoutMs=300000"]
        format: "json"
      }
    ]
  }
```