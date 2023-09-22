# Sources

Sources - section of the configuration file dedicated to the description of data sources.
This is the first step towards describing the calculation pipeline.

The following source types are currently supported:

* **table**: Tables from relational databases. The table will be downloaded entirety,
  but it is possible to change it by creating a virtual source using
  SQL query (see [Virtual Sources](VirtualSources.md)). To load the table, you must specify
  database connection parameters in the [Databases](Databases.md) section.
* **hive**: Hive table loaded via SQL query using Spark SQL API.
* **kafka**: Sources generated from messages from Kafka topics.
* **hdfs**: Files loaded from HDFS. The following file types are supported:
    * ***fixed***: text files without delimiter with a fixed number of characters per column;
    * ***delimited***: delimited text files;
    * ***avro***: .avro files with external schema support in .avsc files;
    * ***parquet***: .parquet files;
    * ***orc***: .orc files.
    * ***delta***: reading delta-tables files from Databricks
* **custom**: Defines custom source using user-defined options.
  Given source type should be used to load data from the sources that are 
  not supported explicitly in one of the source types, described above.

The **sources** section in the configuration file looks like this:

```hocon
sources: {
   table: [ // list of sources based on database tables
   ]
   hive: [ // list of sources based on Hive tables
   ]
   kafka: [ // list of sources based on Kafka topics
   ]
   hdfs: { // hdfs sources grouped by sub-types
     fixed: [ // list of text files without a delimiter with a fixed number of characters per column
     ]
     delimited: [ // list of delimited text files
     ]
     avro: [ // list of .avro files with optional .avsc schema
     ]
     parquet: [ // list of .parquet files
     ]
     orc: [ // list of .orc files
     ]
   }
}
```

##Table

To read data from a database table, you need to specify the following parameters (***all of them are required***):

* `id` - source identifier
* `database` - database identifier from the [databases](Databases.md) section.
* `table [optional]` - table name (the schema is specified in the database connection parameters, if necessary).
* `query [optional]` - query to execute. Query result will be loaded as data source.
  **Query will be executed on the database side.**
* `keyFields [optional]` is a list of key columns that identify the string in the source.
  Used to report errors when calculating metrics.

> Important: For proper source definition it is required to set either `table` (read entire table) 
> or `query` (read query result only) parameter. It is prohibited to use both parameters simultaneously.
> 
> Also, it should be noted that HOCON format supports multi-line string values which are useful to write a queries.
> In order to provide multi-line string value, one must be enclosed in triple quotes.

Example:

```hocon
table:[
   {id: "table1", database: "postgre_db1", table: "db_table1", keyFields: ["id"]}
]
```

##hive

To read Hive tables, you must specify the source ID and HQL query (required parameters).

* `id` - source identifier
* `query` - HQL query
* `keyFields [optional]` is a list of key columns that identify the string in the source.
  Used to report errors when calculating metrics.

Example:

```hocon
hive:[
   {id: "hive_table1", query: "select * from schema.table1", keyFields: ["cnum", "deal_id"]},
   {id: "hive_table2", query: "select * from schema.table2"}
]
```

## Kafka

To read data from Kafka topics, you must specify the following parameters:

* `id` - source identifier
* `brokerId` - ID of the Kafka broker described in the [messageBrokers](MessageBrokers.md) section.
* `topics [optional]` - list of topics to read. Topics can be specified in two formats:
    * Topics without partitions (read from all partitions): `["topic1", "topic2"]`
    * Topics with partitions to read: `["topic1@[0, 1]", "topic2@[2, 4]"]`
    * **All topics must be in one of these formats.**
* `topicPattern [optional]`
  > **Important:** Reading topics must be in one of the above formats:
  > either in `topics` or `topicPattern`.
* `startingOffsets [optional]` - Json string indicating starting offsets for reading.
  Default: `earliest` (read the whole topic)
* `endingOffsets [optional]` - Json string indicating end offsets for reading.
  Default: `latest` (read the topic to the end)
  > **Format for specifying offsets:** Json string of the following format (set in triple quotes):
  >
  >`"""{"topic1":{"0":1234,"1":2345},"topic2":{"0":3456,"1":4567}}"""`
* `format` - message format in the topic.
  > Currently, only two formats are supported: `xml` and `json`.
* `options [optional]` - additional reading options from the following list
  (See [Spark Kafka Integration Guide](https://spark.apache.org/docs/2.3.2/structured-streaming-kafka-integration.html)):
    * `failOnDataLoss, kafkaConsumer.pollTimeoutMs, fetchOffset.numRetries, fetchOffset.retryIntervalMs, maxOffsetsPerTrigger`
* `keyFields [optional]` is a list of key columns that identify the string in the source.
  Used to report errors when calculating metrics.

Example:
```hocon
   kafka:[
   {
     id: "topic1",
     brokerId: "kafka1",
     topics: ["some.topic"]
     startingOffsets: """{"some.topic":{"0":35590000,"1":1234,"2":1432}}"""
     options: ["kafkaConsumer.pollTimeoutMs=300000"]
     format: "json"
   }
]
```

## HDFS

To read data from HDFS, regardless of the file type, the source identifier must be specified
and the path to the file/folder with files:

* `id` - source identifier;
* `path` - path to source in HDFS.
* `keyFields [optional]` is a list of key columns that identify the string in the source.
  Used to report errors when calculating metrics.

### Fixed

In order to read text files without a delimiter, the data scheme must be specified in one of the following formats:

* `schema` - a list of columns described by a dictionary with the following keys:
  * `name` - column name;
  * `type` - column type;
  * `length` - column width (number of characters);
* `shortSchema` - list of columns in "name:size" format. All columns will be of text data type.

Example:

```hocon
fixed: [
   {
     id: "fiexed_file1",
     path: "/data/fixed_file1.txt",
     schema: [{name: "a", type: "string", length: 6}, {name: "b", type: "integer", length: 5}]
   },
   {id: "fixed_file2", path: "/data/fixed_file2.txt", shortSchema: ["id:6", "name:12", "value:8"], keyFields: ["id"]}
]
```

### Delimited

In order to read text files with a delimiter, you must additionally specify where
the data schema is read (from the file header or specified in the configuration file):

* `header [optional]` - read (`true`) or not (`false`) column names from file header. Default: `false`
* `schema` - list of columns described by dictionary with `name` (column name) and `type` (column type) keys
* `keyFields [optional]` is a list of key columns that identify the string in the source.
  Used to report errors when calculating metrics.

If the `header` parameter is absent or `false`, then the presence of a schema (`schema`) is required.
If `header = true` then the schema must be absent.

Additional parameters (all optional):

* `delimiter` - delimiter (default: `,`)
* `quote` - enclosing quotes (default `"`)
* `escape` - escape character (default ``\``)

Example:

```hocon
delimited: [
   {id: "csv_file1", path: "/data/delimited_file1.csv", header: true}
   {
     id: "csv_file2",
     path: "/data/delimited_file2.csv",
     delimiter: ":",
     quote: "'",
     escape: "|",
     header: false
     schema: [{name: "a", type: "string"}, {name: "b", type: "integer"}, {name: "c", type: "string"}],
     keyFields: ["a", "b"]
   }
]
```

> **Supported data types in text file schemas:**
>
> * string
> * boolean
> * date
> * timestamp
> * integer (32-bit integer)
> * long (64-bit integer)
> * short (16-bit integer)
> * byte (signed integer in a single byte)
> * double
> * float
> * decimal(precision, scale) _(precision <= 38; scale <= precision)_

### Avro

Required parameters are only `id` and `path`. Additionally, you can specify the path to the file
with a data schema (.avsc) using the `schema [optional]` parameter. You can also specify a list of
key-columns: `keyFields [optional]` which identify the string in the source and will
be used to report errors in metric calculation.

Example:

```hocon
avro: [
   {id: "avro_file1", path: "/data/some_file.avro", schema: "/data/some_schema.avsc", keyFields: ["id", "name"]}
]
```

### Parquet

Additionally, you can specify a list of key columns: `keyFields [optional]`, which identify the string in the source
and will be used to report errors in the calculation of metrics.

Example:

```hocon
parquet:[
   {id: "parquet_file", path: "/data/some_file.parquet", keyFields: ["id", "name"]}
]
```

###Orc

Additionally, you can specify a list of key columns: `keyFields [optional]`, which identify the string in the source
and will be used to report errors in the calculation of metrics.

Example:

```hocon
orc:[
   {id: "orc_data", path: "/data/some_folder_with_orc_files/", keyFields: ["id", "name"]}
]
```

### Delta

Additionally, you can specify a list of key columns: `keyFields [optional]`, which identify the string in the source
and will be used to report errors in the calculation of metrics.

Example:

```hocon
delta:[
   {id: "delta_file", path: "/data/some_folder_with_orc_files/", keyFields: ["id", "name"]}
]
```

## Custom

Enables data loading from the sources that are not supported explicitly in the above source types.
The following parameters are used in order to define custom source:

* `id` - source identifier;
* `format` - format name, which will be used by Spark to load data.
* `path [optional]` - Path to load data from.
* `options [optional]` - Options that will be used by Spark to load data.
* `keyFields [optional]` - is a list of key columns that identify the string in the source.
  Used to report errors when calculating metrics.

The aforementioned parameters are used to read data by spark in general way by transforming them to a 
spark reader as follows:
```
val df = spark.read.format(format).options(options).load(path)
```

If additional libraries are required to load data from the custom source,
then these libraries must be added to spark job on startup as follows:
```bash
--jars jar1.jar,jar2.jar
```