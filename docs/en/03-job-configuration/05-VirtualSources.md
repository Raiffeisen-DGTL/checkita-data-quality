# Virtual Sources Configuration

Checkita framework supports creation of virtual (temporary) sources base on regular once (defined in `sources` section
of job configuration, as described in [Sources Configuration](03-Sources.md) chapter). Virtual sources are created by
applying transformations to existing sources using Spark SQL API. Subsequently, metrics and checks can also be applied
to virtual sources.

It is also important to note, that virtual sources are created recursively, therefore, once virtual source is created
it can be used to create another one in the same way as regular sources.

The following types of virtual sources are supported:

* `SQL`: enables creation of virtual source from existing once using arbitrary SQL query.
* `Join`: creates virtual source by joining two (and only 2) existing sources.
* `Filter`: creates virtual source from existing one by applying filter expression.
* `Select`: creates virtual source from existing one by applying select expression.
* `Aggregate`: creates virtual source by applying groupBy and aggregate operations to existing one.

All types of virtual sources have common features:

* It is possible to cache virtual sources in memory or on disk. This could be handful when virtual sources is used as
  parent for more than one virtual source. In such cases caching virtual source allows not to calculate it multiple times.
* Virtual source can be saved as a file in one of the supported format. This feature can be used for debugging purposes
  or just to keep data transformations applied during quality checks.

Thus, virtual sources are defined in `virtualSources` section of job configuration and have following common parameters:

* `id` - *Required*. Virtual source ID;
* `description` - *Optional*. Virtual source description;
* `parentSources` - *Required*. List of parent sources to use for creation of virtual sources. There could be a
  limitations imposed in number of parent sources, depending on virtual source type.
* `persist` - *Optional*. One of the allowed Spark StorageLevels used to cache virtual sources. By default, virtual
  sources are not cached. Supported Spark StorageLevels are:
    * `NONE`, `DISK_ONLY`, `DISK_ONLY_2`, `MEMORY_ONLY`, `MEMORY_ONLY_2`, `MEMORY_ONLY_SER`,
      `MEMORY_ONLY_SER_2`, `MEMORY_AND_DISK`, `MEMORY_AND_DISK_2`, `MEMORY_AND_DISK_SER`,
      `MEMORY_AND_DISK_SER_2`, `OFF_HEAP`.
* `save` - *Optional*. File output configuration used to save virtual source. By default, virtual sources are not saved.
  For more information on configuring file outputs, see [File Output Configuration](11-FileOutputs.md) chapter.
* `keyFields` - *Optional*. List of columns that form a Primary Key or are used to identify row within a dataset.
  Key fields are primarily used in error collection reports. For more details on error collection, see
  [Metric Error Collection](../02-general-concepts/04-ErrorCollection.md) chapter.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this virtual source where each parameter
  is a string in format: `param.name=param.value`.

## SQL Virtual Source Configuration

`SQL` type of virtual sources is allowed only when `allowSqlQueries` is set to true. Otherwise, any usage of arbitrary
SQL queries will not be permitted. See [Enablers](../01-application-setup/01-ApplicationSettings.md#enablers) chapter
for more information. At the same time, there is no limitation on number of parent sources used to create 
SQL virtual source.

In order to define SQL virtual source, it is required to provide an SQL query:

* `kind: "sql"` - *Required*. Sets `SQL` virtual source type.
* `query` - *Required*. SQL query to build virtual source. _Existing sources are referred in SQL query by their IDs._

## Join Virtual Source Configuration

In order to define `Join` type of virtual sources, it is required to provided two _(and only two)_ parent sources 
that are being joined as well as type of the join to use and list of column to join by. Note, that in order to 
perform join, parent sources should have matching column names to join by. Join by condition is not currently supported:

* `kind: "join"` - *Required*. Sets `Join` virtual source type.
* `joinBy` - *Required*. List of columns to join by. Thus, parent sources must have the same columns names used for join.
* `joinType` - *Required*. Type of Spark join to apply. Following join types are supported:
    * `inner`, `outer`, `cross`, `full`, `right`, `left`, `semi`, `anti`, 
      `fullOuter`,  `rightOuter`, `leftOuter`, `leftSemi`, `leftAnti`
  
## Filter Virtual Source Configuration

`Filter` virtual source is defined by applying sequence of filter expressions to parent source. Thus, only one parent
source must be supplied to this type of virtual source configuration:

* `kind: "filter"` - *Required*. Sets `Filter` virtual source type.
* `expr` - *Required*. Sequence of filter SQL expressions applied to parent source.

## Select Virtual Source Configuration

`Select` virtual source is defined by applying sequence of select expression to parent source. Each select expression 
should yield a new column. Thus, the number of columns in the virtual source correspond to number of provided select
expressions. Subsequently, only one parent source must be supplied to this type of virtual source configuration:

* `kind: "select"` - *Required*. Sets `Select` virtual source type.
* `expr` - *Required*. Sequence of select SQL expressions applied to parent source.

## Aggregate Virtual Source Configuration

`Aggregate` virtual source is defined by applying groupBy and aggregate operations to parent source. Thus, it is 
required to provide a list of columns used to group rows as well as list of aggregate operations in form of SQL 
expressions used to create columns with aggregated results. Thus, the number of columns in the virtual source 
correspond to number of provided aggregate expressions. Subsequently, only one parent source must be supplied 
to this type of virtual source configuration:

* `kind: "aggregate"` - *Required*. Sets `Aggregate` virtual source type.
* `groupBy` - *Required*. Sequence of columns used to group rows from parent source.
* `expr` - *Required*. Sequence of SQL expressions used to get columns with aggregated results. 

## Virtual Sources Configuration Example

As it is shown in the example below, `virtualSources` section represent a list of virtual source definitions
of various kinds.

```hocon
jobConfig: {
  virtualSources: [
    {
      id: "sqlVS"
      kind: "sql"
      description: "Filter data for specific date only"
      parentSources: ["hive_source_1"]
      persist: "disk_only"
      save: {
        kind: "orc"
        path: "some/path/to/vs/location"
      }
      query: "select id, name, entity, description from hive_source_1 where load_date == '2023-06-30'"
      metadata: [
        "source.owner=some.preson@some.domain"
        "critical.source=false"
      ]
    }
    {
      id: "joinVS"
      kind: "join"
      parentSources: ["hdfs_avro_source", "hdfs_orc_source"]
      joinBy: ["id"]
      joinType: "leftouter"
      persist: "memory_only"
      keyFields: ["id", "order_id"]
    }
    {
      id: "filterVS"
      kind: "filter"
      parentSources: ["kafka_source"]
      expr: ["key is not null"]
      keyFields: ["orderId", "dttm"]
    }
    {
      id: "selectVS"
      kind: "select"
      parentSources: ["table_source_1"]
      expr: [
        "count(id) as id_cnt",
        "sum(amount) as total_amount"
      ]
    }
    {
      id: "aggVS"
      kind: "aggregate"
      parentSources: ["hdfs_fixed_file"]
      groupBy: ["col1"]
      expr: [
        "avg(col2) as avg_col2",
        "sum(col3) as sum_col3"
      ],
      keyFields: ["col1", "avg_col2", "sum_col3"]
    }
  ]
}
```