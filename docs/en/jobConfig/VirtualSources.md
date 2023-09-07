# Virtual Sources

**Checkita** framework supports creation of temporary (virtual) sources
based on basic data sources (see [Sources](Sources.md)) applying various transformations to them using
 Spark SQL API. Subsequently, metrics and checks can also be applied to virtual sources.

The following types of virtual sources are currently supported:

* `filterSql` - creates a virtual source based on an existing one by applying the specified SQL query to it.
* `joinSql` - creates a virtual source by joining two other sources with a SQL query.
* `join` - creates a virtual source by joining two other sources without SQL query,
  but simply with a list of columns.

For all virtual sources, it is required to specify the following parameters:

* `id` - virtual source id
* `parentSources` - list of parent source id's
  (list from 1st source for `filterSql` and list from 2 sources for `joinSql` or `join`)
* `persist [optional]` - **optional**, virtual source can be cached before the calculation of the metrics.
  To do this, one of the possible Spark StorageLevels must be specified in the `persist` field.
  If the `persist` field is missing, then the virtual source will not be cached.
  > Spark Storage Levels
  >
  > * NONE
  > * DISK_ONLY
  > * DISK_ONLY_2
  > * MEMORY_ONLY
  > * MEMORY_ONLY_2
  > * MEMORY_ONLY_SER
  > * MEMORY_ONLY_SER_2
  > * MEMORY_AND_DISK
  > * MEMORY_AND_DISK_2
  > * MEMORY_AND_DISK_SER
  > * MEMORY_AND_DISK_SER_2
  > * OFF_HEAP.
* `save [optional]` optional section that can be filled in order to save the virtual source:
  > * `path` - path to the directory where the virtual source will be saved;
  > * `fileFormat` - the format in which to save the source: orc, parquet, csv
  > * `date` is the date for which the Targets are written, if different from `executionDate`.
  >
  > For text formats (csv, txt) you can specify the following optional parameters:
  > * `delimiter [optional]` - delimiter (default: `,`)
  > * `quote [optional]` - enclosing quotes (default `"`)
  > * `quoted [optional]` - boolean parameter indicating whether all values in the file should be quoted.
  >   Default: `false`.
  > * `escape [optional]` - escape character (default ` \ `)

* `keyFields [optional]` is a list of key columns that identify the string in the source.
  Used to report errors when calculating metrics.

For **filterSql** and **joinSql**, the query to be executed is additionally specified in the `sql` field.
For **join**, the list of columns to join on is additionally specified in the `joinColumns` field,
as well as the type of the join in the `joinType` field.

> The "join" type must be one of the following list:
>
> * inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti.

An example of a **virtualSources** section in a configuration file is shown below:

```hocon
virtualSources: {
   filterSql: [ // list of virtual sources of type filterSql
     {
       id: "vSource1",
       parentSources: ["table1"],
       sql: "select distinct company_name as name from table1 where company_name like 'C%'"
       persist: "MEMORY_AND_DISK"
     }
   ]
   joinSql: [ // list of virtual sources of type joinSql
     {
       id: "vSource2",
       parentSources: ["table1", "table2"],
       sql: "select * from table1 left join table on client_name=supplier_name",
       save: {
         path: "/path/to/virtual/source/save/directory"
         fileFormat: "orc"
       }
       keyFields: ["id", "client_name"]
     }
   ]
   join: [ // list of virtual sources of type join
     {id: "vSource3", parentSources=["hive_table1, hive_table2"], joinColumns=["ORDER_ID","CURRENCY"], joinType="inner"}
   ]
}
```