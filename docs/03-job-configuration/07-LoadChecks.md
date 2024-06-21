# Load Checks Configuration

Load checks are the special type of checks that are distinguished from other checks as they are applied not to results
of metrics computation but to sources metadata. Other key feature of load checks is that they are run prior actual
data loading from the sources what is possible due Spark lazy evaluation mechanisms: sources are, essentially, Spark
dataframes and load checks are used to verify their metadata.

Load checks are defined in `loadChecks` section of job configuration and have following common parameters:

* `id` - *Required*. Load check ID;
* `description` - *Optional*. Load check description;
* `source` - *Required*. Reference to a source ID which metadata is being checked;
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this load check where each parameter
  is a string in format: `param.name=param.value`.

Currently, supported load checks are described below as well as configuration parameters specific to them.

## Minimum Column Number Check

This check is used to verify if number of columns in the source is equal to or greater than specified number.
Load checks of this type are configured in the `minColumnNum` subsection of the `loadChecks` section.
In addition to common parameters, following parameters should be specified:

* `option` - *Required*. Minimum number of columns that checked source must contain.

## Exact Column Number Check

This check is used to verify if number of columns in the source is exactly equal to specified number.
Load checks of this type are configured in the `exactColumnNum` subsection of the `loadChecks` section.
In addition to common parameters, following parameters should be specified:

* `option` - *Required*. Required number of columns that checked source must contain.

## Columns Existence Check

This check is used to verify if source contains columns with required names. 
Load checks of this type are configured in the `columnsExist` subsection of the `loadChecks` section.
In addition to common parameters, following parameters should be specified:

* `columns` - *Required*. List of column names that must exists in checked source.

## Schema Match Check

This check is used to verify if source schema matches predefined reference schema. Reference schema must be defined 
in `schemas` section of configuration files as described in [Schemas Configuration](02-Schemas.md) chapter.
Load checks of this type are configured in the `schemaMatch` subsection of the `loadChecks` section.
In addition to common parameters, following parameters should be specified:

* `schema` - *Required*. Reference Schema ID which should be used for comparison with source schema.
* `ignoreOrder` - *Optional, default is `false`*. Boolean parameter indicating whether columns order should be ignored
  during comparison of the schemas.

## Load Checks Configuration Example

As it is shown in the example below, load checks of the same type are grouped within subsections named after the type
of the load check. These subsections should contain a list of load checks configurations of the corresponding type.

```hocon
jobConfig: {
  loadChecks: {
    minColumnNum: [
      {id: "load_check_1", source: "kafka_source", option: 2}
    ]
    exactColumnNum: [
      {
        id: "load_check_2", 
        description: "Checking that source has exactly required number of columns", 
        source: "hdfs_delimited_source", option: 3
        metadata: [
          "critical.loadcheck=true"
        ]
      }
    ]
    columnsExist: [
      {id: "loadCheck3", source: "sqlVS", columns: ["id", "name", "entity", "description"]},
      {id: "load_check_4", source: "hdfs_delimited_source", columns: ["id", "name", "value"]}
    ]
    schemaMatch: [
      {id: "load_check_5", source: "kafka_source", schema: "hive_schema"}
    ]
  }
}
```