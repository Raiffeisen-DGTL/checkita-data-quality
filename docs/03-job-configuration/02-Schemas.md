# Schemas Configuration

Schemas are used in Data Quality jobs for two purposes:

* Provide explicit schema when reading sources without embedded schemas such as delimited or fixed width text files.
* Provide reference schema to validate actual source schema. Schemas are used in `schemaMatch` load checks.
  See [Schema Match Check](07-LoadChecks.md#schema-match-check).

Schemas are set in `schemas` section of job configuration and can be defined in different formats as described below.
Format in which schema is defined is set in `kind` field and defines what other fields are need to be provided.

Apart from `kind` field, all types of schemas configuration contain following common parameters:

* `id` - Schema ID that uniquely identifies its configuration;
* `description` - Optional schema description;
* `metadata` - Optional list of arbitrary user-defined metadata parameters.

## Delimited Schema Configuration

This kind of schema definition is primarily used to provide schemas for delimited text files such as CSV or TSV.
Nevertheless, these schemas can be used for `schemaMatch` load checks as well. Using this type of configuration, 
only flat schemas can be defined (nested columns are not allowed).

Thus, delimited definition contains following parameters:

* `kind: "delimited"` - *Required*. Sets delimited schema definition format.
* `id` - *Required*. Schema ID;
* `description` - *Optional*. Schema description;
* `schema` - *Required*. List of schema columns where each column is an object with following fields:
    * `name` - *Required*. Name of the column;
    * `type` - *Required*. Type of the column. See [Supported Type Literals](#supported-type-literals) for allowed types.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this schema where each parameter
  is a string in format:`param.name=param.value`.

## Fixed-Full Schema Configuration

Fixed-full kind of schema definition is used to provide schemas for read fixed-width text files. The key difference from
other schema definitions is that columns widths are also provided which is crucial information for parsing fixed-width
files. This kind of schema may also be used for reading delimited files and for reference in `schemaMatch` load checks.
Using this type of configuration, only flat schemas can be defined (nested columns are not allowed).

Fixed-fill schema definition contains following parameters:

* `kind: "fixedFull"` - *Required*. Sets fixed-full schema definition format.
* `id` - *Required*. Schema ID;
* `description` - *Optional*. Schema description;
* `schema` - *Required*. List of schema columns where each column is an object with following fields:
    * `name` - *Required*. Name of the column;
    * `type` - *Required*. Type of the column. See [Supported Type Literals](#supported-type-literals) for allowed types.
    * `width` - *Required*. Integer width of column (number of symbols).
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this schema where each parameter
  is a string in format:`param.name=param.value`.

## Fixed-Short Schema Configuration

Fixed-short kind of schema definition provides a more compact syntax for defining schemas used for reading fixed-width
files. The columns are defined by their name and width only. *Subsequently, all columns will have StringType.*
This kind of schema may also be used for reading delimited files and for reference in `schemaMatch` load checks.
Using this type of configuration, only flat schemas can be defined (nested columns are not allowed).

Fixed-short schema definition contains following parameters:

* `kind: "fixedShort"` - *Required*. Sets fixed-short schema definition format.
* `id` - *Required*. Schema ID;
* `description` - *Optional*. Schema description;
* `schema` - *Required*. List of schema columns where each column is a string in format `columnName:columnWidth`.
    *Type of columns is always a StringType.*
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this schema where each parameter
  is a string in format:`param.name=param.value`.

## Avro Schema Configuration

Avro kind of schema configuration is used to read schema from file with avro schema `.avsc`. Thus, schema read from 
avro schema file can be used to read both, avro files and delimited text files as well as be used as reference 
in `schemaMatch` load checks. In addition, avro schema format supports complex schemas with nested columns.

In order to read schema from avro file it is required to supply following parameters:

* `kind: "avro"` - *Required*. Sets avro schema definition format.
* `id` - *Required*. Schema ID;
* `description` - *Optional*. Schema description;
* `schema` - *Required*. Path to avro schema file `.avsc` to read schema from.
* `validateDefaults` - *Optional, default is `false`*. Boolean flag enabling or disabling default values
  validation in Avro schema.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this schema where each parameter
  is a string in format:`param.name=param.value`.

## Hive Schema Configuration

Hive catalogue can be used as a source of schemas. Hive kind of schema definition is intended to retrieve schemas from
hive tables. These schemas can be used to read both, avro files and delimited text files as well as be used as reference
in `schemaMatch` load checks.

To retrieve schema from hive table it is required to set up following parameters:

* `kind: "hive"` - *Required*. Sets hive schema definition format.
* `id` - *Required*. Schema ID;
* `description` - *Optional*. Schema description;
* `schema` - *Required*. Hive schema to search for a table.
* `table` - *Required*. Hive table to retrieve schema from.
* `excludeColumns` - *Optional*. List of column names to exclude from schema. Sometimes it is required, e.g.
  to exclude partition columns from schema.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this schema where each parameter
  is a string in format: `param.name=param.value`.

## Registry Schema Configuration

Confluent Schema Registry can be used as a source of schemas. 
This, `registry` kind of schema definition is intended to read schemas from
confluent registry. These schemas can be used to read both, avro files and delimited text files as well as 
be used as reference in `schemaMatch` load checks.

To retrieve schema from Confluent Schema Registry it is required to set up following parameters:

* `kind: "registry"` - *Required*. Sets hive schema definition format.
* `id` - *Required*. Schema ID;
* `description` - *Optional*. Schema description;
* `baseUrls` - *Required*. List of urls to connect to schema registry.
* `schemaId` - *Optional*. Schema ID to search in schema registry.
* `schemaSubject` - *Optional*. Schema subject to search in schema registry.
* `version` - *Optional*. Schema version to read from schema registry. If empty, then latest available version is read.
* `validateDefaults` - *Optional, default is `false`*. Boolean flag enabling or disabling default values
  validation in Avro schema.
* `properties` - *Optional, default is empty*. List of additional connection properties: 
  sequence of strings in format `key=value`.
* `headers` - *Optional, default is empty*. List of additional HTML headers: 
  sequence of strings in format `key=value`.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this schema where each parameter
  is a string in format: `param.name=param.value`.

## Supported Type Literals

The following type literals are supported when defining schema columns in job configuration file:

* `string`
* `boolean`
* `date`
* `timestamp`
* `integer (32-bit integer)`
* `long (64-bit integer)`
* `short (16-bit integer)`
* `byte (signed integer in a single byte)`
* `double`
* `float`
* `decimal(precision, scale)` _(precision <= 38; scale <= precision)_

## Schemas Configuration Example

As it is shown in the example below, `schemas` section represent a list of schema definitions of various kinds.

```hocon
jobConfig: {
  schemas: [
    {
      id: "schema1"
      kind: "delimited"
      description: "Schema describing content of CSV file"
      schema: [
        {name: "colA", type: "string"},
        {name: "colB", type: "timestamp"},
        {name: "colC", type: "decimal(10, 3)"}
      ]
    }
    {
      id: "schema2"
      kind: "fixedFull",
      schema: [
        {name: "col1", type: "integer", width: 5},
        {name: "col2", type: "double", width: 6},
        {name: "col3", type: "boolean", width: 4}
      ]
    }
    {id: "schema3", kind: "fixedShort", schema: ["colOne:5", "colTwo:7", "colThree:9"]}
    {id: "hive_schema", kind: "hive", schema: "some_schema", table: "some_table"}
    {
      id: "avro_schema", 
      kind: "avro", 
      schema: "path/to/avro_schema.avsc"
      metadata: [
        "schema.origin=http://some-schema-registry-location"
      ]
    }
  ]
}
```