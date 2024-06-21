# File Output Configuration

Checkita framework has mechanism designed to save some it results to a file either in a local or remote (HDFS, S3, etc.)
file system. Thus, it is possible to save [virtual sources](05-VirtualSources.md) that are build during Data Quality
job execution. Saved virtual sources can later be used for various purposes such as for investigating data quality
problems. Apart from that, Checkita supports saving various Data Quality job results as files. In order to do that,
it is required to configure targets of the desired type. See [Targets Configuration](10-Targets.md) for more information.

Thus, Checkita framework support saving file outputs of the following formats:

* Delimited text (CSV or TSV).
* ORC format.
* Parquet format.
* Avro format.

This, in order to configure file output it is required to supply following parameters:

* `kind` - *Required*. File format. Should be one of the following: `delimited`, `orc`, `parquet`, `avro`.
* `path` - *Required*. File path to save. Spark DataFrame writer is used under hood to save outputs. Therefore, path,
  that is provided should point to a directory. If directory non-empty then content is overwritten.

Additional parameters can be defined for delimited text file output. These are:

* `delimiter` - *Optional, default is `,`*. Column delimiter.
* `quote` - *Optional, default is `"`*. Column enclosing character.
* `escape` - *Optional, default is ``\``*. Escape character.
* `header` - *Optional, default is `false`*. Boolean parameter indicating whether file should be written with 
  columns header or without it.

## File Output Configuration Example

* parquet file
  ```hocon
    {
      kind: "parquet"
      path: "/tmp/parquet_file_ooutput"
    }
  ```

* delimited file
  ```hocon
  {
    kind: "delimited"
    path: "/tmp/dataquality/results"
    header: true
  }
  ```