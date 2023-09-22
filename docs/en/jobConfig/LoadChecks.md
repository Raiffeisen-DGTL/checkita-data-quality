# Load Checks

Load Checks are distinguished into a separate group of checks. These checks work on top of the source metadata
and do not require reading its contents. This type of checks is also independent of any metrics.

Load Checks are divided into two main types: **Pre** and **Post** load.
Accordingly, some checks are performed before the source is loaded, and others after.

The following types of Load Checks are currently supported:

* **Pre**
    * `exist` - check that the source is available at the specified path
    * `encoding` - check that the source can be loaded with the specified encoding
    * `fileType` - check that the source can be loaded as the specified file type
* **Post**
    * `exactColumnNum` - check that the number of columns matches the given one.
    * `minColumnNum` - check that the number of columns is not less than the specified one.

> **Important!** `Pre` load checks works only with hdfs-file sources: `hdfs`.

To describe Load Check in the configuration file, you must specify the following parameters:

* `id` - check id
* `source` - id of the source to be checked
* `option` - condition to check. The type and possible values depend on the type of check:
    * `exist` - file existence flag: true/false
      (false if, for some reason, we want to make sure that the source does not exist at the given path)
    * `encoding` - file encoding name. The following encodings are supported:
        * US-ASCII, ISO-8859-1, UTF-8, UTF-16BE, UTF-16LE, UTF-16
    * `fileType` - one of the supported file types (see [Sources](Sources.md))
    * `exactColumnNum` - exact number of columns
    * `minColumnNum` - the minimum allowed number of columns.

An example of a **loadChecks** section in a configuration file is shown below:

```hocon
loadChecks: {
   exists: [
     {id: "loadCheck1", source: "fixed_file1", option: true}
   ]
   encoding: [
     {id: "loadCheck2", source: "csv_file1", option: "UTF-8"}
   ]
   fileType: [
     {id: "loadCheck3", source: "avro_file1", option: "avro"},
     {id: "loadCheck4", source: "csv_file2", option: "delimited"}
   ]
   exactColumnNum: [
     {id: "loadCheck5", source: "csv_file2", option: 3}
   ]
   minColumnNum: [
     {id: "loadCheck6", source: "fixed_file2", option: 2}
   ]
}
```