#Checkita Data Quality Framework

**Latest Release: 0.3.0**

---

To ensure quality of big data, it is necessary to perform calculations of a large number of metrics and checks on huge
datasets, which in turn is a difficult task.

**Checkita** is a Data Quality Framework that solves this problem by formalizing and simplifying the process
connecting and reading data from various sources, describing metrics and checks on data from these sources,
as well as sending results and notifications via various channels.

Thus, **Checkita** allows calculating various metrics and checks on data (both structured,
and unstructured). The framework is able to perform distributed computing on data in a "single pass",
using Spark as a computation core. Hocon configurations are used to describe job pipeline. And job results are saved in 
a dedicated framework database, and can also be sent to users via various channels such as HDFS, Email, 
Mattermost and Kafka.

Using Spark as a computation engine allows performing metrics and checks calculations
at the level of "raw" data, without requiring any SQL abstractions over the data (such as Hive or Impala),
which in turn can hide some errors in the data (e.g. bad formatting or schema mismatch).

Summarizing, **Checkita** is able to do following:

* Read data from various sources (HDFS, Hive, Jdbc, Kafka) and in various formats (text, orc, parquet, avro).
* Accept SQL queries on data, thus forming derived "virtual sources" of data.
  This functionality is implemented through the Spark DataFrame API.
* Perform calculation of a wide range of metrics on data, as well as to perform composition of metrics.
* Perform checks on data based on calculated metrics.
* Perform checks based on previous calculation results (anomaly detection in data).
* Save calculation results to the dedicated framework database and also send them via other channels
  (HDFS, Hive, Kafka, Email, Mattermost).

## Prerequisites

The framework is written in Scala 2.12 and uses Spark 2.4+ as the computation core.
The project is configured with a parameterized SBT build that allows building the framework for 
a specific version of Spark, publish the project to a given repository,  and also build Uber-jar, 
both with and without Spark dependencies.