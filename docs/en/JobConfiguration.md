# Job Configuration

**Checkita** uses a configuration file, which is in the format
[Hocon](https://github.com/lightbend/config/blob/main/HOCON.md)
in order to describe data sources, metrics to calculate, and checks to be performed.

Each configuration file must start with a `jobId` that unites metrics calculations over sources within some schema,
datamart or other logical formation of data. All IDs (sources, metrics, checks, etc.) must be unique within the same jobId.

```hocon
jobId: "inc5000_companies"
```

> **Important!** When starting the calculation, a temporary version of the configuration file is generated,
> has following standard variables prepended to the beginning of file:
>
> * `referenceDateTime`: the date for which the calculation is performed (specified in the framework arguments when
>   starting Spark application, defaults to the date when application was started);
>   The format of the string representation of the date is set in `application.conf`
> * `executionDateTime`: The actual date the application was started, whose string representation format is also
>   specified in `application.conf`
>
> These variables can be used later when describing sources, metrics or checks. For example, the path to
> data can be specified like this:

```hocon
path: "/data/inc5000_companies/load_date="${referenceDateTime}
```

The configuration file consists of the following main sections, described in details in the specific chapters
in the documentation:

* [Databases](./jobConfig/Databases.md): this section describes parameters for connecting to external databases.
* [Message Brokers](./jobConfig/MessageBrokers.md): this section describes connection parameters for connection to
  message brokers.
  > **Important:** Only **Kafka** is currently supported.
* [Sources](./jobConfig/Sources.md): this section describes the data sources that will be used to calculate
  metrics and checks.
* [Virtual Sources](./jobConfig/VirtualSources.md): **Checkita** allows you to create virtual
  data sources based on physical ones and perform calculations directly on them. Such sources are described
  in this section.
* [Metrics](./jobConfig/Metrics.md): the main section of the configuration file, which describes the metrics that
  need to be calculated.
* [Checks](./jobConfig/Checks.md): This section describes the checks to be performed after calculating the metrics.
* [Targets](./jobConfig/Targets.md): This section allows you to specify which calculation results should be saved and
* where exactly they need to be saved (in addition to the main database of the framework). Also, in this section
* all types of notifications are configured.

> **Important!** Key names in the configuration file are case-sensitive and are specified in **camelCase**

An example of a fully completed configuration file can be found [here](../examples/jobConfig.conf).