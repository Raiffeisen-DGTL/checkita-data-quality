# Job Configuration

Data Quality job in Checkita is a sequence of tasks that need to be performed in order to check quality of data.
These tasks may include following:

* establishing connections to external systems;
* reading user-defined schemas (these schemas may later be used during source reading or as reference to 
  validate actual source schemas);
* reading sources from various file systems and external systems (using already established connections);
* creating virtual sources by applying SQL transformations to already existing sources;
* performing load checks to validate sources metadata;
* performing regular and composed metrics calculation;
* performing checks based on computed metric results;
* sending targets to various channels (specially developed mechanism to allow results communication other than 
  saving them to results storage)

All the aforementioned tasks are configured in one or multiple [Hocon](https://github.com/lightbend/config/blob/main/HOCON.md)
configuration files. All job configurations are set within `jobConfig` section of the configuration files.

There is only one parameter that is set at the top level and this is `jobId` - ID of the job to be run.
This parameter is mandatory for any job configuration. Thus, `jobId` usually unites calculation of various metrics and 
checks that are performed over the sources within single schema, data-mart or other logical formation of data sources.

The rest of the parameters are defined in the subsections that are described in a separate 
chapters of this documentation:

* [Connections](01-Connections.md) - describes configuration of connections to various external systems.
* [Schemas](02-Schemas.md) - describes configuration of user-defined schemas.
* [Sources](03-Sources.md) - describes configuration of sources to read data from.
* [Virtual Sources](04-VirtualSources.md) - describes configuration of virtual sources.
* [Load Checks](05-LoadChecks.md) - describes configuration of load checks.
* [Metrics](06-Metrics.md) - describes configuration of metrics.
* [Checks](07-Checks.md) - describes configuration of checks.
* [Targets](08-Targets.md) - describes configuration of targets.

Example of fully filled job configuration can be found in [Job Configuration Example](10-JobConfigExample.md) 
chapter of this documentation.