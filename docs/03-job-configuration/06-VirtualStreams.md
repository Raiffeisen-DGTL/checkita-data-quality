# Virtual Streaming Sources Configuration

When running Data Quality checks over the streaming data sources it is required to apply transformations to them thus
creating virtual streaming sources. Such sources have to be defined in `virutalStreams` section of the job 
configuration. Thus, transformations defined in this section are applied only to streaming sources using Spark
Structured Streaming API. More details on running data quality checks over streaming sources are given in
[Data Quality Checks over Streaming Sources](../02-general-information/05-StreamingMode.md) chapter.

The configuration of virtual streaming sources is the same as for the static ones. 
See chapter [Virtual Sources Configuration](05-VirtualSources.md) for more details. In addition, column used as source 
of timestamp for windowing can be redefined and derived from the resultant virtual stream scheme. 
See [Streaming Sources Configurations](04-Streams.md) for more details on how to define column used as source of timestamp.

It is important to note that not all supported virtual sources types can be built from streaming sources. 
Currently, only [filter](05-VirtualSources.md#filter-virtual-source-configuration) and
[select](05-VirtualSources.md#select-virtual-source-configuration) types of virtual sources are supported 
in streaming applications.