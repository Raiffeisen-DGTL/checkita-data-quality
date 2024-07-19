# Status Model used in Results

Unified status model is used for results that Checkita framework produces. Thus, all metrics and check results have
common status indication that is following:

* `Success` - Evaluation of metric or check completed without any errors and metric or check condition is met.
* `Failure` - Evaluation of metric or check yielded results that do not meet configured condition, e.g.:
    * Regex match metric got a column value that do not match to required regex pattern;
    * Check requiring that some metric result should be equal to zero got a non-zero metric result.
* `Error` - Caught runtime error during metric or check evaluation. Runtime error message is caught as well.

Result status is always accompanied by message, that describes this status. What not common between metrics and checks
is how statuses are communicated with user:

* When computing metrics, status is obtained for each data row during metric increment step. If status other than 
  `Success` then metric error is collected for this particular row of data. Then, metric error reports can be requested
  as [Error Collection Targets](../03-job-configuration/10-Targets.md#error-collection-targets). For more information 
  on metric error collection, see [Metric Error Collection](04-ErrorCollection.md) chapter.
* As for checks, status is their primary result output. Therefore, it is written into data quality storage along with
  a detailed message.