#Checks

Checks are an important step in the work of the **Checkita** framework. Checks are applied to result of metrics
calculation and allow identifying problems with quality of data.

Checks fall into three main groups:

* **Snapshot**: checks based on the results of calculating metrics in the current "job".
* **Trend**: checks based on the analysis of the behavior of the metric over a certain period of time.
* **SQL**: checks with a SQL query applied directly to the source.

> **Tip:** All checks return a boolean value (Success/Failure).

An example of a **checks** section in a configuration file is shown below:

```hocon
checks: {
   trend: {
     averageBoundFullCheck: [
       {
         id: "avg_bal_check",
         description: "Check that average balance stays within +/-25% of the week average"
         metric: "avro_file1_avg_bal",
         rule: "date"
         timeWindow: 8,
         threshold: 0.25
       }
     ]
     averageBoundUpperCheck: [
       {id: "avg_pct_null", metric: "pct_of_null", rule: "date", timeWindow: 15, threshold: 0.5}
     ]
     averageBoundLowerCheck: [
       {id: "avg_distinct", metric: "fiexed_file1_dist_name", rule: "record", timeWindow: 31, threshold: 0.3}
     ]
     averageBoundRangeCheck: [
       {
         id: "avg_inn_match",
         metric: "parquet_file_inn_regex",
         rule: "date",
         timeWindow: 8
         thresholdLower: 0.2
         thresholdUpper: 0.4
       }
     ]
     topNRankCheck: [
       {id: "top2_curr_match", metric: "table1_top3_currency", rule: "record", timeWindow: 2, targetNumber: 2, threshold: 0.1}
     ]
   }
   snapshot: {
     differByLT:[
       {
         id: "row_cnt_diff",
         description: "Number of rows in two tables should not differ on more than 5%.",
         metric: "hive_table1_row_cnt"
         compareMetric: "csv_file1"
         threshold: 0.05
       }
     ]
     equalTo: [
       {id: "zero_nulls", description: "Hive Table1 mustn't contain nulls", metric: "hive_table1_nulls", threshold: 0}
     ]
     greaterThan:[
       {id: "completeness_check", metric: "orc_data_compl", threshold: 0.99}
     ]
     lessThan:[
       {id: "null_threshold", metric: "pct_of_null", threshold: 0.01}
     ]
   }
   sql: {
     countEqZero: [
       {id: "NaN_names", source: "table1", query: "select count(1) from table1 where name = 'NaN'"}
     ]
   }
}
```

## Snapshot Checks

Basic checks between the current metric result and the threshold value,
which can be expressed by a constant or another metric.

The following **Snapshot** checks are supported:

* `differByLT` - checks if the relative difference between ***two metrics*** is less than a threshold value.
* `equalTo` - check if the metric is equal to the given value or another metric.
* `lessThan` - check if the metric is less than the given value or another metric.
* `greaterThan` - check if a metric is greater than a given value or another metric.

Required parameters:

* `id` - check ID.
* `description [optional]` - description of the check. Optional.
* `metric` - metric to check.
* `threshold` - threshold value for comparison.
* `compareMetric` - metric to compare with.

> **IMPORTANT**:
>
> * When specifying a check, one of two things is required: either the threshold value `threshold`
>   or a metric for comparison `compareMetric`.
> * In order to define `differByLT` check, it is required to specify both values: the metric to compare with
>   and the threshold value of relative difference between two metrics. Check succeeds when following expression is true:
>   `| metric - compareMetric | /compareMetric <= threshold`

## Trend Checks

This class of checks allows you to make sure that the value of the metric corresponds to its average value.
for a certain period within a given deviation.

The following **Trend** checks are supported:

* `averageBoundFullCheck`: (1 - threshold) * avgResult <= currentResult <= (1 + threshold) * avg
* `averageBoundUpperCheck`: currentResult <= (1 + threshold) * avgResult
* `averageBoundLowerCheck`: (1 - threshold) * avgResult <= currentResult
* `averageBoundRangeCheck`: (1 - thresholdLower) * avgResult <= currentResult <= (1 + thresholdUpper) * avgResult

Required parameters:

* `id` - check ID.
* `description [optional]` - description of the check. Optional.
* `metric` - metric to check.
* `rule` - the rule for calculating the average value of the metric. Can take one of the following values:
    * `record` - calculates the average value of the metric over the last R records.
    * `date` - calculates the average value of the metric for the last R days.
* `timeWindow` - sets the window size for calculating the average (R records/days).
* `startDate [optional]` - Optionally, an alternative start date can be specified, to determine the temporary
  interval in the format _"yyyy-MM-dd"_. By default, the time interval starts counting back by
  R records/days from `referenceDateTime` (including it).
* `threshold` - allowable deviation from the mean in the interval [0, 1].
* `thresholdLower` - only for `averageBoundRangeCheck`. The lower threshold of deviation from the mean in the interval [0, 1].
* `thresholdUpper` - only for `averageBoundRangeCheck`. The upper threshold of deviation from the mean in the interval [0, 1].

### topNRankCheck

A special check designed specifically for the topN metric and working only with it.
This check calculates the Jacquard distance between the current and previous sets of topN metrics and checks if 
it does not exceed the threshold value.

> **IMPORTANT**: Calculation of this check is currently supported only between the current and previous topN metric sets.

The basic parameters for this check are the same as for the other **Trend** checks, with the following features:

* `timeWindow` - only `timeWindow = 2` is supported.
* `targetNumber` - an additional parameter that specifies the number of records R from the set of topN metrics (R <= N).
* `threshold` - here, this is the Jacquard threshold distance in the interval [0, 1].

## SQL Checks

> **IMPORTANT**: This type of check is only supported for `table` sources.

This type of validation is independent of metrics and works directly with the data source.
The check is based on an SQL query that should return a number. Accordingly, two subtypes of **SQL** checks are defined:

* `countEqZero` - "Success" if the SQL query returns 0.
* `countNotEqZero` - "Success" if SQL query returns not 0.

Required parameters:

* `id` - check ID.
* `description [optional]` - description of the check. Optional.
* `date [optional]` - date in the format _"yyyy-MM-dd"_, for which the check is performed, if different from `executionDate`.
* `source` - source identifier.
* `query` - SQL query to the source (should return a number).

> These checks are intended to be calculated at the side of the source database and not to utilize Spark resources..