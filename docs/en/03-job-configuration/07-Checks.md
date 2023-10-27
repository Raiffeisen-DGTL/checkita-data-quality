# Checks Configurations

Performing checks ove the metric results is an important step in Checkita framework. As metric results are calculated
then checks can be configured to identify if there are any problems with quality of data.

In Checkita there are two main group of checks:

* `Spanshot` checks - allows comparison of metric results with static thresholds or with other metric results in the 
  same Data Quality job.
* `Trend` checks - allows evaluation of how metric result is changing over a certain period of time. Checks of this type
  are used to detect anomalies in data. In order trend check work it is required to set up Data Quality storage since
  check calculator need to fetch historical results for the metric of interest.

After evaluation, check will have a status as described in 
[Status Model used in Results](../02-general-concepts/03-StatusModel.md) chapter. 

## Snapshot Checks

Snapshot checks represent a simple comparison of metric results with a static threshold or with other metric result.

The following snapshot checks are supported:

* `equalTo` - checks if metric results is equal to a given threshold value or to other metric result.
* `lessThan` - checks if metric result is less than a given threshold value or other metric result.
* `greaterThan` - checks if metric result is greater than a given threshold value or other metric result.
* `differByLT` - checks if relative difference between ***two metric results*** is less than a given threshold.
  This check succeeds when following expression is true: `| metric - compareMetric | / compareMetric < threshold`.

Snapshot checks are configured using common set of parameters, which are:

* `id` - *Required*. Check ID
* `description` - *Optional*. Description of the check.
* `metric` - *Required*. Metric ID which results is checked.
* `compareMetric` - *Optional*. Metric ID which result is used as a threshold.
* `threshold` - *Optional*. Explicit threshold value.

> **IMPORTANT**. When configuring check it should be specified either an explicit threshold value in `threshold` field
> or other metric ID in `compareMetric` field which result will be used as a threshold value.
> The only exception to this rule is `differByLY` check for which it is required to specify both, threshold value and
> metric ID to compare with.

## Trend Checks

Trend checks are used to detect anomalies in data. This type of checks allows to verify that the value of the metric 
corresponds to its average value within a given deviation for a certain period of time. Maximum allowed deviation is 
configured by providing a threshold value.

Following trend checks are supported:

* `averageBoundFull` - sets the same upper and lower deviation from metric average result. Check succeeds when following
  expression is true: `(1 - threshold) * avgResult <= currentResult <= (1 + threshold) * avgResult`.
* `averageBoundUpper` - verifies only upper deviation from the metric average result. Check succeeds when following
  expression is true: `currentResult <= (1 + threshold) * avgResult`.
* `averageBoundLower` - verifies only lower deviation from the metric average result. Check succeeds when following
  expression is ture: `(1 - threshold) * avgResult <= currentResult`.
* `averageBoundRange` - sets different thresholds for upper and lower deviations from metric average results.
  Check succeeds when following expression is true:
  `(1 - thresholdLower) * avgResult <= currentResult <= (1 + thresholdUpper) * avgResult`.

Trend checks are configured using following set of parameters:

* `id` - *Required*. Check ID
* `description` - *Optional*. Description of the check.
* `metric` - *Required*. Metric ID which result is checked.
* `rule` - *Required*. The rule for calculating historical average value of the metric. There are two rules supported:
    * `record` - calculates the average value of metric for the configured number of historical records.
    * `datetime` - calculates the average value of metric for the configured datetime window.
* `windowSize` - *Required*. Size of the window for average metric value calculation:
    * If `rule` is set to `record` then window size is the number of records to retrieve.
    * If `rule` is set to `datetime` then window size is a duration string which should conform to Scala Duration.
* `windowOffset` - *Optional, default is `0` or `0s`*. Set window offset back from current reference date
  (see [Working with Date and Time](../02-general-concepts/01-WorkingWithDateTime.md) chapter for more details on 
  reference date). By default, offset is absent and window start from current reference date.
    * If `rule` is set to `record` then window offset is the number of records to skip from reference date.
    * If `rule` is set to `datetime` then window offset is a duration string which should conform to Scala Duration.
* `threshold` - *Required*. Sets maximum allowed deviation from historical average metric result. *Not used with
  `averageBoundRange` check*.
* `thresholdLower` - *Required*. Sets maximum allowed lower deviation from historical average metric result. *Used only
  for `averageBoundRange` check.
* `thresholdUpper` - *Required*. Sets maximum allowed upper deviation from historical average metric result. *Used only
  for `averageBoundRange` check.

> **NOTE**. Scala Duration string has a format of `<length><unit>` where following units are allowed:
> `d`, `day`, `h`, `hr`, `hour`, `m`, `min`, `minute`, `s`, `sec`, `second`, `ms`, `milli`, `millisecond`,
> `µs`, `micro`, `microsecond`, `ns`, `nano`, `nanosecond`.

### Top N Rank Check

This is a special check designed specifically for [Top N Metric](06-Metrics.md#top-n-metric) and working only with it.
Top N rank check calculates the Jacquard distance between the current and previous sets of top N metric and checks if
it does not exceed the threshold value.

> **IMPORTANT**: Calculation of this check is currently supported only between the current and previous topN metric sets.

Top N rank check is configured using following parameters:

* `id` - *Required*. Check ID
* `description` - *Optional*. Description of the check.
* `metric` - *Required*. Metric ID which result is checked.
* `targetNumber` - *Required*. Number of records from the set of top N metric results that is considered. 
  This number should be less than or equal to number of collected top values in top N metric.
* `threshold` - *Required*. Maximum allowed Jacquard distance between current and previous sets of records from 
  top N metric result. Should be a number in interval `[0, 1]`.

## Checks Configuration Example

As it is shown in the example below, checks are grouped into two subsections: `trend` and `snapshot`.
Then, checks of the same type are grouped within subsections named after the type of the checks. 
These subsections should contain a list of metrics configurations of the corresponding type.

```hocon
jobConfig: {
  checks: {
    trend: {
      averageBoundFull: [
        {
          id: "avg_bal_check",
          description: "Check that average balance stays within +/-25% of the week average"
          metric: "avro_file1_avg_bal",
          rule: "datetime"
          windowSize: "8d"
          threshold: 0.25
        }
      ]
      averageBoundUpper: [
        {id: "avg_pct_null", metric: "pct_of_null", rule: "datetime", windowSize: "15d", threshold: 0.5}
      ]
      averageBoundLower: [
        {id: "avg_distinct", metric: "fixed_file_dist_name", rule: "record", windowSize: 31, threshold: 0.3}
      ]
      averageBoundRange: [
        {
          id: "avg_inn_match",
          metric: "table_source1_inn_regex",
          rule: "datetime",
          windowSize: "8d",
          thresholdLower: 0.2
          thresholdUpper: 0.4
        }
      ]
      topNRank: [
        {id: "top2_curr_match", metric: "filterVS_top3_currency", targetNumber: 2, threshold: 0.1}
      ]
    }
    snapshot: {
      differByLT: [
        {
          id: "row_cnt_diff",
          description: "Number of rows in two tables should not differ on more than 5%.",
          metric: "hive_table_row_cnt"
          compareMetric: "csv_file_row_cnt"
          threshold: 0.05
        }
      ]
      equalTo: [
        {id: "zero_nulls", description: "Hive Table1 mustn't contain nulls", metric: "hive_table_nulls", threshold: 0}
      ]
      greaterThan: [
        {id: "completeness_check", metric: "orc_data_compl", threshold: 0.99}
      ]
      lessThan: [
        {id: "null_threshold", metric: "pct_of_null", threshold: 0.01}
      ]
    }
  }
}
```