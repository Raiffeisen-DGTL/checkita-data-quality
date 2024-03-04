# Metrics Configuration

Calculation of various metrics over the data is the main part of Data Quality job. Metrics allow evaluation of 
various indicators that describe data from both technical and business points of view. Indicators in their turn can
signal about problems in the data.

All metrics are linked to a source over which they are calculated. Such metrics are called `regular`. Apart from regular
metrics there is a special kind of metrics that can be calculated based on other metrics results thus allowing metric
compositions. These metrics are called `composed` accordingly.

Metrics are defined in `metrics` section of job configuration. Regular metrics are grouped by their type in
`regular` subsection while composed metrics are listed in `composed` subsection.


## Regular Metrics

All regular metrics are defined using following common parameters: 

* `id` - *Required*. Metric ID;
* `description` - *Optional*. Metric description.
* `source` - *Required*. Reference to a source ID over which metric is caclulated;
* `columns` - *Required*. List of columns over which metric is calculated. Regular metrics can be calculated for 
  multiple columns. This means that the result of the metrics will be calculated for row values in these columns.
  There could be a limitation imposed on number of columns which metric can process. 
  The only exception is [Row Count Metric](#row-count-metric) which does not need columns to be specified.
* `params` - Some of the metrics may require additional parameters to be set. They should be specified within this
  object. The details on what parameters should be configured for metric are given below for each metric individually.
  Some metric definitions that require additional parameters are also have their default values set. In this case,
  `params` object can be omitted to use default options for all parameters.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this metric where each parameter
  is a string in format: `param.name=param.value`.

Additionally, some regular metrics have a logical condition that needs to be met when calculating metric increment per
each individual row. If metric condition is not met, then `Failure` status is returned for this particular row of data.
Scenario when metric can yield `Failure` status are explicitly described for each metric below.
See [Status Model used in Results](../02-general-concepts/03-StatusModel.md) chapter for more information on status model.

### Row Count Metric

Calculates number of rows in the source. This is the only metric for which columns list should not be
specified as it is not required to compute number of rows. Metric definition does not require additional parameters:
`params` should not be set.

All row count metrics are defined in `rowCount` subsection.

### Distinct Values Metric

Counts number of unique values in provided columns. When applied to multiple columns, total
number of unique values in these columns is returned. Metric definition does not require additional parameters:
`params` should not be set.

All distinct values metrics are defined in `distinctValues` subsection.

> **IMPORTANT**. Calculation of exact number of unique values required O(N) memory. Therefore, to prevent OOM errors
> when working with extremely large dataset and with high-cardinality columns it is recommended to use
> [Approximate Distinct Values Metric](#approximate-distinct-values-metric) which uses HLL probabilistic algorithm
> to estimate number of unique values.

### Approximate Distinct Values Metric

Calculates number of unique values approximately, using 
[HyperLogLog](https://twitter.github.io/algebird/datatypes/approx/hyperloglog.html) algorithm.

**This metric works with only one column.**

All approximate distinct values metrics are defined in `approximateDistinctValues` subsection. 
Additional parameters can be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for estimating number of unique values.

### Null Values Metrics

Counts number of null values in the specified columns. When applied to multiple columns, total
number of null values in these columns is returned. Metric definition does not require additional parameters:
`params` should not be set.

All distinct values metrics are defined in `nullValues` subsection.

Metric increment returns `Failure` status for rows where some values in the specified columns are null.

### Empty Values Metric

Counts number of empty values in the specified columns (i.e. empty string values). When applied to multiple columns,
total number of empty values in these columns is returned. Metric definition does not require additional parameters:
`params` should not be set.

All distinct values metrics are defined in `emptyValues` subsection.

Metric increment returns `Failure` status for rows where some values in the specified columns are empty.

### Completeness Metric

Calculates the measure of completeness in the specified columns: `(values_count - null_count) / values_count`.
When applied to multiple columns, total number of values and total number of nulls are used in the equation above.

All completeness metrics are defined in `completeness` subsection.
Additional parameters can be supplied:

* `includeEmptyStrings` - *Optional, default is `false`*. Boolean parameter indicating whether empty string values
  should be considered as nulls.

### Sequence Completeness Metric

Calculates measure of completeness of an incremental sequence of integers. In other words, it looks for the missing 
elements in the sequence and returns the relation: `actual number of elements / required number of elements`.

**This metric works with only one column.**

The actual number of elements is just the number of unique values in the sequence.
This metric defines it exactly, and therefore requires `O(N)` memory to store these values.
Therefore, to prevent OOM errors for extremely large sequences, it is recommended to use
the [Approximate Sequence Completeness Metric](#approximate-sequence-completeness-metric), which uses HLL probabilistic
algorithm to estimate number of unique values.

The required number of elements is determined by the formula: `(max_value - min_value) / increment + 1`,
Where:
* `min_value` - the minimum value in the sequence;
* `max_value` - the maximum value in the sequence;
* `increment` - sequence step, default is 1.

All sequence completeness metrics are defined in `sequenceCompleteness` subsection.
Additional parameters can be supplied:

* `incremet` - *Optional, default is `1`*. Sequence increment step.

### Approximate Sequence Completeness Metric

Calculates the measure of completeness of an incremental sequence of integers ***approximately*** using 
the [HyperLogLog](https://twitter.github.io/algebird/datatypes/approx/hyperloglog.html) algorithm. Works in the same 
way is [Sequence Completeness Metric](#sequence-completeness-metric) with only difference, that
actual number of elements in the sequence is determined approximately using HLL algorithm.

**This metric works with only one column.**

All approximate sequence completeness metrics are defined in `approximateSequenceCompleteness` subsection.
Additional parameters can be supplied:

* `incremet` - *Optional, default is `1`*. Sequence increment step.
* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for estimating number of unique values.

### Minimum String Metric

Calculates the minimum string length in the values of the specified columns. 
Metric definition does not require additional parameters: `params` should not be set.

All minimum string metrics are defined in `minString` subsection.

Metric increment returns `Failure` status for rows where all values in the specified columns are not castable 
to string and, therefore, minimum string length cannot be computed.

### Maximum String Metric

Calculates the maximum string length in the values of the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

All maximum string metrics are defined in `maxString` subsection.

Metric increment returns `Failure` status for rows where all values in the specified columns are not castable
to string and, therefore, maximum string length cannot be computed.

### Average String Metric

Calculates the average string length in the values of the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

All average string metrics are defined in `avgString` subsection.

Metric increment returns `Failure` status for rows where all values in the specified columns are not castable
to string and, therefore, average string length cannot be computed.

### String Length Metric

Calculate number of values that meet the defined string length criteria.

All string length metrics are defined in `stringLength` subsection.
Additional parameters should be supplied:

* `length` - *Required*. Required string length threshold.
* `compareRule` - *Required*. Comparison rule used to compare actual value string length with threshold one.
    * Following comparison rules are supported: `eq` (==), `lt` (<), `lte` (<=), `gt` (>), `gte` (>=).

Metric increment returns `Failure` status for rows where some values in the specified columns do not meet defined string
length criteria.

### String In Domain Metric

Counts number of values which fall into specified set of allowed values.

All string in domain metrics are defined in `stringInDomain` subsection.
Additional parameters should be supplied:

* `domain` - *Required*. List of allowed values.

Metric increment returns `Failure` status for rows where some values in the specified columns do not fall into set of 
allowed values.

### String Out Domain Metric

Counts number of values which do **not** fall into specified set of avoided values.

All string out domain metrics are defined in `stringOutDomain` subsection.
Additional parameters should be supplied:

* `domain` - *Required*. List of avoided values.

Metric increment returns `Failure` status for rows where some values in the specified columns **do fall into** set of
avoided values.

### String Values Metric

Counts number of values that are equal to the value given in metric definition.

All string values metrics are defined in `stringValues` subsection.
Additional parameters should be supplied:

* `compareValue` - *Required*. String value to compare with.

Metric increment returns `Failure` status for rows where some values in the specified columns do not match defined
compare value.

### Regex Match

Calculates number of values that match the defined regular expression.

All regex match metrics are defined in `regexMatch` subsection.
Additional parameters should be supplied:

* `regex` - *Required*. Regular expression to match.

Metric increment returns `Failure` status for rows where some values in the specified columns do not match defined
regular expression.

### Regex Mismatch

Calculates number of values that do **not** match the defined regular expression.

All regex mismatch metrics are defined in `regexMismatch` subsection.
Additional parameters should be supplied:

* `regex` - *Required*. Regular expression that values should **not** match.

Metric increment returns `Failure` status for rows where some values in the specified columns **do match** defined
regular expression.

### Formatted Date Metric

Counts number of values which have the specified datetime format.

All formatted date metrics are defined in `formattedDate` subsection.
Additional parameters can be supplied:

* `dateFormat` - *Optional, default is `yyyy-MM-dd'T'HH:mm:ss.SSSZ`*. Target datetime format. 
  The datetime format must be specified as 
  [Java DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) pattern.
  
> **NOTE** If the specified columns are of type `Timestamp`, it is assumed that they fit any datetime format and, 
> therefore,  metric will return the total number of non-empty cells. 
> Accordingly, the datetime format does not need to be specified.

Metric increment returns `Failure` status for rows where some values in the specified columns do not conform to defined
datetime format.

### Formatted Number Metric

Counts number of values which are numeric and number format satisfy defined number format criteria.

All formatted date metrics are defined in `formattedNumber` subsection.
Additional parameters should be supplied:

* `precision` - *Required*. The total number of digits in the value (excluding the decimal separator).
* `scale` - *Required*. Number of decimal digits in the value.
* `compareRule` - *Optional, default is `inbound`*. Number format comparison rule:
    * `inbound` - the value must "fit" into the specified number format: 
      actual precision and scale of the value are less than or equal to given ones.
    * `outbound` - the value must be outside the specified format: 
      actual precision and scale of the value are strictly greater than given ones.

Metric increment returns `Failure` status for rows where some values in the specified columns do not satisfy defined
number format criteria.

### Minimum Number Metric

Finds minimum number from the values in the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

All minimum number metrics are defined in `minNumber` subsection.

Metric increment returns `Failure` status for rows where all values in the specified columns are not castable
to number and, therefore, minimum number cannot be computed.

### Maximum Number Metric

Finds maximum number from the values in the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

All maximum number metrics are defined in `maxNumber` subsection.

Metric increment returns `Failure` status for rows where all values in the specified columns are not castable
to number and, therefore, maximum number cannot be computed.

### Sum Number Metric

Finds sum of the values in the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

All sum number metrics are defined in `sumNumber` subsection.

Metric increment returns `Failure` status for rows where some values in the specified columns are not castable
to number.

### Average Number Metric

Finds average of the values in the specified column.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with only one column.**

All average number metrics are defined in `avgNumber` subsection.

Metric increment returns `Failure` status for rows where value in the specified column is not castable
to number.

### Standard Deviation Number Metric

Finds standard deviation for the values in the specified column.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with only one column.**

All average number metrics are defined in `stdNumber` subsection.

Metric increment returns `Failure` status for rows where value in the specified column is not castable
to number.

### Casted Number Metric

Counts number of values which string value can be converted to a number (double).
Metric definition does not require additional parameters: `params` should not be set.

All sum number metrics are defined in `castedNumber` subsection.

Metric increment returns `Failure` status for rows where some values in the specified columns are not castable
to number.

### Number In Domain Metric

Counts number of values which being cast to number (double) fall into specified set of allowed numbers.

All number in domain metrics are defined in `numberInDomain` subsection.
Additional parameters should be supplied:

* `domain` - *Required*. List of allowed numbers.

Metric increment returns `Failure` status for rows where some values in the specified columns do not fall into set of
allowed numbers.

### Number Out Domain Metric

Counts number of values which being cast to number (double) do **not** fall into specified set of avoided numbers.

All number out domain metrics are defined in `numberOutDomain` subsection.
Additional parameters should be supplied:

* `domain` - *Required*. List of avoided numbers.

Metric increment returns `Failure` status for rows where some values in the specified columns **do fall into** set of
avoided numbers.

### Number Less Than Metric

Counts number of values which being cast to number (double) are less than (or equal to) the specified value.

All number less than metrics are defined in `numberLessThan` subsection.
Additional parameters should be supplied:

* `compareValue` - *Required*. Number to compare with.
* `includeBound` - *Optional, default is `false`*. Specifies whether to include `compareValue` in the range for comparison.

Metric increment returns `Failure` status for rows where some values in the specified columns do not satisfy the 
comparison criteria.

### Number Greater Than Metric

Counts number of values which being cast to number (double) are greater than (or equal to) the specified value.

All number greater than metrics are defined in `numberGreaterThan` subsection.
Additional parameters should be supplied:

* `compareValue` - *Required*. Number to compare with.
* `includeBound` - *Optional, default is `false`*. Specifies whether to include `compareValue` in the range for comparison.

Metric increment returns `Failure` status for rows where some values in the specified columns do not satisfy the
comparison criteria.

### Number Between Metric

Counts number of values which being cast to number (double) are within the given interval.

All number between metrics are defined in `numberBetween` subsection.
Additional parameters should be supplied:

* `lowerCompareValue` - *Required*. The lower bound of the interval.
* `upperCompareValue` - *Required*. The upper bound of the interval.
* `includeBound` - *Optional, default is `false`*. Specifies whether to include interval bounds in the range for comparison.

Metric increment returns `Failure` status for rows where some values in the specified columns do not satisfy the
comparison criteria.

### Number Not Between Metric

Counts number of values which being cast to number (double) are outside the given interval.

All number between metrics are defined in `numberNotBetween` subsection.
Additional parameters should be supplied:

* `lowerCompareValue` - *Required*. The lower bound of the interval.
* `upperCompareValue` - *Required*. The upper bound of the interval.
* `includeBound` - *Optional, default is `false`*. Specifies whether to include interval bounds in the range for comparison.

Metric increment returns `Failure` status for rows where some values in the specified columns do not satisfy the
comparison criteria.

### Number Values Metric

Counts number of values which being cast to number (double) are equal to the number given in metric definition.

All number values metrics are defined in `numberValues` subsection.
Additional parameters should be supplied:

* `compareValue` - *Required*. Number value to compare with.

Metric increment returns `Failure` status for rows where some values in the specified columns do not match defined
compare value.

### Median Value Metric

Calculates median value of the values in the specified column. Metric calculator uses 
[TDigest](https://github.com/isarn/isarn-sketches) library for computation of median value.

**This metric works with only one column.**

All median value metrics are defined in `medianValue` subsection.
Additional parameters can be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of median value.


Metric increment returns `Failure` status for rows where value in the specified column is not castable
to number.

### First Quantile Metric

Calculates first quantile for the values in the specified column. Metric calculator uses
[TDigest](https://github.com/isarn/isarn-sketches) library for computation of first quantile.

**This metric works with only one column.**

All median value metrics are defined in `firstQuantile` subsection.
Additional parameters can be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of first quantile value.

Metric increment returns `Failure` status for rows where value in the specified column is not castable
to number.

### Third Quantile Metric

Calculates third quantile for the values in the specified column. Metric calculator uses
[TDigest](https://github.com/isarn/isarn-sketches) library for computation of third quantile.

**This metric works with only one column.**

All third value metrics are defined in `thirdQuantile` subsection.
Additional parameters can be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of third value.

Metric increment returns `Failure` status for rows where value in the specified column is not castable
to number.

### Get Quantile Metric

Calculates an arbitrary quantile for the values in the specified column. Metric calculator uses
[TDigest](https://github.com/isarn/isarn-sketches) library for computation of quantile.

**This metric works with only one column.**

All get quantile metrics are defined in `getQuantile` subsection.
Additional parameters should be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of quantile value.
* `target` - *Required*. A number in the interval `[0, 1]` corresponding to the quantile that need to be caclulated.

Metric increment returns `Failure` status for rows where value in the specified column is not castable
to number.

### Get Percentile Metric

This metric is inverse of [Get Quantile Metric](#get-quantile-metric). It calculates a percentile value
(quantile in %) which corresponds to the specified number from the set of values in the column.
Metric calculator uses [TDigest](https://github.com/isarn/isarn-sketches) library for computation of percentile value.

**This metric works with only one column.**

All get percentile metrics are defined in `getPercentile` subsection.
Additional parameters should be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of percentile.
* `target` - *Required*. The number from the set of values in the column, for which percentile
  is determined.

Metric increment returns `Failure` status for rows where value in the specified column is not castable
to number.

### Column Equality Metric

Calculates the number of rows where values in the specified columns are equal to each other.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with at least two columns.**

All column equality metrics are defined in `columnEq` subsection.

Metric increment returns `Failure` status for rows where some values in the specified column are not castable to string
or when they are not equal.

### Day Distance Metric

Calculates the number of rows where difference between date in two columns expressed in terms of days is less
(strictly less) than the specified threshold value.

**This metric works with exactly two columns.**

All day distance metrics are defined in `dayDistance` subsection.
Additional parameters should be supplied:

* `threshold` - *Required*. Maximum allowed difference between two dates in days
  (not included in the range for comparison).
* `dateFormat` - *Optional, default is `yyyy-MM-dd'T'HH:mm:ss.SSSZ`*. Target datetime format.
  The datetime format must be specified as
  [Java DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) pattern.

> **NOTE** If the specified columns are of type `Timestamp`, it is assumed that they fit any datetime format and,
> therefore,  metric will return the total number of non-empty cells.
> Accordingly, the datetime format does not need to be specified.

Metric increment returns `Failure` status for rows where some values in the specified columns do not conform to 
the specified datetime format or when date difference in days is greater than or equal to specified threshold.

### Levenshtein Distance Metric

Calculates number of rows where Levenshtein distance between string values in the provided columns is less than
(strictly less) specified threshold.

**This metric works with exactly two columns.**

All levenshtein distance metrics are defined in `levenshteinDistance` subsection.
Additional parameters should be supplied:

* `threshold` - *Required*. Maximum allowed Levenshtein distance.
* `normalize` - *Optional, default is `false`*. Boolean parameter indicating whether the Levenshtein distance
  should be normalized with respect to the maximum of the two string lengths.

> **IMPORTANT**. If Levenshtein distance is normalized then threshold value must be in range `[0, 1]`.

Metric increment returns `Failure` status for rows where some values in the specified columns are not castable to string
or when Levenshtein distance is greater than or equal to specified threshold.

### CoMoment Metric

Calculates the covariance moment of the values in two columns (co-moment).
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with exactly two columns.**

> **IMPORTANT**. For the metric to be calculated, values in the specified columns must not be empty or null and 
> also can be cast to number (double). If at least one corrupt value is found, then metric calculator returns NaN value.

Metric increment returns `Failure` status for rows where some values in the specified columns cannot be cast to number.

### Covariance Metric

Calculates the covariance of the values in two columns.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with exactly two columns.**

> **IMPORTANT**. For the metric to be calculated, values in the specified columns must not be empty or null and
> also can be cast to number (double). If at least one corrupt value is found, then metric calculator returns NaN value.

Metric increment returns `Failure` status for rows where some values in the specified columns cannot be cast to number.

### Covariance Bessel Metric

Calculates the covariance of the values in two columns with the Bessel correction.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with exactly two columns.**

> **IMPORTANT**. For the metric to be calculated, values in the specified columns must not be empty or null and
> also can be cast to number (double). If at least one corrupt value is found, then metric calculator returns NaN value.

Metric increment returns `Failure` status for rows where some values in the specified columns cannot be cast to number.

### Top N Metric

This is a specific metric that calculates approximate N most frequently occurring values in a column.
The metric calculator uses [Twitter Algebird](https://github.com/twitter/algebird) library,
which implements abstract algebra methods for Scala.

**This metric works with only one column.**

All top N metrics are defined in `topN` subsection.
Additional parameters can be supplied:

* `targetNumber` - *Optional, default is `10`*. Number N of values to search.
* `maxCapacity` - *Optional, default is `100`*. Maximum container size for storing top values.

## Composed Metrics

Composed metrics are defined using a formula (specified in the `formula` field) for their calculation. 
As composed metric are intended for using other metric results to compute a derivative result then, these metrics
can be referenced in the formula by their IDs. 

Formula must be written using [Mustache Template](https://mustache.github.io/mustache.5.html) notation, e.g.:
`{{ metric_1 }} + {{ metic_2 }}`.

Basic (+-*/) and exponentiation (^) math operations are supported, as well as grouping using parentheses.

This, composed metrics are defined in the `composed` subsection using following parameters:

* `id` - *Required*. Composed metric ID;
* `description` - *Optional*. Composed metric description.
* `formula` - *Required*. Formula to calculate composed metric

## Metrics Configuration Example

As it is shown in the example below, regular metrics of the same type are grouped within subsections named after the 
type of the metric. These subsections should contain a list of metrics configurations of the corresponding type.
Composed metrics are listed in the separate subsection.

```hocon
jobConfig: {
  metrics: {
    regular: {
      rowCount: [
        {id: "hive_table_row_cnt", description: "Row count in hive_source_1", source: "hive_source_1"},
        {id: "csv_file_row_cnt", description: "Row count in hdfs_delimited_source", source: "hdfs_delimited_source"}
      ]
      distinctValues: [
        {
          id: "fixed_file_dist_name", description: "Distinct values in hdfs_fixed_file",
          source: "hdfs_fixed_file", columns: ["colA"],
          metadata: [
            "requestor=some.person@some.domain"
            "critical.metric=true"
          ]
        }
      ]
      nullValues: [
        {id: "hive_table_nulls", description: "Null values in columns id and name", source: "hive_source_1", columns: ["id", "name"]}
      ]
      completeness: [
        {id: "orc_data_compl", description: "Completness of column id", source: "hdfs_orc_source", columns: ["id"]}
        {
          id: "hive_table_nulls", 
          description: "Completness of columns id and name", 
          source: "hive_source_1", 
          columns: ["id", "name"]
        }
      ]
      avgNumber: [
        {id: "avro_file1_avg_bal", description: "Avg number of column balance", source: "hdfs_avro_source", columns: ["balance"]}
      ]
      regexMatch: [
        {
          id: "table_source1_inn_regex", description: "Regex match for inn column", source: "table_source_1",
          columns: ["inn"], params: {regex: """^\d{10}$"""}
        }
      ]
      stringInDomain: [
        {
          id: "orc_data_segment_domain", source: "hdfs_orc_source",
          columns: ["segment"], params: {domain: ["FI", "MID", "SME", "INTL", "CIB"]}
        }
      ]
      topN: [
        {
          id: "filterVS_top3_currency", description: "Top 3 currency in filterVS", source: "filterVS",
          columns: ["id"], params: {targetNumber: 3, maxCapacity: 10}
        }
      ],
      levenshteinDistance: [
        {
          id: "lvnstDist", source: "table_source_2", columns: ["col1", "col2"],
          params: {normalize: true, threshold: 0.3}
        }
      ]
    }
    composed: [
      {
        id: "pct_of_null", description: "Percent of null values in hive_table1",
        formula: "100 * {{ hive_table_nulls }} ^ 2 / ( {{ hive_table_row_cnt }} + 1)"
      }
    ]
  }
}
```