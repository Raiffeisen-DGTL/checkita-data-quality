# Metrics

Calculating metrics is the main part **Checkita** framework pipeline. They allow calculating indicators describing
data from technical point of view, as well as calculating various business indicators that can
signal about problems in the data.

All metrics are divided into three large groups:

* **File Metrics** - metrics that are calculated without actual reading data in columns.
  Currently, only counting the number of rows is supported: `rowCount`.
* **Column Metrics** - column metrics that are calculated
  based on the values in the cells of the specified columns.
* **Composed Metrics** - compositional metrics that are calculated
  based on File Metrics and Column Metrics using provided formula.

An example of a **metrics** section in a configuration file is shown below:

```hocon
metrics: {
   file: {
     rowCount: [
       {id: "hive_table1_row_cnt", description: "Row count in hive_table1", source: "hive_table1"},
       {id: "csv_file1", description: "Row count in csv_file1", source: "csv_file1"}
     ]
   }
   column: {
     distinctValues: [
       {
         id: "fiexed_file1_dist_name", description: "Distinct names in fiexed_file1",
         source: "fiexed_file1", columns: ["name"]
       }
     ]
     nullValues: [
       {id: "hive_table1_nulls", description: "Null values in columns id and name", source: "hive_table1", columns: ["id", "name"]}
     ]
     completeness: [
       {id: "orc_data_compl", description: "Completness of column id", source: "orc_data", columns: ["id"]}
     ]
     avgNumber: [
       {id: "avro_file1_avg_bal", description: "Avg number of column balance", source: "avro_file1", columns: ["balance"]}
     ]
     regex Match: [
       {
         id: "parquet_file_inn_regex", description: "Regex match for inn column", source: "parquet_file",
         columns: ["inn"], params: {regex: """^\d{10}$"""}
       }
     ]
     stringInDomain: [
       {
         id: "orc_data_segment_domain", description: "Segment should be in domain", source: "orc_data",
         columns: ["segment"], params: {domain: ["FI", "MID", "SME", "INTL", "CIB"]}
       }
     ]
     topN:[
       {
         id: "table1_top3_currency", description: "Top 3 currency in table1", source: "table1",
         columns: ["currency"], params: {targetNumber: 3, maxCapacity: 10}
       }
     ]
   }
   composed: [
     {
       id: "pct_of_null", description: "Percent of null values in hive_table1",
       formula: "100 * $hive_table1_nulls / $hive_table1_row_cnt"
     }
   ]
}
```

## File Metrics
### rowCount

This metric counts the number of rows in the source. Required parameters:

* `id` - metric ID
* `description [optional]` - description of the metric. Optional.
* `source` - identifier of the source by which the metric will be calculated.

## Column Metrics

All column metrics have a unified description format, as shown below.
Some of them require additional parameters for the calculation, which are specified
in the `params` subsection:

```hocon
{
   id: "metric ID"
   description: "metric description [optional]"
   source: "source id"
   columns: ["list", "columns"]
   params: { // additional parameters (if needed)
     param1: "value1",
     param2: "value2"
   }
}
```

Column metrics can be calculated for multiple columns at once.
This means that the result of the metric will be calculated for all cells in these columns.
For example, if you specify a list of three columns in the nullValues metric, the result will be
the total number of null values in these columns. The same is true for other metrics.
There are special types of metrics that can only be calculated on one or only two columns.
This will be indicated explicitly in the description of the metric.

**Metric statuses**

For most column metrics, you can explicitly specify whether the metric calculation for a particular row in the data is
successful or not. For example, for the `regexMatch` metric, the calculation will be successful when string values of
the specified columns in a row match the regex pattern. And, accordingly, if there is no match, then this is an error.

Metrics that support this behavior are called `Statusable Metrics`.
And for such metrics, an error collection mechanism is implemented, which is described in detail in
the [Targets](./Targets.md) section.

Further, when describing each metric, it will be indicated whether it is `Statusable` or not, and also it will be
described when the metric is considered successfully calculated, and when calculation result will be treated as an error.

### distinctValues

Counts the number of unique values in cells.
There are no additional parameters (`params`).

The metric is **not** `Satusable`.

### approximateDistinctValues

Calculates the approximate number of unique values in a column using the HyperLogLog algorithm.
***Works with only one column.***
Additional parameters: `accuracyError` (optional, default 0.01):

The metric is **not** `Satusable`.

```hocon
params: {accuracyError: 0.05}
```

### nullValues

Counts the number of nulls in cells.
There are no additional parameters (`params`).

Metric **is** `Satusable`:
* Succeeds if the value in the column is **not** `Null`;
* Error if the value in the column is `Null`.

### emptyValues

Counts the number of empty cells (with an empty string).
There are no additional parameters (`params`).

Metric **is** `Satusable`:
* Succeeds if the value in the column **is not** an empty string;
* Error if the value in the column is an empty string.

### completeness

Calculates the measure of completeness in the column(s): `(cell_count - null_count) / cell_count`
Additional options: `includeEmptyStrings` (optional, false by default),
to additionally account for empty cells.

The metric is **not** `Satusable`.

```hocon
params: {includeEmptyStrings: true}
```

### sequenceCompleteness

Calculates the measure of completeness of an incremental sequence of integers.
In other words, it looks for the missing elements in the sequence and returns the relation:
`actual number of elements / required number of elements`.
The sequence does not have to be sorted.

***Works with only one column.***

The actual element count is the number of unique elements in the sequence.
This metric defines it exactly, and therefore requires `O(N)` memory to store the values.
For very large sequences (tens of millions or more records), it is recommended to use
the ***approximateSequenceCompleteness*** metric, which approximates the number of unique elements.

The required number of elements is determined by the formula: `(max_value - min_value) / increment + 1`,
Where:
* `min_value` - the minimum value in the sequence;
* `max_value` - the maximum value in the sequence;
* `increment` - sequence step, default is 1.

Thus, the additional parameters for the metric are:
* `incremet [optional]` - sequence step. Default 1;

The metric is **not** `Satusable`.

```hocon
params: {increment: 4}
```

### approximateSequenceCompleteness

Calculates the measure of completeness of an incremental sequence of integers
***approximately*** using the HyperLogLog algorithm.
In other words, it looks for the missing elements in the sequence and returns the relation:
`actual number of elements / required number of elements`.
The sequence does not have to be sorted.

***Works with only one column.***

The actual element count is the number of unique elements in the sequence.
This metric determines it approximately using the HyperLogLog probabilistic algorithm, and therefore
requires significantly less memory.

The required number of elements is determined by the formula: `(max_value - min_value) / increment + 1`,
Where:
* `min_value` - the minimum value in the sequence;
* `max_value` - the maximum value in the sequence;
* `increment` - sequence step, default is 1.

Thus, in the additional parameters for the metric, you can specify:
* `incremet [optional]` - sequence step. Default 1;
* `accuracyError [optional]` - accuracy of calculating unique values in a sequence. The default is 0.01.

The metric is **not** `Satusable`.

```hocon
params: {increment: 4, accuracyError: 0.05}
```


### minString

Calculates the minimum string length in cells.
There are no additional parameters (`params`).

The metric is **not** `Satusable`.

### maxString

Calculates the maximum length of a string in cells.
There are no additional parameters (`params`).

The metric is **not** `Satusable`.

### avgString

Calculates the average length of a string in cells.
There are no additional parameters (`params`).

The metric is **not** `Satusable`.

### stringLength

Calculates the number of cells that meet the string length criteria.
Extra options:

* `length` - the length of the string to compare.
* `compareRule` - comparison rule. Takes one of the following values:
  * `eq` (==), `lt` (<), `lte` (<=), `gt` (>), `gte` (>=).

Metric **is** `Satusable`:
* Succeeds if the value in the column satisfies the string length condition;
* Error otherwise.

```hocon
params: {length: 25, compareRule: "eq"}
```

### stringInDomain

Counts the number of cells in a column/columns whose values fall into the specified set of values.

Additional parameters: `domain` - list of possible values.

Metric **is** `Satusable`:
* Succeeds if the value in the column fall into the specified set of values;
* Error otherwise.

```hocon
params: {domain: ["FI", "MID", "SME", "INTL", "CIB"]}
```

### stringOutDomain

Counts the number of cells in a column/columns whose values do **not** fall into the specified set of values.

Additional parameters: `domain` - list of possible values.

Metric **is** `Satusable`:
* Succeeds if the value in the column **does not** fall into the specified set of values;
* Error otherwise.

```hocon
params: {domain: ["FI", "MID", "SME", "INTL", "CIB"]}
```

### stringValues

Counts the number of cells that have the same value as the given value.
Additional parameters: `compareValue` - target value.

Metric **is** `Satusable`:
* Succeeds if the value in the column matches the given one;
* Error otherwise.

```hocon
params: {compareValue: "MICR"}
```

### regexMatch

Counts the number of cells that match the regular expression.
Additional options: `regex` - regular expression to match.

Metric **is** `Satusable`:
* Succeeds if the value in the column matches the given regular expression;
* Error otherwise.

```hocon
params: {regex: """^\d{10}$"""}
```

> **Tip**: Regular expressions must be enclosed in triple quotes.
> Or in single ones, but escaping special characters with ` \ `

### regexMismatch

Counts the number of cells whose values **do not** match the regular expression.
Additional options: `regex` - regular expression to match.

Metric **is** `Satusable`:
* Succeeds if the value in the column **does not** match the given regular expression;
* Error otherwise.

```hocon
params: {regex: """^\d{10}$"""}
```

### formattedDate

Counts the number of cells where a string value has the specified date format.
Additional parameters: `dateFormat [optional]` - target date format. The date format is specified
as [Joda DateTime Format](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html).
The default date format is `yyyy-MM-dd'T'HH:mm:ss.SSSZ`. If the specified columns are of type `Timestamp`,
it is assumed that they fit any date format and the metric will return the total number of non-empty cells.
Accordingly, the date format does not need to be specified.

Metric **is** `Satusable`:
* Succeeds if the string value in the column has the specified date format;
* Error otherwise.

```hocon
params: {dateFormat: "yyyy-MM-dd"}
```

### formattedNumber

Counts the number of cells where a numeric value matches the specified format.
Extra options:

* `precision` - the total number of digits of the number (excluding the decimal separator).
* `scale` - number of decimal places.
* `compareRule [optional]` - comparison rule:
  * `inbound` - the number must "fit" into the specified format (precision and scale numbers are less than or equal to the given ones)
  * `outbound` - the number is outside the specified format (precision and scale numbers are strictly greater than the given ones).
  * The default comparison rule is `inbound`.

Metric **is** `Satusable`:
* Succeeds if the numeric value in the column fits the specified format with given rule;
* Error otherwise.

```hocon
params: {precision: 14, scale: 8, compareRule: "outbound"}
```

### minNumber

Finds the minimum number in cells. There are no additional parameters (`params`).

The metric is **not** `Satusable`.

### maxNumber

Finds the maximum number in cells. There are no additional parameters (`params`).

The metric is **not** `Satusable`.

### sumNumber

Finds the sum of numbers in cells. There are no additional parameters (`params`).

The metric is **not** `Satusable`.

### avgNumber

Calculates the average of the numbers in a column. ***Works with only one column.***
There are no additional parameters (`params`).

The metric is **not** `Satusable`.

### stdNumber

Calculates the standard deviation for the numbers in a column. ***Works with only one column.***
There are no additional parameters (`params`).

The metric is **not** `Satusable`.

### castedNumber

Counts the number of cells where a string value can be converted to a number (double).
There are no additional parameters (`params`).

Metric **is** `Satusable`:
* Succeeds if the string value in the column can be converted to a numeric value;
* Error otherwise.

### numberInDomain

Counts the number of cells whose numeric values fall into the specified set of values.

Additional parameters: `domain` - list of possible numeric values.

Metric **is** `Satusable`:
* Succeeds if the numeric value in the column falls into the specified set;
* Error otherwise.

```hocon
params: {domain: [1.23, 2.34, 3.45, 4.56, 5.67]}
```

### numberOutDomain

Counts the number of cells whose numeric values **do not** fall into the specified set of values.

Additional parameters: `domain` - list of possible numeric values.

Metric **is** `Satusable`:
* Succeeds if the numeric value in the column **does not** fall into the specified set;
* Error otherwise.

```hocon
params: {domain: [1.23, 2.34, 3.45, 4.56, 5.67]}
```

### numberLessThan

Counts the number of cells where the numeric value is less than the specified value.
Extra options:

* `compareValue` - number to compare.
* `includeBound [optional]` - specifies whether to include `compareValue` in the range for comparison.
  The default is false.

Metric **is** `Satusable`:
* Succeeds if the numeric value in the column is less than the given one;
* Error otherwise.

```hocon
params: {compareValue: 3.14, includeBound: true}
```

### numberGreaterThan

Counts the number of cells where the numeric value is greater than the specified value.
Extra options:

* `compareValue` - number to compare.
* `includeBound [optional]` - specifies whether to include `compareValue` in the range for comparison.
  The default is false.

Metric **is** `Satusable`:
* Succeeds if the numeric value in the column is greater than the given one;
* Error otherwise.

```hocon
params: {compareValue: 3.14, includeBound: true}
```

### numberBetween

Counts the number of cells where the numeric value is within the given interval.
Extra options:

* `lowerCompareValue` - the lower bound of the range.
* `upperCompareValue` - the upper limit of the interval.
* `includeBound [optional]` - indicates whether bounds should be included in the interval for comparison.
  The default is false.

Metric **is** `Satusable`:
* Succeeds if the numeric value in the column is within the given range;
* Error otherwise.

```hocon
params: {lowerCompareValue: 17, upperCompareValue: 71, includeBound: true}
```

### numberNotBetween

Counts the number of cells where the numeric value is outside the specified range.
Extra options:

* `lowerCompareValue` - the lower bound of the range.
* `upperCompareValue` - the upper limit of the range.
* `includeBound [optional]` - indicates whether bounds should be included in the interval for comparison.
  The default is false.

Metric **is** `Satusable`:
* Succeeds if the numeric value in the column is outside the given range;
* Error otherwise.

```hocon
params: {lowerCompareValue: 10, upperCompareValue: 22, includeBound: true}
```

### numberValues

Counts the number of cells that have a numeric value that matches the specified value.
Additional parameters: `compareValue` - target numeric value.

Metric **is** `Satusable`:
* Succeeds if the numeric value in the column matches the given one;
* Error otherwise.

```hocon
params: {compareValue: 42}
```

### TDigest Metrics

Metrics that calculate percentiles and quantiles using the [TDigest] library (https://github.com/isarn/isarn-sketches).

**These metrics work with one column and are not `Satusable`.**

The common additional parameter for this group's metrics is `accuracyError`,
which specifies the precision of the metric calculation. This parameter is optional and defaults to 0.005.

#### medianValue

Calculates the median value for the numbers in the column (second quantile).

#### firstQuantile

Calculates the first quantile for the numbers in the column.

#### thirdQuantile

Calculates the third quantile for the numbers in the column.

#### getQuantile

Calculates an arbitrary quantile for the numbers in a column.
Optional parameter: `targetSideNumber` - a number in the interval [0, 1] corresponding to the quantile to be calculated.

```hocon
params: {accuracyError: 0.01, targetSideNumber: 0.15}
```

#### getPercentile

Inverse of the metric [getQuantile](#getQuantile): calculates the percentile value (quantile in %),
which corresponds to the specified number from the set of values in the column.
Additional parameter: `targetSideNumber` - the number from the set of values in the column, for which percentile
is determined.

```hocon
params: {accuracyError: 0.01, targetSideNumber: 30}
```

### Multi-Column Metrics

Metrics in this group compare cell values in columns with each other row by row
and count the number of rows where the comparison result satisfy the criterion.

#### columnEq

Counts the number of rows where the values in the cells of the specified columns match.
There are no additional parameters (`params`).

Metric **is** `Satusable`:
* Successful if the values in the specified columns match;
* Error otherwise.

#### dayDistance

Counts the number of rows where the difference in date values between two columns
less than the specified threshold (strictly less). **Only works with two columns.**
Extra options:

* `threshold` - maximum difference between two dates in days.
* `dateFormat [optional]` - date format. The date format is specified as
  [Joda DateTime Format](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html).
  The default date format is `yyyy-MM-dd'T'HH:mm:ss.SSSZ`. If the specified columns are of type `Timestamp`,
  it is assumed that they fit any date format. Accordingly, the date format does not need to be specified.

Metric **is** `Satusable`:
* Succeeds if the date difference between two columns is less than the specified threshold;
* Error otherwise.

```hocon
params: {threshold: 1, dateFormat: "yyyy-MM-dd'T'HH:mm:ss.SSSZ"}
```

#### levenshteinDistance

Counts the number of rows where the Levenshtein distance between string values in columns is less than or equal
to the specified threshold. **Only works with two columns.**

Extra options:

* `threshold` - Levenshtein distance threshold value.
* `normalize [optional]` - true/false flag indicating whether the Levenshtein distance will be normalized with respect
  to the maximum of the two string lengths. The default is false. **If the result is normalized, then the threshold value
  `threshold` must be in the range [0, 1].**

Metric **is** `Satusable`:
* Succeeds if the Levenshtein distance between string values in columns is less than or equal to the specified threshold;
* Error otherwise.

```hocon
params: {threshold: 3}
```

#### coMoment

Calculates the covariance moment of the values in two columns (co-moment). **Only works with two columns.
For the correct calculation of the metric, the columns must not contain empty cells, and the values of all cells must be numeric.**
There are no additional parameters (`params`).

Metric **is** `Satusable`:
* Succeeds if the values in the columns are not empty and are numeric;
* Error otherwise.

#### covariance

Calculates the covariance of the values in two columns. **Only works with two speakers.
For the correct calculation of the metric, the columns must not contain empty cells, and the values of all cells must be numeric.**
There are no additional parameters (`params`).

Metric **is** `Satusable`:
* Succeeds if the values in the columns are not empty and are numeric;
* Error otherwise.

#### covarianceBessel

Calculates the covariance of the values in two columns with the Bessel correction. **Only works with two speakers.
For the correct calculation of the metric, the columns must not contain empty cells, and the values of all cells must be numeric.**
There are no additional parameters (`params`).

Metric **is** `Satusable`:
* Succeeds if the values in the columns are not empty and are numeric;
* Error otherwise.

### Special Metrics

#### topN

The TopN metric calculates the approximate N most frequently occurring values in a column.
The metric is calculated using the [Twitter Algebird](https://github.com/twitter/algebird) library,
which implements abstract algebra methods for Scala.
**Works with only one column!**

Additional options:

* `targetNumber [optional]` - specifies the number N of values to search. The default is 10.
* `maxCapacity [optional]` - maximum container size for storing top values. The default is 100.

The metric is **not** `Satusable`.

```hocon
params: {targetNumber: 4, maxCapacity: 10}
```

> TopN metric returns a multiple result: one row for each value in top N.
> The format of the result will be the following:
>
> * the identifier is followed by a suffix indicating the position in the top: id + '_' + position
> * The frequency of occurrence of the value is displayed as a numeric result.
> * As an additional result, the value itself from the top is displayed.

## Composed Metrics

Compositional metrics are defined using a formula (specified in the `formula` field) for their calculation,
in which base metrics are specified with a `$` prefix. Basic math operations are supported (+-*/) and
exponentiation (^), as well as grouping using parentheses.

```hocon
{
   id: "alternative_completeness",
   description: "Percent of non-null values in hive_table1",
   formula: "100 * ($hive_table1_row_cnt - $hive_table1_nulls) / $hive_table1_row_cnt"
}
```
