# Metrics Configuration

Calculation of various metrics over the data is the main part of Data Quality job. Metrics allow evaluation of 
various indicators that describe data from both technical and business points of view. Indicators in their turn can
signal about problems in the data.

Most of the metrics are linked to a source over which they are calculated. Most of the metrics are computed directly over 
the data source. Such metrics are called `regular`. Apart from regular metrics there are two special kinds of metrics:

* `composed` metrics - can be calculated based on other metrics results thus allowing metric compositions.
* `trend` metrics - calculated using historical results of lookup metric thus allowing to compute some statistics over
  historical metric results.

Metrics are defined in `metrics` section of job configuration. Regular metrics are grouped by their type in
`regular` subsection. Composed and trend metrics are listed in `composed` and `trend` subsections respectively. 

## Regular Metrics

All regular metrics are defined using following common parameters: 

* `id` - *Required*. Metric ID;
* `description` - *Optional*. Metric description.
* `source` - *Required*. Reference to a source ID over which metric is calculated;
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

#### Reversible Metrics

Some regular metrics have a logical condition that needs to be met when calculating metric increment per
each individual row. These metrics can ***fail*** if this condition is not met. What is more importantly,
is that in some situations it is quite desirable that metric will ***fail*** when its condition IS met.

In order to support such functionality, we made error collection logic for regular metrics with condition
to be reversible. This means, that you can choose whether metric fails, when its condition is met or vice versa.
This is controlled via additional boolean parameter called `reversed`. This parameter is optional and
is set to `false` for most of the regular metrics with condition.

Further, regular metrics with condition will be called ***reversible metrics*** due to fact that error
collection logic for them can be reversed. Thus, if `reversed` parameter is set to `false` and metric 
condition is not met, then `Failure` status is returned for this particular row of data. In case if
`reversed` parameter is set to `true` metric will fail when its condition IS met.

Regular metrics that do not have logical condition can also fail, but their failure logic is not reversible.

Scenario when metric can yield `Failure` status are explicitly described for each metric below.
See [Status Model used in Results](../02-general-information/03-StatusModel.md) chapter for more information on status model.

### Row Count Metric

Calculates number of rows in the source. This is the only metric for which columns list should not be
specified as it is not required to compute number of rows. Metric definition does not require additional parameters:
`params` should not be set.

Metric is not reversible and cannot fail (might only return `Error` status in case of some runtime exception).

All row count metrics are defined in `rowCount` subsection.

### Duplicate Values Metric

Counts number of duplicate values in provided columns. When applied to multiple columns, number of duplicate
tuples is returned. Metric definition does not require additional parameters:
`params` should not be set.

Metric is not reversible and fails only when duplicate value (or tuple for multi-column definition) is found.

All duplicate values metrics are defined in `duplicateValues` subsection.

> **IMPORTANT**. When using RDD-engine, calculation of exact number of duplicate values required O(N) memory. 
> Therefore, computing exact number of duplicates might cause OOM errors. Consider using DF-engine if exact number
> of duplicates is required. DF-engine uses Spark shuffle mechanism to group data by column (or columns) of interest and
> count duplicated values. Alternatively, there is an option to calculate approximate number of distinct values
> using HLL-based metric calculator [Approximate Distinct Values Metric](#approximate-distinct-values-metric).
> Comparing number of distinct values to total number of rows can yield estimated number of duplicate values.

### Distinct Values Metric

Counts number of unique values in provided columns. When applied to multiple columns, total
number of unique tuples is returned. Metric definition does not require additional parameters:
`params` should not be set.

Metric is not reversible and fails only when null value (or tuple) is found.

All distinct values metrics are defined in `distinctValues` subsection.

> **IMPORTANT**. Calculation of exact number of unique values required O(N) memory. Therefore, to prevent OOM errors
> when working with extremely large dataset and with high-cardinality columns it is recommended to use
> [Approximate Distinct Values Metric](#approximate-distinct-values-metric) which uses HLL probabilistic algorithm
> to estimate number of unique values. Alternatively, consider switching to DF-engine to compute exact number of 
> distinct values. This engine uses Spark shuffle mechanism to group data by column (or columns) of interest and
> count distinct values.

### Approximate Distinct Values Metric

Calculates number of unique values approximately, using 
[HyperLogLog](https://twitter.github.io/algebird/datatypes/approx/hyperloglog.html) algorithm.

**This metric works with only one column.**

Metric is not reversible and fails only if provided value cannot be cast to String.

All approximate distinct values metrics are defined in `approximateDistinctValues` subsection. 
Additional parameters can be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for estimating number of unique values.

### Null Values Metrics

Counts number of null values in the specified columns. When applied to multiple columns, total
number of null values in these columns is returned. Metric definition does not require additional parameters:
`params` should not be set.

Metric is reversible. **By default, `reversed` parameter is set to `true`, 
i.e. error collection logic is reversed by default.** 
For direct error collection logic, metric increment returns `Failure` status for rows where some values in the 
specified columns are NON-null. For reversed error collection logic (default one), metric increment returns `Failure`
status when some values in the specified columns are null. 

All distinct values metrics are defined in `nullValues` subsection.

### Empty Values Metric

Counts number of empty values in the specified columns (i.e. empty string values). When applied to multiple columns,
total number of empty values in these columns is returned. Metric definition does not require additional parameters:
`params` should not be set.

Metric is reversible. **By default, `reversed` parameter is set to `true`,
i.e. error collection logic is reversed by default.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in the
specified columns are non-empty. For reversed error collection logic (default one), metric increment returns `Failure`
status when some values in the specified columns are empty.

All distinct values metrics are defined in `emptyValues` subsection.

### Completeness Metric

Calculates the measure of completeness in the specified columns: `(values_count - null_count) / values_count`.
When applied to multiple columns, total number of values and total number of nulls are used in the equation above.

Additional parameters can be supplied:

* `includeEmptyStrings` - *Optional, default is `false`*. Boolean parameter indicating whether empty string values
  should be considered as nulls.

Metric is reversible. **By default, `reversed` parameter is set to `true`,
i.e. error collection logic is reversed by default.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in the
specified columns are non-noll (or non-empty if `includeEmptyStrings` is set to `true`). 
For reversed error collection logic (default one), metric increment returns `Failure` status when some values 
in the specified columns are null (or empty if `includeEmptyStrings` is set to `true`).

All completeness metrics are defined in `completeness` subsection.

### Emptiness Metric

Calculates the measure of emptiness in the specified columns: `null_count / values_count`.
When applied to multiple columns, total number of values and total number of nulls are used in the equation above.

Additional parameters can be supplied:

* `includeEmptyStrings` - *Optional, default is `false`*. Boolean parameter indicating whether empty string values
  should be considered as nulls.

Metric is reversible. **By default, `reversed` parameter is set to `true`,
i.e. error collection logic is reversed by default.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in the
specified columns are non-noll (or non-empty if `includeEmptyStrings` is set to `true`).
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns are null (or empty if `includeEmptyStrings` is set to `true`).

All completeness metrics are defined in `emptiness` subsection.

### Sequence Completeness Metric

Calculates measure of completeness of an incremental sequence of integers. In other words, it looks for the missing 
elements in the sequence and returns the relation: `actual number of elements / required number of elements`.

**This metric works with only one column.**

The actual number of elements is just the number of unique values in the sequence.
This metric defines it exactly, and therefore requires `O(N)` memory to store these values.
Therefore, to prevent OOM errors for extremely large sequences, it is recommended to use
the [Approximate Sequence Completeness Metric](#approximate-sequence-completeness-metric), which uses HLL probabilistic
algorithm to estimate number of unique values.

The required number of elements is determined by the formula: `(max_value - min_value) / increment + 1`, where:

* `min_value` - the minimum value in the sequence;
* `max_value` - the maximum value in the sequence;
* `increment` - sequence step, default is 1.

Additional parameters can be supplied:

* `incremet` - *Optional, default is `1`*. Sequence increment step.

Metric is not reversible and fails only if provided column value cannot be cast to number.

All sequence completeness metrics are defined in `sequenceCompleteness` subsection.

### Approximate Sequence Completeness Metric

Calculates the measure of completeness of an incremental sequence of integers ***approximately*** using 
the [HyperLogLog](https://twitter.github.io/algebird/datatypes/approx/hyperloglog.html) algorithm. Works in the same 
way is [Sequence Completeness Metric](#sequence-completeness-metric) with only difference, that
actual number of elements in the sequence is determined approximately using HLL algorithm.

**This metric works with only one column.**

Additional parameters can be supplied:

* `incremet` - *Optional, default is `1`*. Sequence increment step.
* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for estimating number of unique values.

Metric is not reversible and fails only if provided column value cannot be cast to number.

All approximate sequence completeness metrics are defined in `approximateSequenceCompleteness` subsection.

### Minimum String Metric

Calculates the minimum string length in the values of the specified columns. 
Metric definition does not require additional parameters: `params` should not be set.

All minimum string metrics are defined in `minString` subsection.

Metric is not reversible. Metric increment returns `Failure` status for rows where all values in the specified 
columns cannot be cast to string and, therefore, minimum string length cannot be computed.

### Maximum String Metric

Calculates the maximum string length in the values of the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

Metric is not reversible. Metric increment returns `Failure` status for rows where all values in the specified 
columns cannot be cast to string and, therefore, maximum string length cannot be computed.

All maximum string metrics are defined in `maxString` subsection.

### Average String Metric

Calculates the average string length in the values of the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

Metric is not reversible. Metric increment returns `Failure` status for rows where all values in the specified
columns cannot be cast to string and, therefore, average string length cannot be computed.

All average string metrics are defined in `avgString` subsection.

### String Length Metric

Calculate number of values that meet the defined string length criteria.
Additional parameters should be supplied:

* `length` - *Required*. Required string length threshold.
* `compareRule` - *Required*. Comparison rule used to compare actual value string length with threshold one.
    * Following comparison rules are supported: `eq` (==), `lt` (<), `lte` (<=), `gt` (>), `gte` (>=).

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in 
the specified columns do not meet defined string length criteria.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO meet defined string length criteria.

All string length metrics are defined in `stringLength` subsection.

### String In Domain Metric

Counts number of values which fall into specified set of allowed values.
Additional parameters should be supplied:

* `domain` - *Required*. List of allowed values.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in 
the specified columns do not fall into set of allowed values.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns ARE IN set of allowed values.

All string in domain metrics are defined in `stringInDomain` subsection.

### String Out Domain Metric

Counts number of values which do **not** fall into specified set of avoided values.
Additional parameters should be supplied:

* `domain` - *Required*. List of avoided values.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns are in set of allowed values.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO NOT fall into set of allowed values.

All string out domain metrics are defined in `stringOutDomain` subsection.

### String Values Metric

Counts number of values that are equal to the value given in metric definition.
Additional parameters should be supplied:

* `compareValue` - *Required*. String value to compare with.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not equal to defined compare value.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO equal to defined compare value.

All string values metrics are defined in `stringValues` subsection.

### Regex Match

Calculates number of values that match the defined regular expression.
Additional parameters should be supplied:

* `regex` - *Required*. Regular expression to match.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not match defined regular expression.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO match defined regular expression.

All regex match metrics are defined in `regexMatch` subsection.

### Regex Mismatch

Calculates number of values that do **not** match the defined regular expression.
Additional parameters should be supplied:

* `regex` - *Required*. Regular expression that values should **not** match.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns DO match defined regular expression.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns do not match defined regular expression.

All regex mismatch metrics are defined in `regexMismatch` subsection.

### Formatted Date Metric

Counts number of values which have the specified datetime format.
Additional parameters can be supplied:

* `dateFormat` - *Optional, default is `yyyy-MM-dd'T'HH:mm:ss.SSSZ`*. Target datetime format. 
  The datetime format must be specified as 
  [Java DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) pattern.
  
> **NOTE** If the specified columns are of type `Timestamp`, it is assumed that they fit any datetime format and, 
> therefore,  metric will return the total number of non-empty cells. 
> Accordingly, the datetime format does not need to be specified.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not conform to defined datetime format.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO conform to defined datetime format.

All formatted date metrics are defined in `formattedDate` subsection.

### Formatted Number Metric

Counts number of values which are numeric and number format satisfy defined number format criteria.
Additional parameters should be supplied:

* `precision` - *Required*. The total number of digits in the value (excluding the decimal separator).
* `scale` - *Required*. Number of decimal digits in the value.
* `compareRule` - *Optional, default is `inbound`*. Number format comparison rule:
    * `inbound` - the value must "fit" into the specified number format: 
      actual precision and scale of the value are less than or equal to given ones.
    * `outbound` - the value must be outside the specified format: 
      actual precision and scale of the value are strictly greater than given ones.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not satisfy defined number format criteria.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO meet defined number format criteria.

All formatted date metrics are defined in `formattedNumber` subsection.

### Minimum Number Metric

Finds minimum number from the values in the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

Metrics is not reversible and increment returns `Failure` status for rows where all values in the specified columns 
cannot be cast to number and, therefore, minimum number cannot be computed.

All minimum number metrics are defined in `minNumber` subsection.

### Maximum Number Metric

Finds maximum number from the values in the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

Metrics is not reversible and increment returns `Failure` status for rows where all values in the specified columns
cannot be cast to number and, therefore, maximum number cannot be computed.

All maximum number metrics are defined in `maxNumber` subsection.

### Sum Number Metric

Finds sum of the values in the specified columns.
Metric definition does not require additional parameters: `params` should not be set.

Metrics is not reversible and increment returns `Failure` status for rows where some values in the specified columns 
cannot be cast to number.

All sum number metrics are defined in `sumNumber` subsection.

### Average Number Metric

Finds average of the values in the specified column.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with only one column.**

Metric is not reversible and metric increment returns `Failure` status for rows where value in the specified column
cannot be cast to number.

All average number metrics are defined in `avgNumber` subsection.

### Standard Deviation Number Metric

Finds standard deviation for the values in the specified column.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with only one column.**

Metric is not reversible and metric increment returns `Failure` status for rows where value in the specified column 
cannot be cast to number.

All average number metrics are defined in `stdNumber` subsection.

### Casted Number Metric

Counts number of values which string value can be converted to a number (double).
Metric definition does not require additional parameters: `params` should not be set.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns cannot be cast to number.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns CAN be cast to number.

All sum number metrics are defined in `castedNumber` subsection.

### Number In Domain Metric

Counts number of values which being cast to number (double) fall into specified set of allowed numbers.
Additional parameters should be supplied:

* `domain` - *Required*. List of allowed numbers.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not fall into set of allowed numbers.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns ARE IN set of allowed numbers.

All number in domain metrics are defined in `numberInDomain` subsection.

### Number Out Domain Metric

Counts number of values which being cast to number (double) do **not** fall into specified set of avoided numbers.
Additional parameters should be supplied:

* `domain` - *Required*. List of avoided numbers.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns are in set of allowed numbers.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns do not fall into set of allowed numbers.

All number out domain metrics are defined in `numberOutDomain` subsection.

### Number Less Than Metric

Counts number of values which being cast to number (double) are less than (or equal to) the specified value.
Additional parameters should be supplied:

* `compareValue` - *Required*. Number to compare with.
* `includeBound` - *Optional, default is `false`*. Specifies whether to include `compareValue` in the range for comparison.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not satisfy the comparison criteria.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO MEET the comparison criteria.

All number less than metrics are defined in `numberLessThan` subsection.

### Number Greater Than Metric

Counts number of values which being cast to number (double) are greater than (or equal to) the specified value.
Additional parameters should be supplied:

* `compareValue` - *Required*. Number to compare with.
* `includeBound` - *Optional, default is `false`*. Specifies whether to include `compareValue` in the range for comparison.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not satisfy the comparison criteria.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO MEET the comparison criteria.

All number greater than metrics are defined in `numberGreaterThan` subsection.

### Number Between Metric

Counts number of values which being cast to number (double) are within the given interval.
Additional parameters should be supplied:

* `lowerCompareValue` - *Required*. The lower bound of the interval.
* `upperCompareValue` - *Required*. The upper bound of the interval.
* `includeBound` - *Optional, default is `false`*. Specifies whether to include interval bounds in the range for comparison.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not satisfy the comparison criteria.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO MEET the comparison criteria.

All number between metrics are defined in `numberBetween` subsection.

### Number Not Between Metric

Counts number of values which being cast to number (double) are outside the given interval.
Additional parameters should be supplied:

* `lowerCompareValue` - *Required*. The lower bound of the interval.
* `upperCompareValue` - *Required*. The upper bound of the interval.
* `includeBound` - *Optional, default is `false`*. Specifies whether to include interval bounds in the range for comparison.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not satisfy the comparison criteria.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO MEET the comparison criteria.

All number between metrics are defined in `numberNotBetween` subsection.

### Number Values Metric

Counts number of values which being cast to number (double) are equal to the number given in metric definition.
Additional parameters should be supplied:

* `compareValue` - *Required*. Number value to compare with.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not equal to defined compare value.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns DO equal to defined compare value.

All number values metrics are defined in `numberValues` subsection.

### Median Value Metric

Calculates median value of the values in the specified column. Metric calculator uses 
[TDigest](https://github.com/isarn/isarn-sketches) library for computation of median value.

**This metric works with only one column.**
Additional parameters can be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of median value.

Metric is not reversible and metric increment returns `Failure` status for rows where value in the specified column
cannot be cast to number.

All median value metrics are defined in `medianValue` subsection.

### First Quantile Metric

Calculates first quantile for the values in the specified column. Metric calculator uses
[TDigest](https://github.com/isarn/isarn-sketches) library for computation of first quantile.

**This metric works with only one column.**
Additional parameters can be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of first quantile value.

Metric is not reversible and metric increment returns `Failure` status for rows where value in the specified column
cannot be cast to number.

All median value metrics are defined in `firstQuantile` subsection.

### Third Quantile Metric

Calculates third quantile for the values in the specified column. Metric calculator uses
[TDigest](https://github.com/isarn/isarn-sketches) library for computation of third quantile.

**This metric works with only one column.**
Additional parameters can be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of third value.

Metric is not reversible and metric increment returns `Failure` status for rows where value in the specified column
cannot be cast to number.

All third value metrics are defined in `thirdQuantile` subsection.

### Get Quantile Metric

Calculates an arbitrary quantile for the values in the specified column. Metric calculator uses
[TDigest](https://github.com/isarn/isarn-sketches) library for computation of quantile.

**This metric works with only one column.**
Additional parameters should be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of quantile value.
* `target` - *Required*. A number in the interval `[0, 1]` corresponding to the quantile that need to be calculated.

Metric is not reversible and metric increment returns `Failure` status for rows where value in the specified column
cannot be cast to number.

All get quantile metrics are defined in `getQuantile` subsection.

### Get Percentile Metric

This metric is inverse of [Get Quantile Metric](#get-quantile-metric). It calculates a percentile value
(quantile in %) which corresponds to the specified number from the set of values in the column.
Metric calculator uses [TDigest](https://github.com/isarn/isarn-sketches) library for computation of percentile value.

**This metric works with only one column.**
Additional parameters should be supplied:

* `accuracyError` - *Optional, default is `0.01`*. Accuracy error for calculation of percentile.
* `target` - *Required*. The number from the set of values in the column, for which percentile
  is determined.

Metric is not reversible and metric increment returns `Failure` status for rows where value in the specified column
cannot be cast to number.

All get percentile metrics are defined in `getPercentile` subsection.

### Column Equality Metric

Calculates the number of rows where values in the specified columns are equal to each other.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with at least two columns.**

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns cannot be cast to string or are not equal
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns cannot be cast to string or ARE equal.

All column equality metrics are defined in `columnEq` subsection.

### Day Distance Metric

Calculates the number of rows where difference between date in two columns expressed in terms of days is less
(strictly less) than the specified threshold value.

**This metric works with exactly two columns.**
Additional parameters should be supplied:

* `threshold` - *Required*. Maximum allowed difference between two dates in days
  (not included in the range for comparison).
* `dateFormat` - *Optional, default is `yyyy-MM-dd'T'HH:mm:ss.SSSZ`*. Target datetime format.
  The datetime format must be specified as
  [Java DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) pattern.

> **NOTE** If the specified columns are of type `Timestamp`, it is assumed that they fit any datetime format and,
> therefore,  metric will return the total number of non-empty cells.
> Accordingly, the datetime format does not need to be specified.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns do not conform to the specified datetime format or when date difference in days 
is greater than or equal to specified threshold.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns do not conform to the specified datetime format or when date difference in days
is lower than specified threshold.

All day distance metrics are defined in `dayDistance` subsection.

### Levenshtein Distance Metric

Calculates number of rows where Levenshtein distance between string values in the provided columns is less than
(strictly less) specified threshold.

**This metric works with exactly two columns.**
Additional parameters should be supplied:

* `threshold` - *Required*. Maximum allowed Levenshtein distance.
* `normalize` - *Optional, default is `false`*. Boolean parameter indicating whether the Levenshtein distance
  should be normalized with respect to the maximum of the two string lengths.

> **IMPORTANT**. If Levenshtein distance is normalized then threshold value must be in range `[0, 1]`.

Metric is reversible. **By default, `reversed` parameter is set to `false`.**
For direct error collection logic, metric increment returns `Failure` status for rows where some values in
the specified columns cannot be cast to string or when Levenshtein distance is greater than or equal to 
specified threshold.
For reversed error collection logic (default one), metric increment returns `Failure` status when some values
in the specified columns cannot be cast to string or when Levenshtein distance is lower than specified threshold.

All levenshtein distance metrics are defined in `levenshteinDistance` subsection.

### CoMoment Metric

Calculates the covariance moment of the values in two columns (co-moment).
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with exactly two columns.**

Metric is not reversible and metric increment returns `Failure` status for rows where some values in the specified 
columns cannot be cast to number.

All co-moment metrics are defined in `coMoment` subsection.

### Covariance Metric

Calculates the covariance of the values in two columns.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with exactly two columns.**

Metric is not reversible and metric increment returns `Failure` status for rows where some values in the specified 
columns cannot be cast to number.

All covariance metrics are defined in `covariance` subsection.

### Covariance Bessel Metric

Calculates the covariance of the values in two columns with the Bessel correction.
Metric definition does not require additional parameters: `params` should not be set.

**This metric works with exactly two columns.**

Metric is not reversible and metric increment returns `Failure` status for rows where some values in the specified 
columns cannot be cast to number.

All covariance metrics are defined in `covarianceBessel` subsection.

### Top N Metric

This is a specific metric that calculates approximate N most frequently occurring values in a column.
The metric calculator uses [Twitter Algebird](https://github.com/twitter/algebird) library,
which implements abstract algebra methods for Scala.

**This metric works with only one column.**

Additional parameters can be supplied:

* `targetNumber` - *Optional, default is `10`*. Number N of values to search.
* `maxCapacity` - *Optional, default is `100`*. Maximum container size for storing top values.

Metric is not reversible and metric increment returns `Failure` status for rows where some values in the specified
columns cannot be cast to string.

All top N metrics are defined in `topN` subsection.

## Composed Metrics

Composed metrics are defined using a formula (arithmetic expression specified in the `formula` field) for their calculation. 
As composed metric are intended for using other metric results to compute a derivative result then, these metrics
can be referenced in the formula by their IDs. 

Formula must be written using [Mustache Template](https://mustache.github.io/mustache.5.html) notation, e.g.:
`{{ metric_1 }} + {{ metic_2 }}`.

There are following operations supported to build arithmetic expressions:

* Basic `+-*/` and exponentiation `^` math operations.
* Grouping using parentheses.
* Mathematical functions of single argument:
  `abs`, `sqrt`, `floor`, `ceil`, `round` (rounds to the closest integer), `ln` (natural logarithm), `lg` (decimal logarithm), `exp`.
* Mathematical functions of two arguments: `max` and `min`.

Thus, composed metrics are defined in the `composed` subsection using following parameters:

* `id` - *Required*. Composed metric ID;
* `description` - *Optional*. Composed metric description.
* `formula` - *Required*. Formula to calculate composed metric
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this metric where each parameter
  is a string in format: `param.name=param.value`.

## Trend Metrics

Trend metrics allows computing statistic over historical metric results and are primarily intended to detect anomalies
in the data. In order to compute statistic over historical metric results it is required to define a lookup metric
which results are pulled from DQ storage and also a time window for which to compute the desired statistic.

Thus, trend metrics are defined `trend` subsection using following set of parameters:

* `id` - *Required*. Trend metric ID;
* `description` - *Optional*. Trend metric description.
* `kind` - *Required*. Kind of statistic to be calculated over historical metric results.
    * Available trend metric kinds are: `avg`, `std`, `min`, `max`, `sum`, `median`, `firstQuartile`, `thirdQuartile`, `quantile`.
* `quantile` - *Required*. **ONLY FOR `quantile` TREND METRIC**. Quantile to compute over historical metric results
  (must be a number in range `[0, 1]`).
* `lookupMetric` - *Required*. Lookup metric ID: metric which results will be pulled from DQ storage.
* `rule` - *Required*. The rule for loading historical metric results from DQ storage. There are two rules supported:
    * `record` - loads specified number of historical metric result records.
    * `datetime` - loads historical metric results for configured datetime window.
* `windowSize` - *Required*. Size of the window for which historical results are loaded:
    * If `rule` is set to `record` then window size is the number of records to retrieve.
    * If `rule` is set to `datetime` then window size is a duration string which should conform to Scala Duration.
* `windowOffset` - *Optional, default is `0` or `0s`*. Set window offset back from current reference date
  (see [Working with Date and Time](../02-general-information/01-WorkingWithDateTime.md) chapter for more details on
  reference date). By default, offset is absent and window start from current reference date (not including it).
    * If `rule` is set to `record` then window offset is the number of records to skip from reference date.
    * If `rule` is set to `datetime` then window offset is a duration string which should conform to Scala Duration.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this metric where each parameter
  is a string in format: `param.name=param.value`.

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
          columns: ["inn"], params: {regex: """^\d{10}$"""}, reversed: true
        }
      ]
      stringInDomain: [
        {
          id: "orc_data_segment_domain", source: "hdfs_orc_source",
          columns: ["segment"], params: {domain: ["FI", "MID", "SME", "INTL", "CIB"]}
          reversed: true
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
    trend: [
      {
        id: "avg_completeness",
        kind: "avg",
        lookupMetric: "orc_data_compl",
        description: "Average completeness of column id for last 14 days"
        rule: "datetime"
        windowSize: "14d"
      }
    ]
  }
}
```