# Metrics SQL Equivalency

The purpose of this chapter is to bring more transparency on how metric calculation is performed by providing an
equivalent SQL queries which can be used to get the same result over the same data.

> **Note**: Spark SQL notation is used to provide equivalent SQL queries.

All SQL queries will be written for test data table show below.

## Sample Table
| textCol1<br/>(text) | textCol2<br/>(text) | textCol3<br/>(text) | numCol1<br/>(double) | rangeCol<br/>(integer) | dateCol1<br/>(text)   | dateCol2<br/>(text)   |
|---------------------|---------------------|---------------------|----------------------|------------------------|-----------------------|-----------------------|
| "monkey"            | ""                  | "4"                 | 5.94                 | 1                      | "2022-01-01"          | "2022-01-01"          |
| "monkey"            | "7.71"              | "3.14"              | 1.72                 | 2                      | "1999-12-31"          | "2000-01-01"          |
| "turtle"            | "2.54"              | "foo"               | null                 | null                   | "2005-03-03"          | ""                    |
| "rabbit"            | "2.16"              | "3.0"               | 5.87                 | 3                      | null                  | "2010-10-18"          |
| "monkey"            | null                | "-25.321"           | 2.54                 | 4                      | "2022-01-01"          | "2022-01-01"          |
| ""                  | "2.54"              | "-25.321"           | 5.94                 | 5                      | "01-12-2012"          | "01-12-2012"          |
| null                | "2.54"              | "[12, 35]"          | 5.94                 | 7                      | "12-05-2005"          | "25-04-2024"          |
| "turtle"            | "7.71"              | "true"              | 1.72                 | 8                      | "2022-01-01 12:31:48" | "2022-01-01 07:12:34" |
| "rabbit"            | "2.16"              | null                | 5.87                 | 9                      | "2001-05-25 09:03:13" | "2001-05-25 09:03:13" |
| "turtle"            | "7.71"              | "4"                 | 1.72                 | 10                     | "2011-04-07 19:48:32" | "2003-09-01 12:11:10" |
| "jaguar"            | "6.85"              | "bar"               | 8.26                 | 12                     | "13:03:51"            | "06:33:07"            |
| "jaguar"            | "6.85"              | "3123dasd"          | 8.26                 | 13                     | "15:16:17"            | "15:16:17"            |

## Row Count Metric

* Metric configuration:
  ```hocon
  rowCount: [{id: "row_count", source: "sample_table"}]
  ```
* Equivalent SQL query:
  ```SQL
  SELECT count(*) FROM sample_table;
  ```
* **Metric Result: 12**

## Distinct Values Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  distinctValues: [{id: "distinct_values", source: "sample_table", columns: ["textCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT COUNT(DISTINCT textCol1) AS distinct_values FROM sample_table;
  ```
* **Metric Result: 5**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  distinctValues: [{id: "distinct_values", source: "sample_table", columns: ["textCol1", "textCol3"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT COUNT(*) AS distinct_values 
  FROM (
    SELECT textCol1, textCol3
    FROM sample_table
    GROUP BY textCol1, textCol3
  )
  WHERE NOT(textCol1 IS NULL AND textCol3 IS NULL);
  ```
  > **Note:** Rows where entire tuple of columns is null are omitted from the results in 
  > the same way is it is done by `COUNT(DISTINCT col_name)` expression.
* **Metric Result: 12**

## Null Values Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  nullValues: [{id: "null_values", source: "sample_table", columns: ["textCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN textCol1 IS NULL THEN 1 ELSE 0 END
  ) AS null_values
  FROM sample_table;
  ```
* **Metric Result: 1**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  nullValues: [{id: "null_values", source: "sample_table", columns: ["textCol1", "textCol3"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN textCol1 IS NULL THEN 1 ELSE 0 END + 
    CASE WHEN textCol3 IS NULL THEN 1 ELSE 0 END
  ) AS null_values
  FROM sample_table;
  ```
  > **Note:** Each cell value of each requested column is considered separately and total number of null values is returned.
* **Metric Result: 2**

## Empty Values Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  emptyValues: [{id: "empty_values", source: "sample_table", columns: ["textCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol1 AS STRING) = '' THEN 1 ELSE 0 END
  ) AS null_values
  FROM sample_table;
  ```
* **Metric Result: 1**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  emptyValues: [{id: "empty_values", source: "sample_table", columns: ["textCol1", "textCol2"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol1 AS STRING) = '' THEN 1 ELSE 0 END +
    CASE WHEN CAST(textCol2 AS STRING) = '' THEN 1 ELSE 0 END
  ) AS null_values
  FROM sample_table;
  ```
* **Metric Result: 2**

## Completeness Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  completeness: [{id: "completeness", source: "sample_table", columns: ["textCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN textCol1 IS NULL THEN 0 ELSE 1 END
  ) / COUNT(1) AS completeness
  FROM sample_table;
  ```
* **Metric Result: 0.91666**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  completeness: [{id: "completeness", source: "sample_table", columns: ["textCol1", "textCol2"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN textCol1 IS NULL THEN 0 ELSE 1 END +
    CASE WHEN textCol2 IS NULL THEN 0 ELSE 1 END
  ) / COUNT(1) / 2 AS completeness
  FROM sample_table;
  ```
* **Metric Result: 0.91666**

> **Note**: If `includeEmptyStrings = true`, then case-when expression for each column changes to:
> `CASE WHEN COALESCE(CAST(<columnName> AS STRING), '') = '' THEN 0 ELSE 1 END` and metric will yield result
> of 0.83333 for the cases above.

## Emptiness Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  emptiness: [{id: "emptiness", source: "sample_table", columns: ["textCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN textCol1 IS NULL THEN 1 ELSE 0 END
  ) / COUNT(1) AS completeness
  FROM sample_table;
  ```
* **Metric Result: 0.08333**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  emptiness: [{id: "emptiness", source: "sample_table", columns: ["textCol1", "textCol2"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN textCol1 IS NULL THEN 1 ELSE 0 END +
    CASE WHEN textCol2 IS NULL THEN 1 ELSE 0 END
  ) / COUNT(1) / 2 AS completeness
  FROM sample_table;
  ```
* **Metric Result: 0.08333**

> **Note**: If `includeEmptyStrings = true`, then case-when expression for each column changes to:
> `CASE WHEN COALESCE(CAST(<columnName> AS STRING), '') = '' THEN 1 ELSE 0 END` and metric will yield result
> of 0.16666 for the cases above.

## Sequence Completeness Metric

**This metric works with only one column.**

* Metric configuration:
  ```hocon
  sequenceCompleteness: [{id: "seqCompleteness", source: "sample_table", columns: ["rangeCol"], params: {increment: 1}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT COUNT(1) / ((MAX(long_num) - MIN(long_num)) / 1 + 1) AS seq_comp -- denominator of 1 === increment of 1
  FROM (
    SELECT CAST(rangeCol AS LONG) AS long_num
    FROM sample_table
    GROUP BY CAST(rangeCol AS LONG)
  ) t
  WHERE long_num IS NOT NULL;
  ```
* **Metric Result: 0.84615**

## Minimum String Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  minString: [{id: "min_string", source: "sample_table", columns: ["textCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT MIN(LENGTH(CAST(textCol1 AS STRING))) AS min_string
  FROM sample_table;
  ```
* **Metric Result: 0.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  minString: [{id: "min_string", source: "sample_table", columns: ["textCol1", "textCol2"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT MIN(LEAST(
    LENGTH(CAST(textCol1 AS STRING)), 
    LENGTH(CAST(textCol2 AS STRING))
  )) AS min_string
  FROM sample_table;
  ```
* **Metric Result: 0.0**

> **Note**: Each cell of each requested column is considered separately and minimum string length is returned.

## Max String Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  maxString: [{id: "max_string", source: "sample_table", columns: ["textCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT MAX(LENGTH(CAST(textCol1 AS STRING))) AS max_string
  FROM sample_table;
  ```
* **Metric Result: 6.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  maxString: [{id: "max_string", source: "sample_table", columns: ["textCol1", "textCol2"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT MAX(GREATEST(
    LENGTH(CAST(textCol1 AS STRING)), 
    LENGTH(CAST(textCol2 AS STRING))
  )) AS max_string
  FROM sample_table;
  ```
* **Metric Result: 6.0**

> **Note**: Each cell of each requested column is considered separately and maximum string length is returned.

## Average String Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  avgString: [{id: "avg_string", source: "sample_table", columns: ["textCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT AVG(LENGTH(CAST(textCol1 AS STRING))) AS avg_string
  FROM sample_table;
  ```
* **Metric Result: 5.4545**

> **Note**: `AVG` function omits null values.

**Multiple column definition:**

* Metric configuration:
  ```hocon
  avgString: [{id: "avg_string", source: "sample_table", columns: ["textCol1", "textCol2"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    COALESCE(LENGTH(CAST(textCol1 AS STRING)), 0) + 
    COALESCE(LENGTH(CAST(textCol2 AS STRING)), 0)
  ) / SUM(
    CASE WHEN CAST(textCol1 AS STRING) IS NULL THEN 0 ELSE 1 END +
    CASE WHEN CAST(textCol2 AS STRING) IS NULL THEN 0 ELSE 1 END
  ) AS avg_string
  FROM sample_table;
  ```
* **Metric Result: 4.5454**

> **Note**: Each cell of each requested column is considered separately and average string length is returned. 
> Cells that become null after casting to string(text) type are omitted.

## String Length Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  stringLength: [{id: "string_length", source: "sample_table", columns: ["textCol2"], params: {length: 4, compareRule: "eq"}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN COALESCE(LENGTH(CAST(textCol2 AS STRING)), 0) = 4 THEN 1 ELSE 0 END) AS string_length
  FROM sample_table;
  ```
* **Metric Result: 10.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  stringLength: [{id: "string_length", source: "sample_table", columns: ["textCol2", "textCol3"], params: {length: 4, compareRule: "eq"}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN COALESCE(LENGTH(CAST(textCol2 AS STRING)), 0) = 4 THEN 1 ELSE 0 END + 
    CASE WHEN COALESCE(LENGTH(CAST(textCol3 AS STRING)), 0) = 4 THEN 1 ELSE 0 END
  ) AS string_length
  FROM sample_table;
  ```
* **Metric Result: 12.0**

## String In Domain Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  stringInDomain: [{id: "string_in_domain", source: "sample_table", columns: ["textCol1"], params: {domain: ["rabbit", "jaguar"]}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol1 AS STRING) IN ('rabbit', 'jaguar') THEN 1 ELSE 0 END) AS string_in_domain
  FROM sample_table;
  ```
* **Metric Result: 4.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  stringInDomain: [{id: "string_in_domain", source: "sample_table", columns: ["textCol1", "textCol2"], params: {domain: ["rabbit", "jaguar", "2.16"]}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol1 AS STRING) IN ('rabbit', 'jaguar', '2.16') THEN 1 ELSE 0 END +
    CASE WHEN CAST(textCol2 AS STRING) IN ('rabbit', 'jaguar', '2.16') THEN 1 ELSE 0 END
  ) AS string_in_domain
  FROM sample_table;
  ```
* **Metric Result: 6.0**

## String Out Domain Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  stringOutDomain: [{id: "string_out_domain", source: "sample_table", columns: ["textCol1"], params: {domain: ["rabbit", "jaguar"]}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol1 AS STRING) NOT IN ('rabbit', 'jaguar') THEN 1 ELSE 0 END) AS string_in_domain
  FROM sample_table;
  ```
* **Metric Result: 7.0**

> **Note**: Null values are omitted. Thus, only 7 not null values are out of provided domain.

**Multiple column definition:**

* Metric configuration:
  ```hocon
  stringOutDomain: [{id: "string_out_domain", source: "sample_table", columns: ["textCol1", "textCol2"], params: {domain: ["rabbit", "jaguar", "2.16"]}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol1 AS STRING) NOT IN ('rabbit', 'jaguar', '2.16') THEN 1 ELSE 0 END +
    CASE WHEN CAST(textCol2 AS STRING) NOT IN ('rabbit', 'jaguar', '2.16') THEN 1 ELSE 0 END
  ) AS string_in_domain
  FROM sample_table;
  ```
* **Metric Result: 16.0**

> **Note**: Null values are omitted. Thus, only 16 not null values are out of provided domain.

## String Values Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  stringValues: [{id: "string_values", source: "sample_table", columns: ["dateCol1"], params: {compareValue: "2022-01-01"}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(dateCol1 AS STRING) = '2022-01-01' THEN 1 ELSE 0 END) AS string_values
  FROM sample_table;
  ```
* **Metric Result: 2.0**

> **Note**: Null values are omitted. Thus, only 7 not null values are out of provided domain.

**Multiple column definition:**

* Metric configuration:
  ```hocon
  stringValues: [{id: "string_values", source: "sample_table", columns: ["dateCol1", "dateCol2"], params: {compareValue: "2022-01-01"}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(dateCol1 AS STRING) = '2022-01-01' THEN 1 ELSE 0 END +
    CASE WHEN CAST(dateCol2 AS STRING) = '2022-01-01' THEN 1 ELSE 0 END
  ) AS string_values
  FROM sample_table;
  ```
* **Metric Result: 4.0**

## Regex Match Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  regexMatch: [{id: "regex_match", source: "sample_table", columns: ["textCol2"], params: {regex: """^2\.[0-9]+$"""}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN RLIKE(CAST(textCol2 AS STRING), '^2\.[0-9]+$') THEN 1 ELSE 0 END) AS regex_match
  FROM sample_table;
  ```
* **Metric Result: 5.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  regexMatch: [{id: "regex_match", source: "sample_table", columns: ["textCol2", "numCol1"], params: {regex: """^2\.[0-9]+$"""}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN RLIKE(CAST(textCol2 AS STRING), '^2\.[0-9]+$') THEN 1 ELSE 0 END +
    CASE WHEN RLIKE(CAST(numCol1 AS STRING), '^2\.[0-9]+$') THEN 1 ELSE 0 END
  ) AS regex_match
  FROM sample_table;
  ```
* **Metric Result: 6.0**

## Regex Mismatch Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  regexMismatch: [{id: "regex_mismatch", source: "sample_table", columns: ["textCol2"], params: {regex: """^2\.[0-9]+$"""}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN NOT RLIKE(CAST(textCol2 AS STRING), '^2\.[0-9]+$') THEN 1 ELSE 0 END) AS regex_mismatch
  FROM sample_table;
  ```
* **Metric Result: 6.0**

> **Note**: Null values are omitted. Thus, only 6 not null values mismatched provided regex expression.

**Multiple column definition:**

* Metric configuration:
  ```hocon
  regexMismatch: [{id: "regex_mismatch", source: "sample_table", columns: ["textCol2", "numCol1"], params: {regex: """^2\.[0-9]+$"""}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN NOT RLIKE(CAST(textCol2 AS STRING), '^2\.[0-9]+$') THEN 1 ELSE 0 END +
    CASE WHEN NOT RLIKE(CAST(numCol1 AS STRING), '^2\.[0-9]+$') THEN 1 ELSE 0 END
  ) AS regex_mismatch
  FROM sample_table;
  ```
* **Metric Result: 16.0**

> **Note**: Null values are omitted. Thus, only 16 not null values mismatched provided regex expression.

## Formatted Date Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  formattedDate: [{id: "formatted_date", source: "sample_table", columns: ["dateCol1"], params: {dateFormat: "yyyy-MM-dd"}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN to_timestamp(dateCol1, 'yyyy-MM-dd') IS NULL THEN 0 ELSE 1 END) AS formatted_date
  FROM sample_table;
  ```
* **Metric Result: 4.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  formattedDate: [{id: "formatted_date", source: "sample_table", columns: ["dateCol1", "dateCol2"], params: {dateFormat: "yyyy-MM-dd"}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN to_timestamp(dateCol1, 'yyyy-MM-dd') IS NULL THEN 0 ELSE 1 END +
    CASE WHEN to_timestamp(dateCol2, 'yyyy-MM-dd') IS NULL THEN 0 ELSE 1 END
  ) AS formatted_date
  FROM sample_table;
  ```
* **Metric Result: 8.0**

## Formatted Number Metric

Formatted number metric does not have SQL equivalent since it works directly with Java BigDecimal type:
metric calculator attempts to convert cell value to Java BigDecimal value. If conversion is successful, 
then big decimal precision and scale are checked to meet bounds provided in metric definition.
If requirement is met then calculator is incremented by 1. For all other situation calculator is not incremented.

## Minimum Number Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  minNumber: [{id: "min_number", source: "sample_table", columns: ["numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT MIN(CAST(numCol1 AS DOUBLE)) AS min_number
  FROM sample_table;
  ```
* **Metric Result: 1.72**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  minNumber: [{id: "min_number", source: "sample_table", columns: ["textCol2", "numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT MIN(LEAST(CAST(textCol2 AS DOUBLE), CAST(numCol1 AS DOUBLE))) AS min_number
  FROM sample_table;
  ```
* **Metric Result: 1.72**

## Maximum Number Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  maxNumber: [{id: "max_number", source: "sample_table", columns: ["numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT MAX(CAST(numCol1 AS DOUBLE)) AS max_number
  FROM sample_table;
  ```
* **Metric Result: 8.26**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  maxNumber: [{id: "max_number", source: "sample_table", columns: ["textCol2", "numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT MAX(GREATEST(CAST(textCol2 AS DOUBLE), CAST(numCol1 AS DOUBLE))) AS max_number
  FROM sample_table;
  ```
* **Metric Result: 8.26**

## Sum Number Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  sumNumber: [{id: "sum_number", source: "sample_table", columns: ["numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CAST(numCol1 AS DOUBLE)) AS sum_number
  FROM sample_table;
  ```
* **Metric Result: 53.78**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  sumNumber: [{id: "sum_number", source: "sample_table", columns: ["textCol2", "numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    COALESCE(CAST(textCol2 AS DOUBLE), 0) + 
    COALESCE(CAST(numCol1 AS DOUBLE), 0)
  ) AS sum_number
  FROM sample_table;
  ```
* **Metric Result: 102.55**

## Avg Number Metric

**This metric works with only one column.**

* Metric configuration:
  ```hocon
  avgNumber: [{id: "avg_number", source: "sample_table", columns: ["numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT AVG(CAST(numCol1 AS DOUBLE)) AS avg_number
  FROM sample_table;
  ```
* **Metric Result: 4.889**

## Std Number Metric

**This metric works with only one column.**

* Metric configuration:
  ```hocon
  stdNumber: [{id: "std_number", source: "sample_table", columns: ["numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT STDDEV_POP(CAST(numCol1 AS DOUBLE)) AS std_number
  FROM sample_table;
  ```
* **Metric Result: 2.40475**

## Casted Number Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  castedNumber: [{id: "casted_number", source: "sample_table", columns: ["textCol3"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol3 AS DOUBLE) IS NULL THEN 0 ELSE 1 END) AS casted_number
  FROM sample_table;
  ```
* **Metric Result: 6.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  castedNumber: [{id: "casted_number", source: "sample_table", columns: ["textCol2", "textCol3"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol2 AS DOUBLE) IS NULL THEN 0 ELSE 1 END +
    CASE WHEN CAST(textCol3 AS DOUBLE) IS NULL THEN 0 ELSE 1 END
  ) AS casted_number
  FROM sample_table;
  ```
* **Metric Result: 16.0**

## Number In Domain Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  numberInDomain: [{id: "number_in_domain", source: "sample_table", columns: ["textCol2"], params: {domain: [7.71, 6.85]}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol2 AS DOUBLE) IN (7.71, 6.85) THEN 1 ELSE 0 END) AS number_in_domain
  FROM sample_table;
  ```
* **Metric Result: 5.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  numberInDomain: [{id: "number_in_domain", source: "sample_table", columns: ["textCol2", "textCol3"], params: {domain: [7.71, 6.85, -25.321]}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol2 AS DOUBLE) IN (7.71, 6.85, -25.321) THEN 1 ELSE 0 END + 
    CASE WHEN CAST(textCol3 AS DOUBLE) IN (7.71, 6.85, -25.321) THEN 1 ELSE 0 END
  ) AS number_in_domain
  FROM sample_table;
  ```
* **Metric Result: 7.0**

## Number Out Domain Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  numberOutDomain: [{id: "number_out_domain", source: "sample_table", columns: ["textCol2"], params: {domain: [7.71, 6.85]}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol2 AS DOUBLE) NOT IN (7.71, 6.85) THEN 1 ELSE 0 END) AS number_out_domain
  FROM sample_table;
  ```
* **Metric Result: 5.0**

> **Note**: Null values are omitted. Thus, only 5 not null values are out of provided domain.

**Multiple column definition:**

* Metric configuration:
  ```hocon
  numberOutDomain: [{id: "number_out_domain", source: "sample_table", columns: ["textCol2", "textCol3"], params: {domain: [7.71, 6.85, -25.321]}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol2 AS DOUBLE) NOT IN (7.71, 6.85, -25.321) THEN 1 ELSE 0 END + 
    CASE WHEN CAST(textCol3 AS DOUBLE) NOT IN (7.71, 6.85, -25.321) THEN 1 ELSE 0 END
  ) AS number_out_domain
  FROM sample_table;
  ```
* **Metric Result: 9.0**

> **Note**: Null values are omitted. Thus, only 9 not null values are out of provided domain.

## Number Less Than Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  numberLessThan: [{id: "number_less_than", source: "sample_table", columns: ["textCol2"], params: {compareValue: 4.0, includeBound: false}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol2 AS DOUBLE) < 4.0 THEN 1 ELSE 0 END) AS number_less_than
  FROM sample_table;
  ```
* **Metric Result: 5.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  numberLessThan: [{id: "number_less_than", source: "sample_table", columns: ["textCol2", "numCol1"], params: {compareValue: 4.0, includeBound: false}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol2 AS DOUBLE) < 4.0 THEN 1 ELSE 0 END +
    CASE WHEN CAST(numCol1 AS DOUBLE) < 4.0 THEN 1 ELSE 0 END
  ) AS number_less_than
  FROM sample_table;
  ```
* **Metric Result: 9.0**

## Number Greater Than Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  numberGreaterThan: [{id: "number_greater_than", source: "sample_table", columns: ["textCol2"], params: {compareValue: 4.0, includeBound: false}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol2 AS DOUBLE) > 4.0 THEN 1 ELSE 0 END) AS number_greater_than
  FROM sample_table;
  ```
* **Metric Result: 5.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  numberGreaterThan: [{id: "number_greater_than", source: "sample_table", columns: ["textCol2", "numCol1"], params: {compareValue: 4.0, includeBound: false}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol2 AS DOUBLE) > 4.0 THEN 1 ELSE 0 END +
    CASE WHEN CAST(numCol1 AS DOUBLE) > 4.0 THEN 1 ELSE 0 END
  ) AS number_greater_than
  FROM sample_table;
  ```
* **Metric Result: 12.0**

## Number Between Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  numberBetween: [{id: "number_between", source: "sample_table", columns: ["textCol2"], params: {lowerCompareValue: 3.0, upperCompareValue: 7.0, includeBound: true}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol2 AS DOUBLE) >= 3.0 AND CAST(textCol2 AS DOUBLE) <= 7.0 THEN 1 ELSE 0 END) AS number_between
  FROM sample_table;
  ```
* **Metric Result: 2.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  numberBetween: [{id: "number_between", source: "sample_table", columns: ["textCol2", "numCol1"], params: {lowerCompareValue: 3.0, upperCompareValue: 7.0, includeBound: true}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol2 AS DOUBLE) >= 3.0 AND CAST(textCol2 AS DOUBLE) <= 7.0 THEN 1 ELSE 0 END +
    CASE WHEN CAST(numCol1 AS DOUBLE) >= 3.0 AND CAST(numCol1 AS DOUBLE) <= 7.0 THEN 1 ELSE 0 END
  ) AS number_between
  FROM sample_table;
  ```
* **Metric Result: 7.0**

## Number Not Between Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  numberNotBetween: [{id: "number_not_between", source: "sample_table", columns: ["textCol2"], params: {lowerCompareValue: 3.0, upperCompareValue: 7.0, includeBound: true}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol2 AS DOUBLE) <= 3.0 OR CAST(textCol2 AS DOUBLE) >= 7.0 THEN 1 ELSE 0 END) AS number_between
  FROM sample_table;
  ```
* **Metric Result: 8.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  numberNotBetween: [{id: "number_not_between", source: "sample_table", columns: ["textCol2", "numCol1"], params: {lowerCompareValue: 3.0, upperCompareValue: 7.0, includeBound: true}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol2 AS DOUBLE) <= 3.0 OR CAST(textCol2 AS DOUBLE) >= 7.0 THEN 1 ELSE 0 END +
    CASE WHEN CAST(numCol1 AS DOUBLE) <= 3.0 OR CAST(numCol1 AS DOUBLE) >= 7.0 THEN 1 ELSE 0 END
  ) AS number_between
  FROM sample_table;
  ```
* **Metric Result: 14.0**

## Number Values Metric

**Single column definition:**

* Metric configuration:
  ```hocon
  numberValues: [{id: "number_values", source: "sample_table", columns: ["textCol2"], params: {compareValue: 7.71}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN CAST(textCol2 AS DOUBLE) = 7.71 THEN 1 ELSE 0 END) AS number_between
  FROM sample_table;
  ```
* **Metric Result: 3.0**

**Multiple column definition:**

* Metric configuration:
  ```hocon
  numberValues: [{id: "number_values", source: "sample_table", columns: ["textCol2", "numCol1"], params: {compareValue: 7.71}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(textCol2 AS DOUBLE) = 7.71 THEN 1 ELSE 0 END +
    CASE WHEN CAST(numCol1 AS DOUBLE) = 7.71 THEN 1 ELSE 0 END
  ) AS number_between
  FROM sample_table;
  ```
* **Metric Result: 3.0**

## T-Digest Metrics

All metrics computing rank-based statistics implement T-Digest algorithm under the hood, to compute
their values during single-pass over the data. The resultant metric value is not precise and has some error,
which is controlled by `accuracyError` parameter. Equivalent SQL query utilize functions available in Spark SQL
for precise computing of corresponding rank-based statistic. In all T-Digest metrics, null values are omitted from
computation of metric result.

> **Note** T-Digest yields high error for small collections of numbers and provides quite accurate results for large ones.

**All T-Digest metrics work with only one column.**

### Median Value Metric

* Metric configuration:
  ```hocon
  medianValue: [{id: "median_value", source: "sample_table", columns: ["numCol1"], params: {accuracyError: 0.001}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT MEDIAN(CAST(numCol1 AS DOUBLE)) AS median_value
  FROM sample_table;
  ```
* **Metric Result: 5.87 (both metric result using T-Digest and exact calculation)**

### First Quantile Metric

* Metric configuration:
  ```hocon
  firstQuantile: [{id: "first_quantile", source: "sample_table", columns: ["numCol1"], params: {accuracyError: 0.001}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT PERCENTILE(CAST(numCol1 AS DOUBLE), 0.25) AS first_quantile
  FROM sample_table;
  ```
* **Metric Result: 2.36 (metric result using T-Digest); 2.13 (exact calculation)**

### Third Quantile Metric

* Metric configuration:
  ```hocon
  thirdQuantile: [{id: "third_quantile", source: "sample_table", columns: ["numCol1"], params: {accuracyError: 0.001}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT PERCENTILE(CAST(numCol1 AS DOUBLE), 0.75) AS first_quantile
  FROM sample_table;
  ```
* **Metric Result: 5.94 (both metric result using T-Digest and exact calculation)**

### Get Quantile Metric

* Metric configuration:
  ```hocon
  getQuantile: [{id: "get_quantile", source: "sample_table", columns: ["numCol1"], params: {accuracyError: 0.001, target: 0.85}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT PERCENTILE(CAST(numCol1 AS DOUBLE), 0.85) AS get_quantile
  FROM sample_table;
  ```
* **Metric Result: 7.166 (metric result using T-Digest); 7.1 (exact calculation)**

### Get Percentile Metric

There is no equivalent Spark SQL function to calculate percentile value for provided number from collection.
But the general process remains the same: value is cast to number and omitted from computation if casting yields null.
After that T-Digest calculator is incremented with resultant numeric value and, after all values are consumed, the final
result of the calculator is returned.

## Column Equality Metric

**This metric works with at least two columns.**

* Metric configuration:
  ```hocon
  columnEq: [{id: "column_eq", source: "sample_table", columns: ["dateCol1", "dateCol2"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(dateCol1 AS STRING) = CAST(dateCol2 AS STRING) THEN 1 ELSE 0 END
  ) AS column_eq
  FROM sample_table;
  ```
* **Metric Result: 5.0**

## Column Equality Metric

**This metric works with at least two columns.**

* Metric configuration:
  ```hocon
  columnEq: [{id: "column_eq", source: "sample_table", columns: ["dateCol1", "dateCol2"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN CAST(dateCol1 AS STRING) = CAST(dateCol2 AS STRING) THEN 1 ELSE 0 END
  ) AS column_eq
  FROM sample_table;
  ```
* **Metric Result: 5.0**

## Day Distance Metric

**This metric works with exactly two columns.**

* Metric configuration:
  ```hocon
  dayDistance: [{id: "day_distance", source: "sample_table", columns: ["dateCol1", "dateCol2"], params: {threshold: 3, dateFormat: "yyyy-MM-dd"}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(CASE WHEN COALESCE(
    ABS(DATE_DIFF(
      TO_TIMESTAMP(dateCol1, 'yyyy-MM-dd'), 
      TO_TIMESTAMP(dateCol2, 'yyyy-MM-dd')
    )), 2147483647) < 3 THEN 1 ELSE 0 END  -- use integer max value if day distance is null
  ) AS day_distance
  FROM sample_table;
  ```
* **Metric Result: 3.0**

## Levenshtein Distance Metric

**This metric works with exactly two columns.**

* Metric configuration:
  ```hocon
  levenshteinDistance: [{id: "levenshtein_distance", source: "sample_table", columns: ["textCol2", "numCol1"], params: {threshold: 3, normalize: false}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT SUM(
    CASE WHEN LEVENSHTEIN(CAST(textCol2 AS STRING), CAST(numCol1 AS STRING)) < 3 THEN 1 ELSE 0 END
  ) AS levenshtein_distance
  FROM sample_table;
  ```
* **Metric Result: 5.0**

## CoMoment Metric

**This metric works with exactly two columns.**

* Metric configuration:
  ```hocon
  coMoment: [{id: "co_moment", source: "sample_table", columns: ["textCol2", "numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT COVAR_POP(CAST(textCol2 AS DOUBLE), CAST(numCol1 AS DOUBLE)) * SUM(
    CASE WHEN CAST(textCol2 AS DOUBLE) IS NULL OR CAST(numCol1 AS DOUBLE) IS NULL THEN 0 ELSE 1 END
  ) AS co_moment
  FROM sample_table;
  ```
* **Metric Result: -24.2118**

## Covariance Metric

**This metric works with exactly two columns.**

* Metric configuration:
  ```hocon
  covariance: [{id: "covariance", source: "sample_table", columns: ["textCol2", "numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT COVAR_POP(CAST(textCol2 AS DOUBLE), CAST(numCol1 AS DOUBLE)) AS covariance
  FROM sample_table;
  ```
* **Metric Result: -2.69**

## Covariance Bessel Metric

**This metric works with exactly two columns.**

* Metric configuration:
  ```hocon
  covarianceBessel: [{id: "covariance_bessel", source: "sample_table", columns: ["textCol2", "numCol1"]}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT COVAR_SAMP(CAST(textCol2 AS DOUBLE), CAST(numCol1 AS DOUBLE)) AS covariance_bessel
  FROM sample_table;
  ```
* **Metric Result: -3.026**

## Top N Metric

This metric calculate the N most frequently occurring values in a column and corresponding occurrence frequency.
All calculations are done approximately using [Twitter Algebird](https://github.com/twitter/algebird) library.
This, there is no equivalent SQL query for this metric calculator. 

Nevertheless, below SQL query shows an example of exact top-N values and their occurrence frequency calculation.

**This metric works with only one column.**

* Metric configuration:
  ```hocon
  covarianceBessel: [{id: "covariance_bessel", source: "sample_table", columns: ["textCol1"], params: {targetNumber: 2, maxCapacity: 10}}]
  ```
* Equivalent SQL Query:
  ```sql
  SELECT value, num_occurrences / row_cnt AS freq
  FROM (
    SELECT CAST(textCol1 AS STRING) AS value,
           COUNT(1) AS num_occurrences
    FROM sample_table
    WHERE CAST(textCol1 AS STRING) IS NOT NULL
    GROUP BY CAST(textCol1 AS STRING)
  ) t1 CROSS JOIN (
    SELECT COUNT(1) AS row_cnt
    FROM sample_table
    WHERE CAST(textCol1 AS STRING) IS NOT NULL
  ) t2
  ORDER BY num_occurrences DESC
  LIMIT 2;
  ```
* **Metric Result: `[(turtle, 0.2727), (monkey, 0.2727)]`**