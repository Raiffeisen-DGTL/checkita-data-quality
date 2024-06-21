# Metric Calculator Engines Overview and Benchmarking

Checkita DataQuality contains various data quality metrics which all are computed during single pass over the data.
Still, there are some metrics that are based on exact cardinality evaluation. Computation of this metrics requires
either storing large state in memory (unique records) or shuffling data. First approach can cause OOM errors
and the second one violates statement that data is read only once.

Checkita has to engines for metric computation: RDD-based engine and DF-based engine. Thus, RDD-based engine
uses first approach to calculate cardinality-based metrics: metric calculators store in memory collection of all
unique records. The DF-based engine, on the other hand, groups and shuffles the data to compute cardinality during
latter reduce stage.

There are some additional points that need to be considered when choosing appropriate engine to run data quality job:

* RDD-based engine provide more flexibility when it is required to work closely with row data. Downside of such 
  flexibility is that accessing row elements as Java-objects yields significant serialization/deserialization overhead.
* DF-based engine on the other hand operates using Spark UnsafeRow format which is stored off-heap and, in addition, 
  has some performance leverage from using Tungsten whole-stage code generation. Problem here is that intermediate
  state of the calculator cannot be accessed and operated with.

**Concluding the statements above, we recommend using DF-based engine for batch applications. While streaming 
applications support only RDD-based engine due to states of the calculators have to be merged between the
processing windows and out of the Spark runtime.**


## Benchmarking

Benchmark tests below separated into two parts:

* evaluate all single-pass metrics (except cardinality-based ones)
* evaluate cardinality-based metrics

Along with running Checkita DQ jobs using either RDD-based of DF-based engine, the same metrics were computed 
using PySpark job in order to assess how well Checkita performs metric computation as compared to raw Spark code.

### About Dataset

The New York Taxi dataset was chosen for processing as it contains massive amount of data collected since 2009.

For the purpose of DQ job run the data for 2009 year was selected from following sources:

```bash
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-02.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-03.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-04.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-05.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-06.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-07.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-08.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-09.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-10.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-11.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2009-12.parquet
```

Datasets are stored in Parquet format and are read as is during DQ job processing.

All parquet files has the same schema:

```
root
 |-- vendor_name: string (nullable = true)
 |-- Trip_Pickup_DateTime: string (nullable = true)
 |-- Trip_Dropoff_DateTime: string (nullable = true)
 |-- Passenger_Count: long (nullable = true)
 |-- Trip_Distance: double (nullable = true)
 |-- Start_Lon: double (nullable = true)
 |-- Start_Lat: double (nullable = true)
 |-- Rate_Code: double (nullable = true)
 |-- store_and_forward: double (nullable = true)
 |-- End_Lon: double (nullable = true)
 |-- End_Lat: double (nullable = true)
 |-- Payment_Type: string (nullable = true)
 |-- Fare_Amt: double (nullable = true)
 |-- surcharge: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- Tip_Amt: double (nullable = true)
 |-- Tolls_Amt: double (nullable = true)
 |-- Total_Amt: double (nullable = true)
```

### Spark Configuration

All jobs in these test were run using the same Spark application configuration with following parameters:

```properties
"spark.driver.cores" = 2
"spark.driver.memory" = "4G"
"spark.executor.memory" = "12G"
"spark.executor.cores" = "4"
"spark.executor.instances" = "3"
"spark.shuffle.service.enabled" = "true"
"spark.sql.shuffle.partitions" = "12"
"spark.default.parallelism" = "12"
"spark.dynamicAllocation.enabled" = "false"
"spark.sql.orc.enabled" = "true"
"spark.sql.parquet.compression.codec" = "snappy"
"spark.sql.autoBroadcastJoinThreshold" = -1
"spark.sql.caseSensitive" = "false"
"spark.sql.legacy.timeParserPolicy" = "CORRECTED"
```

> **IMPORTANT** For cardinality-based metric tests the parallelism has been increased to 1000:
> ```properties
> "spark.sql.shuffle.partitions" = "1000"
> "spark.default.parallelism" = "1000"
> ```

### PySpark Job Run

First of all, we tried to compute the data quality metrics directly,
using PySpark API and measured time, that job had taken.

Below is the code for computing single-pass metrics:

```python
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, when, avg, sum as Fsum, min as Fmin, max as Fmax, datediff, \
    length, split, covar_pop, covar_samp, percentile_approx, levenshtein, stddev_pop, count, to_timestamp, upper
    
# spark = <initiate spark session>

ytd = spark.read.parquet('<path to folder with NY Taxi dataset>')

ytd.withColumn(
    "null_pmt_typ", when(col('payment_type').isNull(), lit(1)).otherwise(lit(0))
).withColumn(
    "empty_pmt_typ", when(col('payment_type') == lit(''), lit(1)).otherwise(lit(0))
).withColumn(
    "null_vendor", when(col('vendor_name').isNull(), lit(1)).otherwise(lit(0))
).withColumn(
    "date_1", to_timestamp(col('trip_pickup_datetime'), "yyyy-MM-dd HH-mm-ss")
).withColumn(
    "date_2", to_timestamp(col('trip_dropoff_datetime'), "yyyy-MM-dd HH-mm-ss")
).withColumn(
    "date_fmt_1", when(col('date_1').isNull(), lit(0)).otherwise(lit(1))
).withColumn(
    "date_fmt_2", when(col('date_2').isNull(), lit(0)).otherwise(lit(1))
).withColumn(
    "precision", length(col('fare_amt').cast(StringType())) - lit(1)
).withColumn(
    "scale", length(split(col('fare_amt').cast(StringType()), '\.').getItem(1))
).withColumn(
    "num_fmt_crit", (col('precision') <= lit(3)) & (col('scale') <= lit(2))
).withColumn(
    "num_to_cast", split(col('trip_pickup_datetime'), '-').getItem(0)
).select(
    count(lit(1)).alias('row_count'),
    Fsum(col('null_pmt_typ')).alias('null_vals'),
    Fsum(when(col('empty_pmt_typ') == lit(''), lit(1)).otherwise(lit(0))).alias('empty_vals'),
    (lit(1) - Fsum(col('null_pmt_typ') + col('null_vendor')) / lit(2) / count(lit(1))).alias('completeness'),
    Fmin(length(col('payment_type'))).alias('min_str'),
    Fmax(length(col('payment_type'))).alias('max_str'),
    avg(length(col('payment_type'))).alias('avg_str'),
    Fsum(when(length(col('payment_type').cast(StringType())) == lit(4), lit(1)).otherwise(lit(0))).alias('str_len'),
    Fsum(when(col('vendor_name').isin("VTS", "DDS", "CMT"), lit(1)).otherwise(lit(0))).alias('str_in_dmn'),
    Fsum(when(~col('payment_type').isin("Credit", "Cash"), lit(1)).otherwise(lit(0))).alias('str_out_dmn'),
    Fsum(when(col('payment_type') == lit("No Charge"), lit(1)).otherwise(lit(0))).alias('str_vals'),
    Fsum(when(col('tip_amt').rlike(r"^\d\d+\..+$"), lit(1)).otherwise(lit(0))).alias('rgx_match'),
    Fsum(when(~col('end_lat').rlike(r"^40\..*$"), lit(1)).otherwise(lit(0))).alias('rgx_mismatch'),
    Fsum(col('date_fmt_1') + col('date_fmt_2')).alias('fmt_date'),
    Fsum(when(col('num_fmt_crit'), lit(1)).otherwise(lit(0))).alias('fmt_number'),
    Fmin(col('total_amt')).alias('min_num'),
    Fmax(col('total_amt')).alias('max_num'),
    Fsum(col('total_amt')).alias('sum_num'),
    avg(col('total_amt')).alias('avg_num'),
    stddev_pop(col('total_amt')).alias('std_num'),
    Fsum(when(col('num_to_cast').cast(DoubleType()).isNull(), lit(0)).otherwise(lit(1))).alias('cast_num'),
    Fsum(when(col('passenger_count').isin(1, 2, 3), lit(1)).otherwise(lit(0))).alias('num_in_dmn'),
    Fsum(when(~col('passenger_count').isin(1, 2, 3, 4, 5), lit(1)).otherwise(lit(0))).alias('num_out_dmn'),
    Fsum(when(col('trip_distance') < lit(1.0), lit(1)).otherwise(lit(0))).alias('num_less_than'),
    Fsum(when(col('trip_distance') > lit(30.0), lit(1)).otherwise(lit(0))).alias('num_greater_than'),
    Fsum(when(
        (col('trip_distance') >= lit(5.0)) & (col('trip_distance') <= lit(10.0)), lit(1)
    ).otherwise(lit(0))).alias('num_btwn'),
    Fsum(when(
        (col('trip_distance') <= lit(1.0)) | (col('trip_distance') >= lit(10.0)), lit(1)
    ).otherwise(lit(0))).alias('num_not_btwn'),
    Fsum(when(col('tolls_amt') == lit(0.0), lit(1)).otherwise(lit(0))).alias('num_vals'),
    percentile_approx(col('fare_amt'), 0.5, 1000).alias('median_val'),
    percentile_approx(col('fare_amt'), 0.25, 1000).alias('first_quant_val'),
    percentile_approx(col('fare_amt'), 0.75, 1000).alias('third_quant_val'),
    percentile_approx(col('fare_amt'), 0.9, 1000).alias('get_quant_val'),
    Fsum(when(col('start_lon') == col('end_lon'), lit(1)).otherwise(lit(0))).alias('col_eq'),
    Fsum(when(datediff(col('date_1'), col('date_2')) < lit(1), lit(1)).otherwise(lit(0))).alias('date_dist'),
    Fsum(when(
        levenshtein(upper(col('start_lat').cast(StringType())), upper(col('end_lat').cast(StringType()))) < lit(3), lit(1)
    ).otherwise(lit(0))).alias('lvnst_dist'),
    (covar_pop(col('trip_distance'), col('fare_amt')) * count(lit(1))).alias('comoment'),
    covar_pop(col('trip_distance'), col('fare_amt')).alias('covariance'),
    covar_samp(col('trip_distance'), col('fare_amt')).alias('covarianceBessel')
).show(truncate=False, vertical=True)
```

#### Pyspark Single-Pass Metrics Test Results

Summary:

| Measurement                 | Value      |
|-----------------------------|------------|
| Overall Job Execution Time  | 6.3 min    |
| Total Number of Tasks       | 46         |
| Total time across all tasks | 52 min     |
| Total input size            | 5.3 GiB    |
| Total records               | 170896055  |
| Shuffle size                | 1467.6 KiB |
| Shuffle records             | 46         |

Longest tasks statistics (tasks that actually read all 12 dataset parquet files):

| Attempt | Status  | Executor ID | Duration | GC Time | Input Size / Records | Shuffle Write Size / Records |
|---------|---------|-------------|----------|---------|----------------------|------------------------------|
| 0       | SUCCESS | 1           | 6.3 min  | 12 s    | 498.3 MiB / 15604551 | 127.8 KiB / 1                |
| 0       | SUCCESS | 1           | 5.0 min  | 11 s    | 445.9 MiB / 14092413 | 119.5 KiB / 1                |
| 0       | SUCCESS | 1           | 5.0 min  | 11 s    | 452.3 MiB / 14275339 | 124.1 KiB / 1                |
| 0       | SUCCESS | 1           | 4.8 min  | 11 s    | 420.4 MiB / 13380122 | 117.1 KiB / 1                |
| 0       | SUCCESS | 2           | 4.0 min  | 8 s     | 469.1 MiB / 14796313 | 123.6 KiB / 1                |
| 0       | SUCCESS | 2           | 4.0 min  | 8 s     | 461.3 MiB / 14583404 | 124.1 KiB / 1                |
| 0       | SUCCESS | 2           | 3.9 min  | 8 s     | 456.9 MiB / 14387371 | 118.7 KiB / 1                |
| 0       | SUCCESS | 3           | 3.8 min  | 9 s     | 452.8 MiB / 14294783 | 117.5 KiB / 1                |
| 0       | SUCCESS | 2           | 3.8 min  | 8 s     | 434.7 MiB / 13686520 | 122.4 KiB / 1                |
| 0       | SUCCESS | 3           | 3.7 min  | 8 s     | 449.2 MiB / 14184249 | 123.7 KiB / 1                |
| 0       | SUCCESS | 3           | 3.7 min  | 8 s     | 444.3 MiB / 13984887 | 123.1 KiB / 1                |
| 0       | SUCCESS | 3           | 3.6 min  | 8 s     | 431.4 MiB / 13626103 | 122.5 KiB / 1                |

#### Pyspark Grouping Metrics Test Results

##### Distinct Values Metric

First test for cardinality-based metrics is to calculate number of distinct values.
Below is the PySpark code to calculate number of distinct trip distances:

```python
dist_vals = ytd.groupBy('trip_distance').agg(lit(1)).count()

print(dist_vals)
```

Summary:

| Measurement                 | Value      |
|-----------------------------|------------|
| Overall Job Execution Time  | 58 sec     |
| Total Number of Tasks       | 996        |
| Total time across all tasks | 1.1 min    |
| Total input size            | 228.3 MiB  |
| Total records               | 170896055  |
| Shuffle size                | 1015.7 KiB |
| Shuffle records             | 47860      |

Mapping tasks statistics (tasks that actually scanned all 12 dataset parquet files).
Note that scanning parquet files for distinct column values is extremely fast!

| Attempt | Status  | Executor ID | Duration | GC Time | Input Size / Records | Shuffle Write Size / Records |
|---------|---------|-------------|----------|---------|----------------------|------------------------------|
| 0       | SUCCESS | 2           | 1 s      |         | 18.9 MiB / 15604551  | 83.5 KiB / 3882              |
| 0       | SUCCESS | 1           | 1 s      | 19.0 ms | 18.1 MiB / 14092413  | 102.8 KiB / 5716             |
| 0       | SUCCESS | 1           | 1 s      |         | 18.1 MiB / 14583404  | 83.6 KiB / 3862              |
| 0       | SUCCESS | 1           | 1 s      | 18.0 ms | 18.0 MiB / 14796313  | 82.8 KiB / 3817              |
| 0       | SUCCESS | 3           | 1 s      |         | 17.6 MiB / 14294783  | 82.6 KiB / 3799              |
| 0       | SUCCESS | 3           | 1 s      |         | 17.6 MiB / 14184249  | 82.6 KiB / 3816              |
| 0       | SUCCESS | 3           | 1 s      |         | 17.3 MiB / 14275339  | 82.9 KiB / 3826              |
| 0       | SUCCESS | 3           | 1 s      |         | 17.1 MiB / 14387371  | 82.2 KiB / 3797              |
| 0       | SUCCESS | 2           | 1 s      |         | 17.0 MiB / 13984887  | 83.2 KiB / 3841              |
| 0       | SUCCESS | 1           | 1 s      |         | 17.0 MiB / 13686520  | 83.6 KiB / 3879              |
| 0       | SUCCESS | 2           | 1 s      |         | 16.8 MiB / 13626103  | 82.9 KiB / 3834              |
| 0       | SUCCESS | 3           | 1 s      |         | 16.1 MiB / 13380122  | 83.0 KiB / 3791              |

##### Duplicate Values Metric

Let's calculate duplicated trips. These are trips for which following columns do match:

* `vendor_name`
* `trip_pickup_datetime`
* `trip_dropoff_datetime`
* `start_lon`
* `start_lat`
* `end_lon`
* `end_lat`

PySpark code to calculate number of duplicated trips is following:

```python
dupl_vals = ytd.groupBy(
    'vendor_name', 
    'trip_pickup_datetime', 
    'trip_dropoff_datetime',
    'start_lon',
    'start_lat',
    'end_lon',
    'end_lat'
).agg(
    (count(lit(1)) - lit(1)).alias('cnt')
).select(Fsum(col('cnt'))).collect()[0][0]

print(dupl_vals)
```

Summary:
Note that shuffling operations did spill lots of data to both memory and disk!

| Measurement                 | Value     |
|-----------------------------|-----------|
| Overall Job Execution Time  | 2.5 min   |
| Total Number of Tasks       | 996       |
| Total time across all tasks | 15 min    |
| Total input size            | 4.6 GiB   |
| Total records               | 170896055 |
| Shuffle size                | 7.9 GiB   |
| Shuffle records             | 170861620 |
| **Spill Memory**            | 27.4 GiB  |
| **Spill Disk**              | 6.8 GiB   |

Mapping tasks statistics (tasks that actually scanned all 12 dataset parquet files).

| Attempt | Status  | Executor ID | Duration | GC Time | Input Size / Records | Shuffle Write Size / Records | Spill (Memory) | Spill (Disk) |
|---------|---------|-------------|----------|---------|----------------------|------------------------------|----------------|--------------|
| 0       | SUCCESS | 2           | 1.9 min  | 12 s    | 405.4 MiB / 14796313 | 672.8 MiB / 14793946         | 3.9 GiB        | 1 GiB        |
| 0       | SUCCESS | 2           | 1.9 min  | 12 s    | 393.4 MiB / 14387371 | 655.1 MiB / 14386632         | 3.9 GiB        | 1 GiB        |
| 0       | SUCCESS | 2           | 1.8 min  | 11 s    | 387.8 MiB / 14184249 | 647.2 MiB / 14181699         | 2.6 GiB        | 564.6 MiB    |
| 0       | SUCCESS | 2           | 1.7 min  | 12 s    | 372.1 MiB / 13626103 | 622.2 MiB / 13624190         | 3.7 GiB        | 1002.1 MiB   |
| 0       | SUCCESS | 3           | 1.3 min  | 4 s     | 397.2 MiB / 14583404 | 661.9 MiB / 14575564         | 4.4 GiB        | 1.2 GiB      |
| 0       | SUCCESS | 3           | 1.2 min  | 5 s     | 375.2 MiB / 13686520 | 624.4 MiB / 13684841         | 3.8 GiB        | 997.5 MiB    |
| 0       | SUCCESS | 3           | 1.2 min  | 3 s     | 390.0 MiB / 14275339 | 648.4 MiB / 14270095         | 2.6 GiB        | 565 MiB      |
| 0       | SUCCESS | 3           | 1.1 min  | 3 s     | 383.1 MiB / 14092413 | 641.1 MiB / 14090159         | 2.5 GiB        | 559.5 MiB    |
| 0       | SUCCESS | 1           | 40 s     | 3 s     | 429.6 MiB / 15604551 | 808.8 MiB / 15599852         |                |              |
| 0       | SUCCESS | 1           | 36 s     | 3 s     | 390.2 MiB / 14294783 | 740.1 MiB / 14293543         |                |              |
| 0       | SUCCESS | 1           | 34 s     | 1 s     | 383.3 MiB / 13984887 | 724.5 MiB / 13982756         |                |              |
| 0       | SUCCESS | 1           | 33 s     | 0.8 s   | 362.4 MiB / 13380122 | 692.6 MiB / 13378343         |                |              |

### Checkita RDD-Engine Job Run

#### Checkita RDD-Engine Single-Pass Metrics Test Results

Summary:

| Measurement                 | Value     |
|-----------------------------|-----------|
| Overall Job Execution Time  | 23 min    |
| Total Number of Tasks       | 46        |
| Total time across all tasks | 3.7 hour  |
| Total input size            | 5.3 GiB   |
| Total records               | 170896055 |
| Shuffle size                | 2.1 MiB   |
| Shuffle records             | 46        |

Longest tasks statistics (tasks that actually read all 12 dataset parquet files):

| Attempt | Status  | Executor ID | Duration | GC Time | Input Size / Records | Shuffle Write Size / Records |
|---------|---------|-------------|----------|---------|----------------------|------------------------------|
| 0       | SUCCESS | 1           | 23 min   | 49 s    | 456.2 MiB / 14275339 | 129.4 KiB / 1                |
| 0       | SUCCESS | 1           | 22 min   | 49 s    | 448.0 MiB / 14092413 | 179.4 KiB / 1                |
| 0       | SUCCESS | 1           | 22 min   | 49 s    | 446.7 MiB / 13984887 | 138.4 KiB / 1                |
| 0       | SUCCESS | 1           | 22 min   | 48 s    | 422.9 MiB / 13380122 | 156.5 KiB / 1                |
| 0       | SUCCESS | 2           | 18 min   | 27 s    | 503.1 MiB / 15604551 | 139.6 KiB / 1                |
| 0       | SUCCESS | 2           | 17 min   | 27 s    | 472.4 MiB / 14796313 | 131.4 KiB / 1                |
| 0       | SUCCESS | 2           | 17 min   | 26 s    | 456.0 MiB / 14294783 | 159.3 KiB / 1                |
| 0       | SUCCESS | 3           | 17 min   | 23 s    | 465.0 MiB / 14583404 | 133.3 KiB / 1                |
| 0       | SUCCESS | 3           | 16 min   | 23 s    | 460.2 MiB / 14387371 | 166.2 KiB / 1                |
| 0       | SUCCESS | 3           | 16 min   | 23 s    | 451.7 MiB / 14184249 | 135.0 KiB / 1                |
| 0       | SUCCESS | 2           | 16 min   | 26 s    | 437.0 MiB / 13686520 | 139.9 KiB / 1                |
| 0       | SUCCESS | 3           | 16 min   | 23 s    | 433.6 MiB / 13626103 | 137.6 KiB / 1                |

#### Checkita RDD-Engine Cardinality Metrics Test Results

##### Distinct Values Metric

Summary:

| Measurement                 | Value      |
|-----------------------------|------------|
| Overall Job Execution Time  | 2.1 min    |
| Total Number of Tasks       | 996        |
| Total time across all tasks | 14 min     |
| Total input size            | 5.3 GiB    |
| Total records               | 170896055  |
| Shuffle size                | 1621.7 KiB |
| Shuffle records             | 996        |

Longest tasks statistics (tasks that actually read all 12 dataset parquet files):

| Attempt | Status  | Executor ID | Duration | GC Time | Input Size / Records | Shuffle Write Size / Records |
|---------|---------|-------------|----------|---------|----------------------|------------------------------|
| 0       | SUCCESS | 1           | 1.5 min  | 3 s     | 503.1 MiB / 15604551 | 20.6 KiB / 1                 |
| 0       | SUCCESS | 1           | 1.4 min  | 3 s     | 465 MiB / 14583404   | 20.5 KiB / 1                 |
| 0       | SUCCESS | 1           | 1.3 min  | 3 s     | 446.7 MiB / 13984887 | 20.4 KiB / 1                 |
| 0       | SUCCESS | 1           | 1.3 min  | 3 s     | 448 MiB / 14092413   | 31.2 KiB / 1                 |
| 0       | SUCCESS | 3           | 58 s     | 2 s     | 472.4 MiB / 14796313 | 20.1 KiB / 1                 |
| 0       | SUCCESS | 3           | 57 s     | 2 s     | 456 MiB / 14294783   | 20.1 KiB / 1                 |
| 0       | SUCCESS | 3           | 55 s     | 1.0 s   | 460.2 MiB / 14387371 | 20.1 KiB / 1                 |
| 0       | SUCCESS | 2           | 54 s     | 1 s     | 437 MiB / 13686520   | 20.5 KiB / 1                 |
| 0       | SUCCESS | 2           | 53 s     | 0.7 s   | 451.7 MiB / 14184249 | 20.1 KiB / 1                 |
| 0       | SUCCESS | 2           | 53 s     | 0.6 s   | 456.2 MiB / 14275339 | 20.2 KiB / 1                 |
| 0       | SUCCESS | 2           | 53 s     | 1 s     | 433.6 MiB / 13626103 | 20.3 KiB / 1                 |
| 0       | SUCCESS | 3           | 46 s     | 0.4 s   | 422.9 MiB / 13380122 | 20.3 KiB / 1                 |

##### Duplicate Values Metric

Calculation failed with OOM error. The reason is that RDD-metric calculator for duplicate values must
store all unique values within its state, i.e. within simple Java collection. For cases when number of
unique records is high (in our case its about 170 million records) the executors will fail with OOM error.

### Checkita DF-Engine Job Run

#### Checkita DF-Engine Single-Pass Metrics Test Results

Summary:

| Measurement                 | Value     |
|-----------------------------|-----------|
| Overall Job Execution Time  | 4.1 min   |
| Total Number of Tasks       | 46        |
| Total time across all tasks | 46 min    |
| Total input size            | 5.3 GiB   |
| Total records               | 170896055 |
| Shuffle size                | 4.5 MiB   |
| Shuffle records             | 46        |

Longest tasks statistics (tasks that actually read all 12 dataset parquet files):

| Attempt | Status  | Executor ID | Duration | GC Time | Input Size / Records | Shuffle Write Size / Records |
|---------|---------|-------------|----------|---------|----------------------|------------------------------|
| 0       | SUCCESS | 2           | 4.0 min  | 4 s     | 469.1 MiB / 14796313 | 358.9 KiB / 1                |
| 0       | SUCCESS | 3           | 4.0 min  | 3 s     | 498.3 MiB / 15604551 | 401.7 KiB / 1                |
| 0       | SUCCESS | 3           | 3.9 min  | 2 s     | 456.9 MiB / 14387371 | 381.0 KiB / 1                |
| 0       | SUCCESS | 2           | 3.9 min  | 4 s     | 461.3 MiB / 14583404 | 359.1 KiB / 1                |
| 0       | SUCCESS | 2           | 3.9 min  | 4 s     | 449.2 MiB / 14184249 | 359.4 KiB / 1                |
| 0       | SUCCESS | 1           | 3.8 min  | 4 s     | 445.9 MiB / 14092413 | 415.1 KiB / 1                |
| 0       | SUCCESS | 2           | 3.8 min  | 4 s     | 452.8 MiB / 14294783 | 375.9 KiB / 1                |
| 0       | SUCCESS | 1           | 3.8 min  | 4 s     | 444.3 MiB / 13984887 | 390.0 KiB / 1                |
| 0       | SUCCESS | 1           | 3.7 min  | 4 s     | 452.3 MiB / 14275339 | 396.9 KiB / 1                |
| 0       | SUCCESS | 3           | 3.6 min  | 2 s     | 431.4 MiB / 13626103 | 358.4 KiB / 1                |
| 0       | SUCCESS | 1           | 3.5 min  | 3 s     | 420.4 MiB / 13380122 | 391.2 KiB / 1                |
| 0       | SUCCESS | 3           | 3.4 min  | 2 s     | 434.7 MiB / 13686520 | 361.4 KiB / 1                |

#### Checkita DF-Engine Cardinality Metrics Test Results

##### Distinct Values Metric

Summary:

| Measurement                 | Value     |
|-----------------------------|-----------|
| Overall Job Execution Time  | 1.2 min   |
| Total Number of Tasks       | 996       |
| Total time across all tasks | 4.0 min   |
| Total input size            | 908.2 MiB |
| Total records               | 170896055 |
| Shuffle size                | 199.0 MiB |
| Shuffle records             | 47860     |

Longest tasks statistics (tasks that actually read all 12 dataset parquet files):

| Attempt | Status  | Executor ID | Duration | GC Time | Input Size / Records | Shuffle Write Size / Records |
|---------|---------|-------------|----------|---------|----------------------|------------------------------|
| 0       | SUCCESS | 3           | 20 s     | 3 s     | 81.0 MiB / 15604551  | 17.1 MiB / 3882              |
| 0       | SUCCESS | 2           | 15 s     | 1 s     | 74.7 MiB / 14387371  | 16.5 MiB / 3797              |
| 0       | SUCCESS | 1           | 15 s     | 1 s     | 74.9 MiB / 14294783  | 16.3 MiB / 3799              |
| 0       | SUCCESS | 3           | 14 s     | 1 s     | 77.3 MiB / 14796313  | 16.8 MiB / 3817              |
| 0       | SUCCESS | 2           | 14 s     | 0.5 s   | 74.0 MiB / 14275339  | 16.7 MiB / 3826              |
| 0       | SUCCESS | 3           | 13 s     | 1 s     | 74.4 MiB / 14092413  | 16.9 MiB / 5716              |
| 0       | SUCCESS | 3           | 13 s     | 1 s     | 72.4 MiB / 13984887  | 16.7 MiB / 3841              |
| 0       | SUCCESS | 2           | 12 s     | 0.5 s   | 69.3 MiB / 13380122  | 15.6 MiB / 3791              |
| 0       | SUCCESS | 3           | 12 s     | 1.0 s   | 73.8 MiB / 14184249  | 16.5 MiB / 3816              |
| 0       | SUCCESS | 1           | 12 s     | 0.7 s   | 71.2 MiB / 13686520  | 16.7 MiB / 3879              |
| 0       | SUCCESS | 1           | 12 s     | 0.2 s   | 75.9 MiB / 14583404  | 16.8 MiB / 3862              |
| 0       | SUCCESS | 1           | 12 s     | 0.4 s   | 70.7 MiB / 13626103  | 16.4 MiB / 3834              |

##### Duplicate Values Metric

Summary:
Note that shuffling operations did spill lots of data to both memory and disk!

| Measurement                 | Value     |
|-----------------------------|-----------|
| Overall Job Execution Time  | 3.3 min   |
| Total Number of Tasks       | 996       |
| Total time across all tasks | 28 min    |
| Total input size            | 4.6 GiB   |
| Total records               | 170896055 |
| Shuffle size                | 15.8 GiB  |
| Shuffle records             | 170861620 |
| **Spill Memory**            | 85.6 GiB  |
| **Spill Disk**              | 19.6 GiB  |

Mapping tasks statistics (tasks that actually scanned all 12 dataset parquet files).

| Attempt | Status  | Executor ID | Duration | GC Time | Input Size / Records | Shuffle Write Size / Records | Spill (Memory) | Spill (Disk) |
|---------|---------|-------------|----------|---------|----------------------|------------------------------|----------------|--------------|
| 0       | SUCCESS | 1           | 2.6 min  | 9 s     | 390.2 MiB / 14294783 | 1.3 GiB / 14293543           | 7.7 GiB        | 1.8 GiB      |
| 0       | SUCCESS | 1           | 2.5 min  | 6 s     | 383.1 MiB / 14092413 | 1.3 GiB / 14090159           | 6.3 GiB        | 1.4 GiB      |
| 0       | SUCCESS | 1           | 2.5 min  | 8 s     | 372.1 MiB / 13626103 | 1.3 GiB / 13624190           | 6.0 GiB        | 1.4 GiB      |
| 0       | SUCCESS | 1           | 2.4 min  | 9 s     | 375.2 MiB / 13686520 | 1.3 GiB / 13684841           | 7.6 GiB        | 1.8 GiB      |
| 0       | SUCCESS | 2           | 2.3 min  | 7 s     | 390.0 MiB / 14275339 | 1.3 GiB / 14270095           | 6.4 GiB        | 1.4 GiB      |
| 0       | SUCCESS | 2           | 2.2 min  | 8 s     | 383.3 MiB / 13984887 | 1.3 GiB / 13982756           | 6.3 GiB        | 1.4 GiB      |
| 0       | SUCCESS | 3           | 2.2 min  | 7 s     | 429.6 MiB / 15604551 | 1.4 GiB / 15599852           | 8.0 GiB        | 1.8 GiB      |
| 0       | SUCCESS | 2           | 2.2 min  | 8 s     | 387.8 MiB / 14184249 | 1.3 GiB / 14181699           | 8.3 GiB        | 1.9 GiB      |
| 0       | SUCCESS | 2           | 2.2 min  | 9 s     | 405.4 MiB / 14796313 | 1.4 GiB / 14793946           | 7.8 GiB        | 1.8 GiB      |
| 0       | SUCCESS | 3           | 2.1 min  | 7 s     | 397.2 MiB / 14583404 | 1.3 GiB / 14575564           | 8.4 GiB        | 1.9 GiB      |
| 0       | SUCCESS | 3           | 2.1 min  | 7 s     | 393.4 MiB / 14387371 | 1.3 GiB / 14386632           | 8.2 GiB        | 1.9 GiB      |
| 0       | SUCCESS | 3           | 1.9 min  | 3 s     | 362.4 MiB / 13380122 | 1.2 GiB / 13378343           | 4.7 GiB        | 1015.8 MiB   |


### Overall Summary

Below are some general conclusions that can be made based on benchmark test results.

#### Single-Pass Metrics

The average input data for the task corresponds to a single parquet file and is following:
* Average task input size is ~455 MiB.
* Average task input records are 14241338.

| Test Case      | Job Time | All Tasks Time | Avg Task Time | Avg GC Time | Avg Task Shuffle |
|----------------|----------|----------------|---------------|-------------|------------------|
| PySpark Run    | 6.3 min  | 52 min         | 4.3 min       | 9.2 sec     | 122.0 KiB        |
| RDD-Engine Run | 23 min   | 3.7 hour       | 18.5 min      | 32.8 sec    | 145.5 KiB        |
| DF-Engine Run  | 4.1 min  | 46 min         | 3.8 min       | 3.3 sec     | 379.1 KiB        |

Thus, DF-Engine provides roughly **5 times better performance** for single-pass metrics as compared to RDD-Engine.
This is due to RDD metric calculators require data serialization from DataFrame UnsafeRow format to JVM types
while DF metric calculators run purely on Spark DF-API without any loss for data serialization.

Also, it could be noted Checkita DF-engine shuffled more data as compared to PySpark run. This is caused by the fact
that DF metric calculators also collect row data for metric increment errors.

#### Cardinality-Based Metrics

**Distinct Values Metric Calculation**

Not that computation of distinct values via PySpark code written with use of Spark DF-API as well as
via Checkita DF-Engine took credit for Parquet format specifics: the distinct values were obtained from
metadata of the corresponding column chunks rather than from reading all data line by line.
Checkita RDD-based engine on the other hand, read and processed all data records.

| Test Case      | Job Time | All Tasks Time | Avg Task Time | Avg GC Time | Avg Task Shuffle | Avg. Task Input Size |
|----------------|----------|----------------|---------------|-------------|------------------|----------------------|
| PySpark Run    | 58 sec   | 1.1 min        | 1 sec         | 3.1 ms      | 84.6 KiB         | 17.5 MiB             |
| RDD-Engine Run | 2.1 min  | 14 min         | 63 sec        | 1.7 sec     | 21.2 KiB         | 454.4 MiB            |
| DF-Engine Run  | 1.2 min  | 4.0 min        | 13.7 sec      | 0.9 sec     | 16.6 MiB         | 74.1 MiB             |

Thus, according to the test above, the Checkita DF-engine provided roughly **2 times better performance** for distinct
values calculation as compared to RDD-engine. In addition to that, DF-engine can take benefits of data columnar formats
while RDD-engine will process all the records from dataset.

It also should be noted, that difference in performance between DF-Engine and RDD-engine will grow with the
growth of column values cardinality. For the test above we have intentionally chosen column with low cardinality to
prevent possible OOM errors for RDD-based distinct value metric calculator.

In the test above PySpark code run faster than Checkita DF-engine due to fact that we used the simplest way to
calculate number of distinct values for the particular column while DF-metric calculator has more general and complex
logic and, in addition, collects metric increment errors.

**Distinct Values Metric Calculation**

For calculation of duplicate values we have chosen tuple of columns with high cardinality (~170 million records).
Thus, such RDD-based duplicate values metric calculator couldn't store such amount of unique records within
its state and, therefore, application crushed with OOM error.

| Test Case      | Job Time  | All Tasks Time | Avg Task Time | Avg GC Time | Avg Task Shuffle | Avg. Task Input Size | Avg. Task Spill (Mem) | Avg. Task Spill (Disk) |
|----------------|-----------|----------------|---------------|-------------|------------------|----------------------|-----------------------|------------------------|
| PySpark Run    | 2.5 min   | 15 min         | 1.2 min       | 5.8 sec     | 678.3 MiB        | 389.1 MiB            | 2.3 GiB               | 580.5 MiB              |
| RDD-Engine Run | OOM ERROR | ---            | ---           | ---         | ---              | ---                  | ---                   | ---                    |
| DF-Engine Run  | 3.3 min   | 28 min         | 2.3 min       | 7.3 sec     | 1.3 GiB          | 389.1 MiB            | 7.1 GiB               | 1.6 GiB                |

Note, that PySpark application performed better that Checkita DF-Engine for the same reasons as were given above for
distinct values calculation.

### Conclusions

From the test above it can be concluded that new Checkita DF-engine provides significantly better performance comparing
to RDD-engine. We strongly recommend to use DF-engine for all batch applications.

However, DF metric calculators operate completely within Spark runtime. Currently, it is only possible
to retrieve final metric result after data source is processed. RDD metric calculators on the other hand are more
flexible and can be managed outside of Spark runtime. For that reason our streaming applications supports only
RDD-engine for now as we need to merge calculators from different micro-batches outside the Spark runtime.

There are some plans to enhance DF metric calculators to make them support streaming application as well. :)

### DQ Job Configuration files

##### Single-pass metrics

```hocon
SRC = "ytd"
jobConfig: {
  jobId: "yellow_trip_data"
  jobDescription: "Data Quality job for NY Taxi Data"
  sources: {
    file: [
      {
        id: ${SRC},
        kind: "parquet",
        path: "/datalake/data/workspace/mlcib/ruadlu/yellow_trip_data"
        keyFields: ["vendor_name", "trip_pickup_datetime"]
      }
    ]
  }
  virtualSources: [
    {
      id: ${SRC}"_num_to_cast",
      kind: "select"
      parentSources: [${SRC}]
      expr: [
        "vendor_name",
        "trip_pickup_datetime"
        "split(trip_pickup_datetime, '-' )[0] as num_to_cast"
      ]
      keyFields: ["vendor_name", "trip_pickup_datetime"]
    }
  ]
  metrics: {
    regular: {
      rowCount: [{id: ${SRC}"_row_cnt", source: ${SRC}}]
      approximateDistinctValues: [{
        id: ${SRC}"_apprx_dist_vals", source: ${SRC},
        params: {accuracyError: 0.001}, columns: ["trip_distance"]
      }]
      nullValues: [{id: ${SRC}"_null_vals", source: ${SRC}, columns: ["payment_type"]}]
      emptyValues: [{id: ${SRC}"_empty_vals", source: ${SRC}, columns: ["payment_type"]}]
      completeness: [{id: ${SRC}"_completeness", source: ${SRC}, columns: ["vendor_name", "payment_type"]}]
      minString: [{id: ${SRC}"_min_str", source: ${SRC}, columns: ["payment_type"]}]
      maxString: [{id: ${SRC}"_max_str", source: ${SRC}, columns: ["payment_type"]}]
      avgString: [{id: ${SRC}"_avg_str", source: ${SRC}, columns: ["payment_type"]}]
      stringLength: [{
        id: ${SRC}"_str_len", source: ${SRC}, columns: ["payment_type"],
        params: {length: 4, compareRule: "eq"}
      }]
      stringInDomain: [{
        id: ${SRC}"_str_in_dmn", source: ${SRC}, columns: ["vendor_name"],
        params: {domain: ["VTS", "DDS", "CMT"]}
      }]
      stringOutDomain: [{
        id: ${SRC}"_str_out_dmn", source: ${SRC}, columns: ["payment_type"],
        params: {domain: ["Credit", "Cash"]}
      }]
      stringValues: [{
        id: ${SRC}"_str_vals", source: ${SRC}, columns: ["payment_type"],
        params: {compareValue: "No Charge"}
      }]
      regexMatch: [{
        id: ${SRC}"_rgx_match", source: ${SRC}, columns: ["tip_amt"],
        params: {regex: """^\d\d+\..+$"""}
      }]
      regexMismatch: [{
        id: ${SRC}"_rgx_mismatch", source: ${SRC}, columns: ["end_lat"],
        params: {regex: """^40\..*$"""}
      }]
      formattedDate: [{
        id: ${SRC}"_fmt_date", source: ${SRC},
        columns: ["trip_pickup_datetime", "trip_dropoff_datetime"],
        params: {dateFormat: "yyyy-MM-dd HH-mm-ss"}
      }]
      formattedNumber: [{
        id: ${SRC}"_fmt_number", source: ${SRC}, columns: ["fare_amt"],
        params: {precision: 3, scale: 2, compareRule: "inbound"}
      }]
      minNumber: [{id: ${SRC}"_min_num", source: ${SRC}, columns: ["total_amt"]}]
      maxNumber: [{id: ${SRC}"_max_num", source: ${SRC}, columns: ["total_amt"]}]
      sumNumber: [{id: ${SRC}"_sum_num", source: ${SRC}, columns: ["total_amt"]}]
      avgNumber: [{id: ${SRC}"_avg_num", source: ${SRC}, columns: ["total_amt"]}]
      stdNumber: [{id: ${SRC}"_std_num", source: ${SRC}, columns: ["total_amt"]}]
      castedNumber: [{id: ${SRC}"_cast_num", source: ${SRC}"_num_to_cast", columns: ["num_to_cast"]}]
      numberInDomain: [{
        id: ${SRC}"_num_in_dmn", source: ${SRC}, columns: ["passenger_count"],
        params: {domain: [1, 2, 3]}
      }]
      numberOutDomain: [{
        id: ${SRC}"_num_out_dmn", source: ${SRC}, columns: ["passenger_count"],
        params: {domain: [1, 2, 3, 4, 5]}
      }]
      numberLessThan: [{
        id: ${SRC}"_num_less_than", source: ${SRC}, columns: ["trip_distance"],
        params: {compareValue: 1.0, includeBound: false}
      }]
      numberGreaterThan: [{
        id: ${SRC}"_num_greater_than", source: ${SRC}, columns: ["trip_distance"],
        params: {compareValue: 30.0, includeBound: false}
      }]
      numberBetween: [{
        id: ${SRC}"_num_btwn", source: ${SRC}, columns: ["trip_distance"],
        params: {lowerCompareValue: 5.0, upperCompareValue: 10.0, includeBound: true}
      }]
      numberNotBetween: [{
        id: ${SRC}"_num_not_btwn", source: ${SRC}, columns: ["trip_distance"],
        params: {lowerCompareValue: 1.0, upperCompareValue: 30.0, includeBound: true}
      }]
      numberValues: [{
        id: ${SRC}"_num_vals", source: ${SRC}, columns: ["tolls_amt"],
        params: {compareValue: 0.0}
      }]
      medianValue: [{
        id: ${SRC}"_median_val", source: ${SRC}, columns: ["fare_amt"],
        params: {accuracyError: 0.001}
      }]
      firstQuantile: [{
        id: ${SRC}"_first_quant_val", source: ${SRC}, columns: ["fare_amt"],
        params: {accuracyError: 0.001}
      }]
      thirdQuantile: [{
        id: ${SRC}"_third_quant_val", source: ${SRC}, columns: ["fare_amt"],
        params: {accuracyError: 0.001}
      }]
      getQuantile: [{
        id: ${SRC}"_get_quant_val", source: ${SRC}, columns: ["fare_amt"],
        params: {accuracyError: 0.001, target: 0.9}
      }]
      getPercentile: [{
        id: ${SRC}"_get_percent_val", source: ${SRC}, columns: ["fare_amt"],
        params: {accuracyError: 0.001, target: 50.0}
      }]
      columnEq: [{id: ${SRC}"_col_eq", source: ${SRC}, columns: ["start_lon", "end_lon"]}]
      dayDistance: [{
        id: ${SRC}"_date_dist", source: ${SRC},
        columns: ["trip_pickup_datetime", "trip_dropoff_datetime"],
        params: {threshold: 1, dateFormat: "yyyy-MM-dd HH-mm-ss"}
      }]
      levenshteinDistance: [{
        id: ${SRC}"_lvnst_dist", source: ${SRC}, columns: ["start_lat", "end_lat"],
        params: {threshold: 3, normalize: false}
      }]
      coMoment: [{id: ${SRC}"_comoment", source: ${SRC}, columns: ["trip_distance", "fare_amt"]}]
      covariance: [{id: ${SRC}"_covariance", source: ${SRC}, columns: ["trip_distance", "fare_amt"]}]
      covarianceBessel: [{id: ${SRC}"_covariance_bessel", source: ${SRC}, columns: ["trip_distance", "fare_amt"]}]
    }
  }
  checks: {
    snapshot: {
      equalTo: [
        {id: ${SRC}"_row_cnt_chk", metric: ${SRC}"_row_cnt", threshold: 170896055}
      ]
    }
  }
}
```
##### Distinct values metric

```hocon
SRC = "ytd"
jobConfig: {
  jobId: "yellow_trip_data"
  jobDescription: "Data Quality job for NY Taxi Data"
  sources: {
    file: [
      {
        id: ${SRC},
        kind: "parquet",
        path: "/datalake/data/workspace/mlcib/ruadlu/yellow_trip_data"
        keyFields: ["vendor_name", "trip_pickup_datetime"]
      }
    ]
  }
  metrics: {
    regular: {
      distinctValues: [{id: ${SRC}"_dist_vals", source: ${SRC}, columns: ["trip_distance"]}]
    }
  }
  checks: {
    snapshot: {
      equalTo: [
        {id: ${SRC}"_dist_vals_chk", metric: ${SRC}"_dist_vals", threshold: 6876}
      ]
    }
  }
}
```

##### Duplicate values metric

```hocon
SRC = "ytd"
jobConfig: {
  jobId: "yellow_trip_data"
  jobDescription: "Data Quality job for NY Taxi Data"
  sources: {
    file: [
      {
        id: ${SRC},
        kind: "parquet",
        path: "/datalake/data/workspace/mlcib/ruadlu/yellow_trip_data"
        keyFields: ["vendor_name", "trip_pickup_datetime"]
      }
    ]
  }
  metrics: {
    regular: {
      duplicateValues: [{
        id: ${SRC}"_dupl_vals", source: ${SRC}, 
        columns: [
          "vendor_name", "trip_pickup_datetime", "trip_dropoff_datetime",
          "start_lon", "start_lat", "end_lon", "end_lat"
        ]
      }]
    }
  }
  checks: {
    snapshot: {
      equalTo: [
        {id: ${SRC}"_dupl_vals_chk", metric: ${SRC}"_dupl_vals", threshold: 34435}
      ]
    }
  }
}
```