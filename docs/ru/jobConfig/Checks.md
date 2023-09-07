# Checks

Проверки являются важным этапом в работе фреймворка **Checkita**. Проверки накладываются на результаты расчета метрик
и позволяют идентифицировать проблемы в качестве данных.

Проверки подразделяются на три основные группы:

* **Snapshot**: проверки, основанные на результатах расчета метрик в текущей "джобе".
* **Trend**: проверки, основанные на анализе поведения метрики за определенный промежуток времени.
* **SQL**: проверки с помощью SQL запроса примененного непосредственно к источнику.

> **Tip:** Все проверки возвращают булево значение (Success/Failure).

Пример раздела **checks** в конфигурационном файле показан ниже:

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
        timeWindow: 8,
        thresholdLower: 0.2
        thresholdUpper: 0.4
      }
    ]
    topNRankCheck: [
      {id: "top2_curr_match", metric: "table1_top3_currency", rule: "record", timeWindow: 2, targetNumber: 2, threshold: 0.1}
    ]
  }
  snapshot: {
    differByLT: [
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
    greaterThan: [
      {id: "completeness_check", metric: "orc_data_compl", threshold: 0.99}
    ]
    lessThan: [
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

Базовые проверки между текущим результатом метрики и пороговым значением,
которые может быть выражено константой или другой метрикой.

Поддерживаются следующие **Snapshot** проверки:

* `differByLT` - проверка, что относительная разница между ***двумя метриками*** меньше порогового значения.
* `equalTo` - проверка, что метрика равна заданному значению или другой метрике.
* `lessThan` - проверка, что метрика меньше заданного значения или другой метрики.
* `greaterThan` - проверка, что метрика больше заданного значения или другой метрики.

Необходимые параметры:

* `id` - идентификатор проверки.
* `description [optional]` - описание проверки. Указывать необязательно.
* `metric` - метрика для проверки.
* `threshold` - пороговое значение для сравнения.
* `compareMetric` - метрика для сравнения.

> **ВАЖНО**:
>
> * При задании проверки необходимо одно из двух: либо пороговое значение `threshold`
>   либо метрику для сравнения `compareMetric`.
> * Для проверки `differByLT` нужно указать оба значения: метрику для сравнения и пороговое значение
>   относительной разницы между двумя метриками. Значение проверки рассчитывается по истинности
>   следующего выражения: `| metric - compareMetric | / compareMetric <= threshold`

## Trend Checks

Данный класс проверок позволяет удостовериться, что значение метрики соответствует среднему ее значению
за определенный период в пределах заданного отклонения.

Поддерживаются следующие **Trend** проверки:

* `averageBoundFullCheck`: (1 - threshold) * avgResult <= currentResult <= (1 + threshold) * avg
* `averageBoundUpperCheck`: currentResult <= (1 + threshold) * avgResult
* `averageBoundLowerCheck`: (1 - threshold) * avgResult <= currentResult
* `averageBoundRangeCheck`: (1 - thresholdLower) * avgResult <= currentResult <= (1 + thresholdUpper) * avgResult

Необходимые параметры:

* `id` - идентификатор проверки.
* `description [optional]` - описание проверки. Указывать необязательно.
* `metric` - метрика для проверки.
* `rule` - правило для подсчета среднего значения метрики. Может принимать одно из следующих значений:
    * `record` - подсчитывает среднее значение метрики по последним R записям.
    * `date` - подсчитывает среднее значение метрики за последние R дней.
* `timeWindow` - задает размер окна для подсчета среднего (R записей/дней).
* `startDate [optional]` - Опционально можно указать альтернативную начальную дату, для определения временного
  интервала в формате _"yyyy-MM-dd"_. По умолчанию временной интервал начинает отсчитываться назад на
  R записей/дней от `referenceDateTime` (включая ее).
* `threshold` - допустимое отклонения от среднего в интервале [0, 1].
* `thresholdLower` - только для `averageBoundRangeCheck`. Нижний порог отклонения от среднего в интервале [0, 1].
* `thresholdUpper` - только для `averageBoundRangeCheck`. Верхний порог отклонения от среднего в интервале [0, 1].

### topNRankCheck

Специальная проверка разработанная специально для метрики topN и работающая только с ней.
Эта проверка рассчитывает расстояние Джаккарда между текущим и предыдущими наборами topN метрики и проверяет,
что оно не превышает порогового значения.

> **ВАЖНО**: На данный момент поддерживается проверка только между текущим и предыдущим наборами topN метрики.

Основные параметры для этой проверки такие же как и для других **Trend** проверок, со следующими особенностями:

* `timeWindow` - поддерживается только `timeWindow = 2`.
* `targetNumber` - дополнительный параметр, который задает количество записей R из набора topN метрики (R <= N).
* `threshold` - здесь, это пороговое расстояние Джаккарда в интервале [0, 1].

## SQL Checks

> **ВАЖНО**: Этот тип проверок поддерживается только для источников типа `table`.

Данный тип проверок не зависит от метрик и работает непосредственно с источником данных.
Проверка основана на SQL запросе который должен возвращать число. Соответственно, определены два подтипа **SQL**
проверок:

* `countEqZero` - "Success", если SQL запрос возвращает 0.
* `countNotEqZero` - "Success", если SQL запрос возвращает не 0.

Необходимые параметры:

* `id` - идентификатор проверки.
* `description [optional]` - описание проверки. Указывать необязательно.
* `date [optional]` - дата в формате _"yyyy-MM-dd"_, за которую выполняется проверка, если отличается от `executionDate`.
* `source` - идентификатор источника.
* `query` - SQL запрос к источнику (должен возвращать число).

> Данные проверки предназначены, чтобы выполнять вычисление за счет ресурсов БД источника, а не с помощью ресурсов Spark.