# Хранилище Результатов

Для того чтобы использовать все возможности фреймворка, необходимо создать и настроить хранилище результатов.
Checkita фреймворк может работать с различными RDBMS для хранения результатов. Помимо этого, Hive может 
быть использован как хранилище результатов, так же как и обычное файловое хранилище.

Полный список различных типов хранилища результатов дан ниже:

* `PostgreSQL` (v.9.3 и выше) - рекомендуется для использования в качестве хранилища результатов.
* `Oracle`
* `MySQL`
* `Microsoft SQL Server`
* `SQLite`
* `H2`
* `Hive`
* `File` (директория в локальной файловой системе или удаленной (HDFS, S3))

Checkita фреймворк поддерживает эволюцию схемы хранилища результатов. Для выполнения миграций используется
[Flyway](https://flywaydb.org/). Таким образом, если в качестве хранилища результатов выбрана одна из поддерживаемых 
RDBMS, то возможно провести настройку его схемы при первом запуске Data Quality пайплайна, указав аргумент `-m` при старте.
Подробнее о том, как запускать приложения Checkita, см. главу [Запуск Приложений Data Quality](02-ApplicationSubmit.md).

> **ВАЖНО**: Миграции Flyway обычно запускаются либо в пустой базе/схеме, либо в той, которая уже была проинициализирована
> с помощью Flyway. В Checkita фреймворке также можно запускать миграции в непустой базе/схеме. В этом случае, 
> пользователю необходимо убедиться, что в базе/схеме нет конфликтующих имен таблиц.

Если выбран `File` тип хранилища результатов, то достаточно предоставить путь до директории/бакета, где будут храниться
результаты. Результаты сохраняются как `.parquet` файлы с такой же схемой, как и в случае хранения их в RDBMS.
Для файлового хранилища результатов не предусмотрены механизмы эволюции схемы. Поэтому, если структура результатов
изменится в будущем, то пользователю будет необходимо самостоятельно обновить схемы в существующих результатах.

> **ВАЖНО**: При использовании файлового хранилища, результаты не партиционируются ни по одному из полей. Таким образом,
> каждый Data Quality пайплайн при сохранении результатов будет читать файлы целиком и их перезаписывать.
> Ввиду этих особенностей, использование этого типа хранилища в продуктовой среде не рекомендуется.

Для `Hive` типа хранилища результатов, механизмы эволюции схемы также недоступны. Поэтому пользователю необходимо
самостоятельно создать необходимые Hive-таблицы. DDL скрипты из главы 
[Скрипты для Настройки Хранилища Результатов в Hive](#hive).

> **ВАЖНО**: Результаты должны быть позиционированы по `job_id`. Идентификатор пайплайна выбран как колонка
> позиционирования для того, чтобы обеспечить более быстрое получение результатов во время расчетов трендовых проверок
> (используются для обнаружения аномалий в данных). `Hive` хранилище результатов работает быстрее, чем `File` хранилище,
> т.к. только партиция, которая соответствует идентификатору текущего пайплайна читается и перезаписывается.
> Тем не менее, этот тип хранилища также не рекомендуется для использования в продуктовых средах, где будет запускаться
> большое количество пайплайнов.

## Типы и Схемы Результатов

Checkita фреймворк записывает четыре типа результатов:

* Результаты расчета регулярных метрик;
* Результаты расчета композиционных метрик;
* Результаты загрузочных проверок (проверки метаданных источника перед его загрузкой);
* Результаты основных проверок (проверки над вычисленными метриками).

Схемы для всех типов результатов представлены ниже:

### Regular Metrics Results Schema

* Первичный ключ: `(job_id, metric_id, reference_date)`
* `source_id` & `column_names` содержат строковое представление списков в формате: `'[val1,val2,val3]'`.
* `params` представляет собой JSON строку.

| Column Name       | Column Type | Constraint |
|-------------------|-------------|------------|
| job_id            | STRING      | NOT NULL   |
| metric_id         | STRING      | NOT NULL   |
| metric_name       | STRING      | NOT NULL   |
| description       | STRING      |            |
| source_id         | STRING      | NOT NULL   |
| column_names      | STRING      |            |
| params            | STRING      |            |
| result            | DOUBLE      | NOT NULL   |
| additional_result | STRING      |            |
| reference_date    | TIMESTAMP   | NOT NULL   |
| execution_date    | TIMESTAMP   | NOT NULL   |

### Composed Metrics Results Schema

* Первичный ключ: `(job_id, metric_id, reference_date)`
* `source_id` содержит строковое представление списка в формате: `'[val1,val2,val3]'`.

| Column Name       | Column Type | Constraint |
|-------------------|-------------|------------|
| job_id            | STRING      | NOT NULL   |
| metric_id         | STRING      | NOT NULL   |
| metric_name       | STRING      | NOT NULL   |
| description       | STRING      |            |
| source_id         | STRING      | NOT NULL   |
| formula           | STRING      | NOT NULL   |
| result            | DOUBLE      | NOT NULL   |
| additional_result | STRING      |            |
| reference_date    | TIMESTAMP   | NOT NULL   |
| execution_date    | TIMESTAMP   | NOT NULL   |

### Load Checks Results Schema

* Первичный ключ: `(job_id, check_id, reference_date)`
* `source_id` содержит строковое представление списка в формате: `'[val1,val2,val3]'`.

| Column Name    | Column Type | Constraint |
|----------------|-------------|------------|
| job_id         | STRING      | NOT NULL   |
| check_id       | STRING      | NOT NULL   |
| check_name     | STRING      | NOT NULL   |
| source_id      | STRING      | NOT NULL   |
| expected       | STRING      | NOT NULL   |
| status         | STRING      | NOT NULL   |
| message        | STRING      |            |
| reference_date | TIMESTAMP   | NOT NULL   |
| execution_date | TIMESTAMP   | NOT NULL   |

### Checks Results Schema

* Первичный ключ: `(job_id, check_id, reference_date)`
* `source_id` содержит строковое представление списка в формате: `'[val1,val2,val3]'`.

| Column Name        | Column Type | Constraint |
|--------------------|-------------|------------|
| job_id             | STRING      | NOT NULL   |
| check_id           | STRING      | NOT NULL   |
| check_name         | STRING      | NOT NULL   |
| description        | STRING      |            |
| source_id          | STRING      | NOT NULL   |
| base_metric        | STRING      | NOT NULL   |
| compared_metric    | STRING      |            |
| compared_threshold | DOUBLE      |            |
| lower_bound        | DOUBLE      |            |
| upper_bound        | DOUBLE      |            |
| status             | STRING      | NOT NULL   |
| message            | STRING      |            |
| reference_date     | TIMESTAMP   | NOT NULL   |
| execution_date     | TIMESTAMP   | NOT NULL   |

## Скрипты для Настройки Хранилища Результатов в Hive

Ниже представлены HiveQL скрипты, которые могут быть использованы для инициализации хранилища результатов в Hive:

```hiveql
-- Необходимо заменить <schema_name> и <schema_dir> на фактические имя схемы и путь до нее.
set hivevar:schema_name=<schema_name>;
set hivevar:schema_dir=<schema_path>;

CREATE SCHEMA IF NOT EXISTS ${schema_name};

DROP TABLE IF EXISTS ${schema_name}.results_metric_regular;
CREATE EXTERNAL TABLE ${schema_name}.results_metric_regular
(
    job_id            STRING COMMENT '',
    metric_id         STRING COMMENT '',
    metric_name       STRING COMMENT '',
    description       STRING COMMENT '',
    source_id         STRING COMMENT '',
    column_names      STRING COMMENT '',
    params            STRING COMMENT '',
    result            DOUBLE COMMENT '',
    additional_result STRING COMMENT '',
    reference_date    TIMESTAMP COMMENT '',
    execution_date    TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Regular Metrics Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_metric_regular';

DROP TABLE IF EXISTS ${schema_name}.results_metric_composed;
CREATE EXTERNAL TABLE ${schema_name}.results_metric_composed
(
    job_id            STRING COMMENT '',
    metric_id         STRING COMMENT '',
    metric_name       STRING COMMENT '',
    description       STRING COMMENT '',
    source_id         STRING COMMENT '',
    formula           STRING COMMENT '',
    result            DOUBLE COMMENT '',
    additional_result STRING COMMENT '',
    reference_date    TIMESTAMP COMMENT '',
    execution_date    TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Composed Metrics Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_metric_composed';

DROP TABLE IF EXISTS ${schema_name}.results_check_load;
CREATE EXTERNAL TABLE ${schema_name}.results_check_load
(
    job_id         STRING COMMENT '',
    check_id       STRING COMMENT '',
    check_name     STRING COMMENT '',
    source_id      STRING COMMENT '',
    expected       STRING COMMENT '',
    status         STRING COMMENT '',
    message        STRING COMMENT '',
    reference_date TIMESTAMP COMMENT '',
    execution_date TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Load Checks Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_check_load';

DROP TABLE IF EXISTS ${schema_name}.results_check;
CREATE EXTERNAL TABLE ${schema_name}.results_check
(
    job_id             STRING COMMENT '',
    check_id           STRING COMMENT '',
    check_name         STRING COMMENT '',
    description        STRING COMMENT '',
    source_id          STRING COMMENT '',
    base_metric        STRING COMMENT '',
    compared_metric    STRING COMMENT '',
    compared_threshold DOUBLE COMMENT '',
    lower_bound        DOUBLE COMMENT '',
    upper_bound        DOUBLE COMMENT '',
    status             STRING COMMENT '',
    message            STRING COMMENT '',
    reference_date     TIMESTAMP COMMENT '',
    execution_date     TIMESTAMP COMMENT ''
)
COMMENT 'Data Quality Checks Results'
PARTITIONED BY (job_id STRING)
STORED AS PARQUET
LOCATION '${schema_dir}/results_check';
```