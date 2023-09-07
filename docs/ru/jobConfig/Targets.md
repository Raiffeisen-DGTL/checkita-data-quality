# Targets

Раздел **Targets** в конфигурационном файле позволяет сохранять бэкап результатов,
а также настраивать оповещения в случае, если какие-либо проверки не были пройдены.

Поддерживаются следующие типы **Targets**:

* ***Result Targets***: позволяют сохранить результаты расчета в Hive или в HDFS в одном из поддерживаемых форматов.
  Поддерживается сохранение всех основных типов метрик и проверок, посредством следующих подтипов Results Targets:
    * `columnMetrics` - сохраняет результаты расчета колоночных метрик;
    * `fileMetrics` - сохраняет результаты расчета файловых метрик;
    * `composedMetrics` - сохраняет результаты расчета композиционных метрик;
    * `checks` - сохраняет результаты проверок.
    * `loadChecks` - сохраняет результаты Load Checks.
* ***Error Collection***: позволяют задавать список метрик для сбора их ошибок.
  Сбор ошибок реализован только для `statusable` метрик (см. раздел [Metrics](./Metrics.md)).
  Также, для сбора ошибок, для источников должны быть определены `keyFields` - список колонок, которые формируют
  бизнес-ключ для данного источника. В случае, если метрика для какой-либо строки вернула статус "ошибка", значения
  в колонках `keyFields`, а также в колонках по которым считается метрика, будут записаны в отчет.
* ***Check Alerts***: позволяют настраивать уведомления, в случае если какие-либо из указанных проверок не были пройдены.
* ***Summary***: позволяют настраивать отправку отчетов по завершении расчета DQ. Также к отчетам можно прикрепить отчет
  об ошибках по метрикам.

## Save Targets to Hive

Для того чтобы сохранить Targets в Hive, необходимо предварительно создать схему
с соответствующими таблицами.

Необходимые параметры для сохранения Targets в Hive:

* `schema` - схема для Targets в Hive
* `table` - таблица, соответствующая данному подтипу Targets.
* `partitionColumn [optional]` - имя колонки с датой (в формате `string`), по которой партиционируется таблица.
  По умолчанию: `execDate`.

## Save Targets to HDFS

Targets можно сохранить в HDFS в одном из следующих форматов: ***csv***, ***txt***, ***parquet***, ***orc***.

Параметры для сохранения Targets в HDFS схожи с теми, что используются для описания HDFS источников данных
(см. [Sources](Sources.md)).

Необходимые параметры:

* `fileFormat` - формат файла (*csv*, *txt*, *parquet*, *orc*).
* `path` - путь до директории, куда будет сохранен Target. Имя файла будет сформировано автоматически и
  будет соответствовать подтипу Target'a.

Для текстовых форматов (csv, txt) можно указать следующие опциональные параметры:

* `delimiter [optional]` - разделитель (по умолчанию: `,`)
* `quote [optional]` - обрамляющие кавычки (по умолчанию `"`)
* `quoted [optional]` - булевый параметр (true/false), указывающий нужно ли обрамлять все значения в файле в кавычки.
  По умолчанию: `false`.
* `escape [optional]` - символ экранирования (по умолчанию ` \ `)

## Save Targets to Kafka

Result Targets можно отправить в виде сообщений в топик Kafka в `json` формате.
Для этого, необходимо заполнить раздел `results`, подраздел `kafka`, со следующими параметрами:

* `results` - список Result Targets для отправки в Kafka.
* `brokerId` - Id брокера Kafka, описанного в разделе [MessageBrokers](MessageBrokers.md),
* `topic` - имя топика Kafka для отправки сообщений.
* `options [optional]` - дополнительные параметры для отправки сообщений в Kafka

## Error Collection

### Save Errors to HDFS

Для настройки сохранения отчетов в HDFS, необходимо указать следующие параметры:

* `metrics [optional]` - список метрик, по которым нужно сохранить ошибки. Если опущен, то ошибки будут сохранены
  по всем Statusable метрикам.
* `dumpSize [optional]` - максимальное количество строк, где метрика посчиталась с ошибкой, которые будут записаны в отчет.
  Рекомендуется задавать значение <= `100` строк. Максимально допустимое значение `10000`. Значение по умолчанию - `100`.
* `fileFormat` - формат файла для записи отчета об ошибках: `csv`, `orc` или `parquet`.
* `path` - директория в HDFS, куда будет записан отчет.

Для текстового формата (csv) можно указать следующие опциональные параметры:
* `delimiter [optional]` - разделитель (по умолчанию: `,`)
* `quote [optional]` - обрамляющие кавычки (по умолчанию `"`)
* `quoted [optional]` - булевый параметр (true/false), указывающий нужно ли обрамлять все значения в файле в кавычки.
  По умолчанию: `false`.
* `escape [optional]` - символ экранирования (по умолчанию `` \ ``)

### Sent Errors to Kafka

Для настройки отправки сообщений по ошибкам метрик в Kafka, необходимо указать следующие параметры:

* `metrics [optional]` - список метрик, по которым нужно сохранить ошибки. Если опущен, то ошибки будут сохранены
  по всем Statusable метрикам.
* `dumpSize [optional]` - максимальное количество строк, где метрика посчиталась с ошибкой, которые будут записаны в отчет.
  Рекомендуется задавать значение <= `100` строк. Максимально допустимое значение `10000`. Значение по умолчанию - `100`.
* `brokerId` - Id брокера Kafka, описанного в разделе [MessageBrokers](MessageBrokers.md),
* `topic` - имя топика Kafka для отправки сообщений.
* `options [optional]` - дополнительные параметры для отправки сообщений в Kafka.

## Check Alerts

Отправка уведомлений о проверках, которые получили статус `Failure`

### Send Alerts to Email

Для настройки уведомлений о проверках на почту необходимо указать следующие параметры:

* `id` - идентификатор уведомления (можно настраивать несколько типов уведомлений с разными проверками и разными адресатами).
* `checks [optional]` - список идентификаторов проверок, по которым будут рассылаться уведомления.
  По умолчанию - все проверки, описанные в конфигурационном файле.
* `mailingList` - список адресатов для рассылки уведомлений.

### Send Alerts to Mattermost

Для настройки уведомлений о проверках в Mattermost необходимо указать следующие параметры:

* `id` - идентификатор уведомления (можно настраивать несколько типов уведомлений с разными проверками и разными адресатами).
* `checks [optional]` - список идентификаторов проверок, по которым будут рассылаться уведомления.
  По умолчанию - все проверки, описанные в конфигурационном файле.
* `recipients` - список адресатов для рассылки уведомлений. Уведомления можно отправлять как в каналы, так и в директ пользователям.
  > **Важно:** Названия каналов должны начинаться с `#`, а для отправки уведомлений в директ пользователю, нужно
  > указать имя его учетной записи, с префиксом `@`, например: `@someUser`

### Send Alerts to Kafka

Для настройки отправки сообщений о проверках со статусом `Failure` в Kafka необходимо указать следующие параметры:

* `id` - идентификатор уведомления (можно настраивать несколько типов уведомлений с разными проверками и разными адресатами).
* `checks [optional]` - список идентификаторов проверок, по которым будут рассылаться уведомления.
  По умолчанию - все проверки, описанные в конфигурационном файле.
* `brokerId` - Id брокера Kafka, описанного в разделе [MessageBrokers](MessageBrokers.md),
* `topic` - имя топика Kafka для отправки сообщений.
* `options [optional]` - дополнительные параметры для отправки сообщений в Kafka.

## Summary

### Send Summary Report to Email

Для настройки Email рассылки отчетов по завершении каждого расчета DQ, необходимо указать следующие параметры:

* `mailingList` - список адресатов для рассылки уведомлений.
* `attachMetricErrors [optional]` - булевый параметр, указывающий, нужно ли прикреплять к письму файл с отчетом об ошибках в метриках.
  По умолчанию - `false`
* `dumpSize [optional]` - максимальное количество строк, где метрика посчиталась с ошибкой, которые будут записаны в отчет.
  Рекомендуется задавать значение <= `100` строк. Максимально допустимое значение `10000`. Значение по умолчанию - `100`.
  ***Нужно указывать только, если `attachMetricErrors = true`.***

### Send Summary Report to Mattermost

Для настройки рассылки отчетов по завершении каждого расчета DQ в Mattermost, необходимо указать следующие параметры:

* `recipients` - список адресатов для рассылки отчетов. Уведомления можно отправлять как в каналы, так и в директ пользователям.
  > **Важно:** Названия каналов должны начинаться с `#`, а для отправки уведомлений в директ пользователю, нужно
  > указать имя его учетной записи, с префиксом `@`, например: `@somUser`
* `attachMetricErrors [optional]` - булевый параметр, указывающий, нужно ли прикреплять к письму файл с отчетом об ошибках в метриках.
  По умолчанию - `false`
* `dumpSize [optional]` - максимальное количество строк, где метрика посчиталась с ошибкой, которые будут записаны в отчет.
  Рекомендуется задавать значение <= `100` строк. Максимально допустимое значение `10000`. Значение по умолчанию - `100`.
  ***Нужно указывать только, если `attachMetricErrors = true`.***

### Send Summary Report to Mattermost

Для настройки отправки отчетов по завершении каждого расчета DQ в Kafka, необходимо указать следующие параметры:

> **Важно** В случае отправки отчета в виде сообщения в Kafka, естественным образом, к нему нельзя прикрепить отчет
> с ошибками по метрикам. Для этого, в разделе `errorCollection` нужно также настроить отправку сообщени с ошибками
> по метрикам в Kafka.

* `brokerId` - Id брокера Kafka, описанного в разделе [MessageBrokers](MessageBrokers.md),
* `topic` - имя топика Kafka для отправки сообщений.
* `options [optional]` - дополнительные параметры для отправки сообщений в Kafka.


Ниже показан пример раздела `targets` в конфигурационном файле:

```hocon
targets: {
  hive: {
    columnMetrics: {schema: "some_schema", table: "DQ_COLUMNAR_METRICS", partitionColumn: "load_date"},
    fileMetrics: {schema: "some_schema", table: "DQ_FILE_METRICS", partitionColumn: "load_date"},
    composedMetrics: {schema: "some_schema", table: "DQ_COMPOSED_METRICS", partitionColumn: "load_date"}
  }
  hdfs: {
    checks: {
      fileFormat: "csv"
      path: "/tmp/dataquality/results"
      delimiter: ":",
      quote: "'",
      escape: "|",
    },
    loadChecks: {fileFormat: "orc", path: "/tmp/dataquality/results"}
  }
  results: {
    kafka: {
      results: ["columnMetrics", "fileMetrics", "composedMetrics", "loadChecks", "checks"],
      brokerId: "kafka1"
      topic: "some.topic"
    }
  }
  errorCollection: {
    hdfs: {
      metrics: ["metric_1", "metric_2", "metric_n"]
      dumpSize: 50
      fileFormat: "csv"
      path: "tmp/DQ/ERRORS"
      // metric errors report contains JSON string in column ROW_DATA, therefore,
      // it is reccomended to use semicolon or tab separator for csv file.
      delimiter: ";" 
      quote: "\""
      escape: "\\"
      quoted: false
    }
    kafka: {
      metrics: ["hive_table1_nulls", "fiexed_file1_dist_name", "parquet_file_id_regex"]
      dumpSize: 25
      brokerId: "kafka1"
      topic: "some.topic"
    }
  }
  summary: {
    email: {
      attachMetricErrors: true
      metrics: ["metric_1", "metric_2", "metric_n"]
      dumpSize: 10
      mailingList: ["some.user@some.domain"]
    }
    mattermost: {
      attachMetricErrors: true
      metrics: ["hive_table1_nulls", "fiexed_file1_dist_name", "parquet_file_id_regex"]
      dumpSize: 10
      recipients: ["@someUser", "#someChannel"]
    }
    kafka: {
      brokerId: "kafka1"
      topic: "some.topic"
    }
  }
  checkAlerts: {
    email: [
      {
        id: "alert1"
        checks: ["check_1", "check_2"]
        mailingList: ["preson1@some.domain", "person2@some.domain"]
      }
      {
        id: "alert2"
        checks: ["check_3", "check_n"]
        mailingList: ["preson3@some.domain"]
      }
    ]
    mattermost: [
      {
        id: "alert1"
        checks: ["avg_bal_check", "zero_nulls"]
        recipients: ["@someUser"]
      }
      {
        id: "alert2"
        checks: ["top2_curr_match", "completeness_check"]
        recipients: ["#someChannel"]
      }
    ]
    kafka: [
      {
        id: "alert1"
        checks: ["avg_bal_check", "zero_nulls"]
        brokerId: "kafka1"
        topic: "some.topic"
      }
      {
        id: "alert2"
        checks: ["top2_curr_match", "completeness_check"]
        brokerId: "kafka1"
        topic: "some.topic"
      }
    ]
  }
}
```