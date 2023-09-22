# Virtual Sources

**Checkita** фреймворк поддерживает создание временных (виртуальных) источников
на основе базовых источников данных (см. [Sources](Sources.md)) применяя к ним различные трансформации,
посредством Spark SQL API. Впоследствии, к виртуальным источником могут быть также применены метрики и проверки.

На текущий момент поддерживаются следующие типы виртуальных источников:

* `filterSql` - создает виртуальный источник на основе существующего применяя к нему указанный SQL запрос.
* `joinSql` - создает виртуальный источник, объединяя два (или более) других источника посредством SQL запроса.
* `join` - создает виртуальный источник, объединяя два других источника без SQL запроса,
  а просто с указанием списка колонок.

Для всех виртуальных источников необходимо указать следующие параметры:

* `id` - id виртуального источника
* `parentSources` - список id исходных источников
  (список из 1-го источника для `filterSql`, список из двух источников для `join` и список источников произвольной длинны для `joinSql`)
* `persist [optional]` - опционально, можно кешировать полученный источник на время расчета метрик.
  Для этого в поле `persist` необходимо указать один из возможных Spark StorageLevels.
  Если поле `persist` отсутствует, то виртуальный источник не будет кешироваться.
  > Spark Storage Levels
  >
  > * NONE
  > * DISK_ONLY
  > * DISK_ONLY_2
  > * MEMORY_ONLY
  > * MEMORY_ONLY_2
  > * MEMORY_ONLY_SER
  > * MEMORY_ONLY_SER_2
  > * MEMORY_AND_DISK
  > * MEMORY_AND_DISK_2
  > * MEMORY_AND_DISK_SER
  > * MEMORY_AND_DISK_SER_2
  > * OFF_HEAP.
* `save [optional]` - опциональный раздел, который можно заполнить, чтобы сохранить виртуальный источник:
  > * `path` - путь до директории, куда будет сохранен виртуальный источник;
  > * `fileFormat` - формат, в котором нужно сохранить источник: orc, parquet, csv
  > * `date` - дата за которую записываются Targets, если отличается от `executionDate`.
  >
  > Для текстовых форматов (csv, txt) можно указать следующие опциональные параметры:
  > * `delimiter [optional]` - разделитель (по умолчанию: `,`)
  > * `quote [optional]` - обрамляющие кавычки (по умолчанию `"`)
  > * `quoted [optional]` - булевый параметр (true/false), указывающий нужно ли обрамлять все значения в файле в кавычки.
  >   По умолчанию: `false`.
  > * `escape [optional]` - символ экранирования (по умолчанию `` \ ``)

* `keyFields [optional]` - список колонок-ключей, которые идентифицируют строку в источнике.
  Используются для составления отчетов с ошибками при вычислении метрик.

Для **filterSql** и **joinSql**, дополнительно указывается запрос для исполнения в поле `sql`.
Для **join** дополнительно указываются список колонок, по которым происходит объединение в поле `joinColumns`,
а также тип "джоина" в поле `joinType`.

> Тип "джоина" должен быть одним из следующего списка:
>
> * inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, left_anti.

Пример раздела **virtualSources** в конфигурационном файле показан ниже:

```hocon
virtualSources: {
  filterSql: [ // список виртуальных источников типа filterSql
    {
      id: "vSource1",
      parentSources: ["table1"],
      sql: "select distinct company_name as name from table1 where company_name like 'C%'"
      persist: "MEMORY_AND_DISK"
    }
  ]
  joinSql: [ // список виртуальных источников типа joinSql
    {
      id: "vSource2",
      parentSources: ["table1", "table2"],
      sql: "select * from table1 left join table on client_name=supplier_name",
      save: {
        path: "/path/to/virtual/source/save/directory"
        fileFormat: "orc"
      }
      keyFields: ["id", "client_name"]
    }
  ]
  join: [ // список виртуальных источников типа join
    {id: "vSource3", parentSources=["hive_table1, hive_table2"], joinColumns=["ORDER_ID","CURRENCY"], joinType="inner"}
  ]
}
```