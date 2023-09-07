# Database Connections

**Checkita** фреймворк позволяет создавать источники (см. [Sources](Sources.md)) посредством
SQL запросов к таблицам из различных БД.

Раздел **databases** в конфигурационном файле задает параметры подключения к внешним БД.
На текущий момент поддерживаются следующие реляционные БД:

* Oracle
* PostgreSQL
* SQLite.

Пример раздела **databases** в конфигурационном файле показан внизу страницы. Основные параметры подключения:

* `id` - идентификатор БД, для указания в разделе Sources.
* `url` - адрес подключения. В случае БД Oracle или PostgreSQL должна содержать хост, порт и имя базы данных.
* `user` и `passwrod` - логин и пароль для подключения к БД.
* `schema` - схема в БД.

## Oracle & PostgreSQL

* Обязательно параметры: `id`, `url`, `user`, `password`
* Опциональные параметры: `schema`. Если схема не указана, то SQL запрос к таблице будет строиться без указания схемы.

## SQLite

* Необходимо указать `id` и `url` (в случае с SQLite - путь до файла БД).

```hocon
databases: {
  oracle: [
    {id: "oracle_db1", url: "oracle.db.com:1521/public", user: "dq-user", password: "dq-password"}
  ]
  postgresql: [
    {id: "postgre_db1", url: "postgre1.db.com:5432/public", user: "dq-user", password: "dq-password"},
    {
      id: "postgre_db2",
      url: "postgre2.db.com:5432/public",
      user: "dq-user",
      password: "dq-password",
      schema: "dataquality"
    }
  ]
  sqlite: [
    {id: "sqlite_db", url: "/user/home/my_sqlite.db"}
  ]
}
```