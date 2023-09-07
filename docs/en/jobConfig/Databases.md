# Database Connections

**Checkita** framework allows creating sources (see [Sources](Sources.md)) via
SQL queries to tables from different databases.

The **databases** section in the configuration file specifies parameters for connecting to external databases.
The following relational databases are currently supported:

* Oracle
* PostgreSQL
* SQLite.

An example of a **databases** section in a configuration file is shown at the bottom of the page.
Basic connection parameters:

* `id` - database identifier, to be specified in the Sources section.
* `url` - connection address. In the case of an Oracle or PostgreSQL database,
  it should contain the host, port, and database name.
* `user` and `passwrod` - login and password to connect to the database.
* `schema` - schema in the database.

## Oracle & PostgreSQL

* Required parameters: `id`, `url`, `user`, `password`
* Optional parameters: `schema`. If the schema is not specified, then the SQL query to the table will be built
  without specifying the schema (default schema will be used).

## SQLite

* You must specify the `id` and `url` (in the case of SQLite, the path to the database file).

```hocon
databases: {
   oracle:[
     {id: "oracle_db1", url: "oracle.db.com:1521/public", user: "dq-user", password: "dq-password"}
   ]
   postgresql:[
     {id: "postgre_db1", url: "postgre1.db.com:5432/public", user: "dq-user", password: "dq-password"},
     {
       id: "postgre_db2",
       url: "postgre2.db.com:5432/public",
       user: "dq-user",
       password: "dq-password",
       schema: "dataquality"
     }
   ]
   sqlite:[
     {id: "sqlite_db", url: "/user/home/my_sqlite.db"}
   ]
}
```