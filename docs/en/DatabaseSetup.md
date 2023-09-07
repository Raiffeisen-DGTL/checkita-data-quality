# Database Setup

In order to use all the features of the framework, it is required to set up a database,
which will store the results of metrics and checks calculations.

It is proposed to use PostgreSQL 9.3 and higher in order to work with the **Checkita** framework.
SQLite, Hive, and simple file storage (Parquet) are also supported.

Having history of the metrics and checks calculation results it is possible to perform the so-called "trend" checks,
which use values from previous runs to compare with current ones. This approach allows detecting anomalies in the data.

To work with the framework, a schema with the appropriate structure must be created in the database,
in accordance with the following files:
* PostgreSQL: [db_init_postgre.sql](../setup/db_init_postgre.sql)
* SQLite: [db_init_postgre.sql](../setup/db_init_sqlite.sql)
* Hive: [db_init_postgre.sql](../setup/db_init_hive.sql)
* Delta Tables: [db_init_postgre.sql](../setup/db_init_delta_tables.sql)