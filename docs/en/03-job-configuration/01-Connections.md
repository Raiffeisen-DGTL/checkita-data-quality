# Connections Configuration

Checkita framework allows creation of data sources based on data from external systems such as RDBMS or message queues
like Kafka. In order to read data from external systems it is required to establish a connection in a first place.

Thus, connections are described in `connections` section of job configuration. Currently, connection to following
systems are supported:

* Connection to databases via JDBC:
    * PostgreSQL (also can be used for connection to GreenPlum);
    * Oracle
    * SQLite
    * MySQL
    * MS SQL
    * H2
    * ClickHouse
* Connection to message queues:
    * Kafka
* Connection to Greenplum via pivotal connector

All connections are defined with following common parameters:

* `id` - Connection ID that uniquely identifies its configuration;
* `description` - Optional connection description;
* `parameters` - Optional list of additional Spark parameters that can be specified to provide some extra configuration
  required by Spark to read data from a particular system.
* `metadata` - Optional list of arbitrary user-defined metadata parameters.

Example of `connections` section of job configuration is shown in 
[Connections Configuration Example](#connections-configuration-example) below.

## SQLite Connection Configuration

Configuring connection to SQLite database is quite easy. In addition to common parameters it is required to supply
only a path to database file:

* `id` - *Required*. Connection ID;
* `description` - *Optional*. Connection description;
* `url` - *Required*. Path to SQLite database file.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this connection where each parameter 
  is a string in format: `param.name=param.value`.

## PostgreSQL Connection Configuration

Configuration to PostgreSQL can be set up using following parameters:

* `id` - *Required*. Connection ID;
* `description` - *Optional*. Connection description;
* `url` - *Required*. Connection URL. Should contain host, port and name of database.
  In addition, extra parameters can be supplied in connection URL if required.
  *Connection protocol must not be specified.*
* `username` - *Optional*. Username used to connect to PostgreSQL database if required.
* `password` - *Optional*. Password used to connect to PostgreSQL database if required.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this connection where each parameter
  is a string in format: `param.name=param.value`.

## Oracle Connection Configuration

Configuration to Oracle can be set up in the same way as to PostgreSQL, using following parameters:

* `id` - *Required*. Connection ID;
* `description` - *Optional*. Connection description;
* `url` - *Required*. Connection URL. Should contain host, port and name of database.
  In addition, extra parameters can be supplied in connection URL if required.
  *Connection protocol must not be specified.*
* `username` - *Optional*. Username used to connect to Oracle database if required.
* `password` - *Optional*. Password used to connect to Oracle database if required.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this connection where each parameter
  is a string in format: `param.name=param.value`.

## MySQL Connection Configuration

Configuration to MySQL can be set up in the same way as to PostgreSQL and Oracle, using following parameters:

* `id` - *Required*. Connection ID;
* `description` - *Optional*. Connection description;
* `url` - *Required*. Connection URL. Should contain host, port and name of database.
  In addition, extra parameters can be supplied in connection URL if required.
  *Connection protocol must not be specified.*
* `username` - *Optional*. Username used to connect to MySQL database if required.
* `password` - *Optional*. Password used to connect to MySQL database if required.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this connection where each parameter
  is a string in format: `param.name=param.value`.

## MS SQL Connection Configuration

Configuration to MS SQL can be set up similarly to the 3 previous, using following parameters:

* `id` - *Required*. Connection ID;
* `description` - *Optional*. Connection description;
* `url` - *Required*. Connection URL. Should contain host, port and name of database.
  In addition, extra parameters can be supplied in connection URL if required.
  *Connection protocol must not be specified.*
* `username` - *Optional*. Username used to connect to MS SQL database if required.
* `password` - *Optional*. Password used to connect to MS SQL database if required.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this connection where each parameter
  is a string in format: `param.name=param.value`.

## H2 Connection Configuration

Configuring connection to H2 database has similarly to SQLite. It is required supplying only two parameters:

* `id` - *Required*. Connection ID;
* `description` - *Optional*. Connection description;
* `url` - *Required*. Connection URL. Should contain host, port and name of database.
  In addition, extra parameters can be supplied in connection URL if required.
  *Connection protocol must not be specified.*
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this connection where each parameter
  is a string in format: `param.name=param.value`.

## ClickHouse Connection Configuration

Configuration to ClickHouse can be set up in the same way as to MS SQL, using following parameters:

* `id` - *Required*. Connection ID;
* `description` - *Optional*. Connection description;
* `url` - *Required*. Connection URL. Should contain host, port and name of database.
  In addition, extra parameters can be supplied in connection URL if required.
  *Connection protocol must not be specified.*
* `username` - *Optional*. Username used to connect to ClickHouse database if required.
* `password` - *Optional*. Password used to connect to ClickHouse database if required.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this connection where each parameter
  is a string in format: `param.name=param.value`.

## Kafka Connection Configuration

In order to connect to set up connection to Kafka brokers, it is required to supply following parameters:

* `id` - *Required*. Connection ID;
* `description` - *Optional*. Connection description;
* `servers` - *Required*. List of broker servers to connect to.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`. Usually, Kafka authorisation settings are provided by means of spark parameters.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this connection where each parameter
  is a string in format: `param.name=param.value`.

If connection to Kafka cluster requires JAAS configuration file, then it should be provided via Java environment
variables. Note, that these variables must be declared prior JVM starts, therefore, they must be set in `spark-submit` 
command as follows:

* When running application in `cluster` mode:
  ```bash
  --deploy-mode cluster \
  --conf 'spark.driver.extraJavaOptions="-Djava.security.auth.login.config=./jaas.conf"' \
  --conf 'spark.executor.extraJavaOptions="-Djava.security.auth.login.config=./jaas.conf"' \
  --files /path/to/your/jaas.conf,<other files required for DQ>
  ```
* When running application in `client` mode the driver JVM starts on client prior Spark configuration is read,
  therefore, Java environment variables for driver must be set in advance using `--driver-java-options` argument:
  ```bash
  --deploy-mode client \
  --driver-java-options "-Djava.security.auth.login.config=.jaas.conf" \
  --conf 'spark.executor.extraJavaOptions="-Djava.security.auth.login.config=./jaas.conf"' \
  --files file.keytab,jaas.conf,<other files required for DQ>
  ```

## Greenplum Connection Configuration (via pivotal)

Configuring connection to Greenplum, you must specify the following parameters:

* `id` - *Required*. Connection ID;
* `description` - *Optional*. Connection description;
* `url` - *Required*. Connection URL. Should contain host, port and name of database.
  In addition, extra parameters can be supplied in connection URL if required.
  *Connection protocol must not be specified.*
* `username` - *Optional*. Username used to connect to Greenplum database if required.
* `password` - *Optional*. Password used to connect to Greenplum database if required.
* `schema` - *Optional*. schema to lookup tables from. If omitted, default schema is used.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.
* `metadata` - *Optional*. List of user-defined metadata parameters specific to this connection where each parameter
  is a string in format: `param.name=param.value`.

Pivotal connector is not published in public repositories such as Maven Central. Therefore, this dependency is 
unmanaged and should be manually added to Spark application during submit (using spark.jars configuration parameter). 
Connector jar-file can be downloaded from official [Pivotal releases](https://network.pivotal.io/products/vmware-greenplum#/releases/1427678/file_groups/17497).

## Connections Configuration Example

As it is shown in the example below, connections of the same type are grouped within subsections named after the type
of connection. These subsections should contain a list of connection configurations of the corresponding type.

```hocon
jobConfig: {
  connections: {
    postgres: [
      {
        id: "postgre_db1",
        description: "Connection to production instance of DB"
        url: "postgre1.db.com:5432/public", 
        username: "dq-user", 
        password: "dq-password",
        metadata: [
          "db.owner=some.user@some.domain",
          "environment=prod"
        ]
      }
      {
        id: "postgre_db2",
        description: "Connection to test instance of DB"
        url: "postgre2.db.com:5432/public",
        username: "dq-user",
        password: "dq-password",
        schema: "dataquality",
        metadata: [
          "db.owner=some.user@some.domain",
          "environment=test"
        ]
      }
    ]
    oracle: [
      {id: "oracle_db1", url: "oracle.db.com:1521/public", username: "db-user", password: "dq-password"}
    ]
    sqlite: [
      {id: "sqlite_db", url: "some/path/to/db.sqlite"}
    ],
    mysql: [
      {id: "mysql_db1", url: "mysql.db.com:8306/public", username: "user", password: "pass"}
    ],
    mssql: [
      {id: "mssql_db1", url: "mssql.db.com:8433", username: "user", password: "pass"}
    ],
    h2: [
      {id: "h2_db1", url: "h2.db.com:9092/default", username: "user", password: "pass"}
    ],
    clickhouse: [
      {id: "clickhouse_db1", url: "clickhouse.db.com:8123", username: "user", password: "pass"}
    ],
    kafka: [
      {id: "kafka_cluster_1", servers: ["server1:9092", "server2:9092"]}
      {
        id: "kafka_cluster_2",
        servers: ["kafka-broker1:9092", "kafka-broker2:9092", "kafka-broker3:9092"]
        parameters: [
          "security.protocol=SASL_PLAINTEXT",
          "sasl.mechanism=GSSAPI",
          "sasl.kerberos.service.name=kafka-service"
        ]
      }
    ],
    greenplum: [
      {
        id: "greenplum_db1", 
        url: "greenplum.db.com:5432/postgres", 
        username: "user", 
        password: "pass",
        schema: "public"
      }
    ]
  }
}
```