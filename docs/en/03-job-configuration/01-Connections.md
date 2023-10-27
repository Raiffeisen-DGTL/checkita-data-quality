# Connections Configuration

Checkita framework allows creation of data sources based on data from external systems such as RDBMS or message queues
like Kafka. In order to read data from external systems it is required to establish a connection in a first place.

Thus, connections are described in `connections` section of job configuration. Currently, connection to following
systems are supported:

* Connection to databases via JDBC:
    * PostgreSQL (also can be used for connection to GreenPlum);
    * Oracle
    * SQl
* Connection to message queues:
    * Kafka

All connections must have and `id` to uniquely identify its configuration and also may have an optional list of
additional  Spark parameters can be specified in field `parameters` to provide some extra configuration required
by Spark to read data from a particular system.

Example of `connections` section of job configuration is shown in 
[Connections Configuration Example](#connections-configuration-example) below.

## SQLite Connection Configuration

Configuring connection to SQLite database is quite easy. It is required supplying only two parameters:

* `id` - *Required*. Connection ID;
* `url` - *Required*. Path to SQLite database file.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.

## PostgreSQL Connection Configuration

Configuration to PostgreSQL can be set up using following parameters:

* `id` - *Required*. Connection ID;
* `url` - *Required*. Connection URL. Should contain host, port and name of database.
  In addition, extra parameters can be supplied in connection URL if required.
  *Connection protocol must not be specified.*
* `username` - *Optional*. Username used to connect to PostgreSQL database if required.
* `password` - *Optional*. Password used to connect to PostgreSQL database if required.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.

## Oracle Connection Configuration

Configuration to Oracle can be set up in the same way as to PostgreSQL, using following parameters:

* `id` - *Required*. Connection ID;
* `url` - *Required*. Connection URL. Should contain host, port and name of database.
  In addition, extra parameters can be supplied in connection URL if required.
  *Connection protocol must not be specified.*
* `username` - *Optional*. Username used to connect to PostgreSQL database if required.
* `password` - *Optional*. Password used to connect to PostgreSQL database if required.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`.

## Kafka Connection Configuration

In order to connect to set up connection to Kafka brokers, it is required to supply following parameters:

* `id` - *Required*. Connection ID;
* `servers` - *Required*. List of broker servers to connect to.
* `parameters` - *Optional*. List of Spark parameters if required where each parameter is a string in format:
  `spark.param.name=spark.param.value`. Usually, Kafka authorisation settings are provided by means of spark parameters.

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

## Connections Configuration Example

As it is shown in the example below, connections of the same type are grouped within subsections named after the type
of connection. These subsections should contain a list of connection configurations of the corresponding type.

```hocon
jobConfig: {
  connections: {
    postgresql: [
      {id: "postgre_db1", url: "postgre1.db.com:5432/public", user: "dq-user", password: "dq-password"}
      {
        id: "postgre_db2",
        url: "postgre2.db.com:5432/public",
        user: "dq-user",
        password: "dq-password",
        schema: "dataquality"
      }
    ]
    oracle: [
      {id: "oracle_db1", url: "oracle.db.com:1521/public", username: "db-user", password: "dq-password"}
    ]
    sqlite: [
      {id: "sqlite_db", url: "some/path/to/db.sqlite"}
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
    ]
  }
}
```