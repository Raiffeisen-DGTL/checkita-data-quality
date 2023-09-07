# Message Brokers

The **Checkita** framework allows you to create data sources based on message queues (see [Sources](Sources.md)).
In addition, it is also possible to send messages with the results of the calculation of metrics and checks.
For that purpose it is necessary to describe the connections to the appropriate message brokers.

> **Important:** Only **Kafka** is currently supported.

An example of a **messageBrokers** section in a configuration file is shown at the bottom of the page.
The main parameters for connecting to a Kafka broker are as follows:

* `id` - broker ID, to be specified in the [Sources](Sources.md) or [Targets](Targets.md) section.
* `servers` is a list of Kafka bootstrap servers to connect to.
* `jaasConfigFile [optional]` - Jaas file describing Kafka authorization method
  > **Important:** This option only works when running in local mode (with the `-l` flag).
* `parameters` - additional parameters for connecting to Kafka.
  (See [Kafka Connect Configs](https://kafka.apache.org/documentation/#connectconfigs))

### Kafka Connection with JAAS Configuration File

Currently, only **ONE** connection is supported using a Jaas file.

In case of running the framework in local mode (with the `-l` flag), you can specify the location of the Jaas file
in the configuration file as shown above. In case Kerberos authentication is used, the path to
keytab file is specified according to its location on the local file system.

If running on a cluster, such as YARN, the above setting in the configuration file will not work, and
the location of the Jaas file must be specified in the Spark application settings:
* If running in `cluster-mode`, the parameters should be set as follows:
   ```bash
   --deploy-mode cluster \
   --conf 'spark.driver.extraJavaOptions="-Djava.security.auth.login.config=./jaas.conf"' \
   --conf 'spark.executor.extraJavaOptions="-Djava.security.auth.login.config=./jaas.conf"' \
   --files /path/to/your/kerberos/file.keytab,/path/to/your/jaas.conf,<other files required for DQ>
   ```
  Thus, the files specified in `--files` will be copied to the working directories of the driver and executors,
  and then the `extraJavaOptions` parameters specify the path to the Jaas file already in the working directory.
* In case of launching application in the `client-mode`, the type of the `extraJavaOptions` specification
  for the driver changes, because it starts on the client machine. And, what is also important, the JVM starts before
  `--conf` options are initialized:
   ```bash
   --deploy-mode client \
   --driver-java-options "-Djava.security.auth.login.config=.jaas.conf" \
   --conf 'spark.executor.extraJavaOptions="-Djava.security.auth.login.config=./jaas.conf"' \
   --files file.keytab,jaas.conf,<other files required for DQ>
   ```
  Thus, for the driver, the path to the file is specified in the local file system, and for executors, the file
  will also be copied to the working directory. However, if Kerberos authentication is used,
  the Jaas file specifies the path to the keytab file. To make this path the same for the driver and executors,
  you need to put the files in the working directory from which the Spark application will be started.

### Examples

```hocon
messageBrokers: {
  kafka: [
    {
      id: "kafka1",
      servers: ["kafka-broker1:9092", "kafka-broker2:9092", "kafka-broker3:9092"]
      jaasConfigFile: "./configs/jaas.conf"
      parameters: [
        "security.protocol=SASL_PLAINTEXT",
        "sasl.mechanism=GSSAPI",
        "sasl.kerberos.service.name=kafka-service"
      ]
    }
  ]
}
```

**Jaas file example**

```
KafkaClient {
com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   storeKey=true
   serviceName=kafka-service
   keyTab="./configs/some_user.keytab"
   principal="some_principle@some_realm";
};
```