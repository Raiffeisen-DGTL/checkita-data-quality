# Message Brokers

**Checkita** фреймворк позволяет создавать источники данных на основе очередей сообщений (см. [Sources](Sources.md)).
Помимо этого, также имеется возможность отправки сообщений с результатами расчета метрик и проверок.
Для этого необходимо описать подключения к соответствующим брокерам сообщений.

> **Важно:** На данный момент поддерживается только **Kafka**.

Пример раздела **messageBrokers** в конфигурационном файле показан внизу страницы.
Основные параметры подключения к Kafka-брокеру следующие:

* `id` - идентификатор брокера, для указания в разделе [Sources](Sources.md) или [Targets](Targets.md).
* `servers` - список bootstrap-серверов Kafka для подключения.
* `jaasConfigFile [optional]` - Jaas файл с описанием способа авторизации в Kafka
  > **Важно:** Данный параметр работает только при запуске в локальном режиме (с флагом `-l`).
* `parameters` - дополнительные параметры подключения к Kafka.
  (см. [Kafka Connect Configs](https://kafka.apache.org/documentation/#connectconfigs))

### Kafka Connection with JAAS Configuration File

На данный момент поддерживается только **ОДНО** подключение с помощью Jaas файла.

В случае запуска фреймворка в локальном режиме (с флагом `-l`), можно указать местоположение Jaas файла
в конфигурационном файле, как указано выше. В случае, если используется Kerberos аутентификация, путь до
keytab-файла указывается в соответствии с его расположением в локальной файловой системе.

В случае запуска в кластере, например в YARN, вышеуказанный параметр в конфигурационном файле работать не будет, и
расположение Jaas файла нужно указывать в параметрах Spark-приложения:
* В случае запуска в `cluster-mode` параметры нужно задавать следующим образом:
  ```bash
  --deploy-mode cluster \
  --conf 'spark.driver.extraJavaOptions="-Djava.security.auth.login.config=./jaas.conf"' \
  --conf 'spark.executor.extraJavaOptions="-Djava.security.auth.login.config=./jaas.conf"' \
  --files /path/to/your/kerberos/file.keytab,/path/to/your/jaas.conf,<other files required for DQ>
  ```
  Таким образом, файлы, указанные в `--files`, будут скопированы в рабочие директории драйвера и экзекьюторов,
  и далее параметрах `extraJavaOptions` указывается путь до Jaas файла уже в рабочей директории.
* В случае запуска в `client-mode` параметр, меняется тип указания `extraJavaOptions` для драйвера, т.к.
  он запускается на клиентской машине и, что важно, JVM стартует до того, как будут проинициализированы
  `--conf` параметры:
  ```bash
  --deploy-mode client \
  --driver-java-options "-Djava.security.auth.login.config=.jaas.conf" \
  --conf 'spark.executor.extraJavaOptions="-Djava.security.auth.login.config=./jaas.conf"' \
  --files file.keytab,jaas.conf,<other files required for DQ>
  ```
  Таким образом, для драйвера путь до файла указывается в локальной файловой системе, а для экзекьюторов файл
  будет также скопирован в рабочую директорию. При этом, в случае, если используется Kerberos-аутентификация,
  в Jaas файле указывается путь до keytab-файла. Чтобы этот путь был одинаковым для драйвера и экзекьюторов,
  нужно положить файлы в рабочую директорию, из которой будет запускаться Spark-приложение.


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

**Пример Jaas файла**

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