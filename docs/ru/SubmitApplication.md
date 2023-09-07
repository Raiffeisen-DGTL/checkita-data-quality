# Submit Application

Поскольку **Checkita** работает на базе Spark, то запуск фреймворка выполняется посредством
обычной `spark-submit` команды.

Аналогично любому Spark-приложению, расчет Data Quality может быть запушен как в локальном режиме, так и на кластере
(в `client` или `cluster` mode).

В сам фреймворк при запуске необходимо передать следующие аргументы:

* `-a`: путь до файла с настройками приложения `application.conf`;
* `-c`: путь до конфигурационного файла с описанием источников, метрик, проверок и т.д.;
* `-d [optional]`: дата за которую выполняется расчет в формате, указанном в `application.conf`
  (по умолчанию дата запуска приложения).
* `-l [optional]`: флаг, указывающий на то, что приложение нужно запустить в локальном режиме.
* `-r [optional]`: флаг, указывающий на необходимость репартиционирования источников после чтения.
* `-e [optional]`: флаг, в рамках которого можно передать дополнительные переменные при старте приложения.
  Эти переменные будут добавлены в начало конфигурационного файла, указанного после флага `-c` и, таким образом, могут
  быть использованы в описании источников, метрик и проверок.
  Переменные передаются в формате ключ-значение: `"k1=v1,k2=v2,k3-v3,.."`
* `-v [optional]`: флаг, в рамках которого также можно передать дополнительные переменные при старте приложения.
  Однако, эти переменные будут добавлены в начало файла с настройками приложения `application.conf`, который указан
  после флага `-a`. Таким образом, эти переменные могут быть использованы для передачи, например, секретов для
  подключения к SMTP или Mattermost серверам.
  Переменные также передаются в формате ключ-значение: `"k1=v1,k2=v2,k3-v3,.."`

Ниже показан пример запуска приложения в YARN в _**cluster mode**_. Параметры подключения к базе данных фреймворка
указываются либо в application.conf либо в параметрах Spark-приложения (см. [Application Settings](./ApplicationSettings.md)).
В данном примере показан второй способ.

```bash
export DQ_APPLICATION=<локальный или HDFS путь до uber-jar приложения>
export DQ_APP_CONFIG=<локальный или HDFS путь до application.conf>
export DQ_LOG_CONFIG=<локальный или HDFS путь до log4j.properties>
export DQ_METRICS_CONFIG=<локальный или HDFS путь до конфигурационного файла>

export DQ_APP_CONFIG_FILE=$(basename $DQ_APP_CONFIG)
export DQ_METRICS_CONFIG_FILE=$(basename $DQ_METRICS_CONFIG)
export REFERENCE_DATE="2023-08-01"

# точка входа в приложение (исполняемый класс):
# ru.raiffeisen.checkita.apps.DQMasterBatch

spark-submit \
  --class ru.raiffeisen.checkita.apps.DQMasterBatch \
  --name "Checkita Data Quality" \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 1 \
  --executor-memory 2g \
  --executor-cores 4 \
  --driver-memory 2g \
  --files $DQ_APP_CONFIG,$DQ_LOG_CONFIG,$DQ_METRICS_CONFIG \
  --conf "spark.yarn.queue=data_science" \
  --conf "spark.default.parallelism=10" \
  --conf "spark.executor.memoryOverhead=2g" \
  --conf "spark.driver.memoryOverhead=2g" \
  --conf "spark.driver.maxResultSize=4g" \
  --conf "spark.jdbc.db_type=<тип БД>" \
  --conf "spark.jdbc.host=<url для подключения>" \
  --conf "spark.jdbc.login=<логин>" \
  --conf "spark.jdbc.password=<пароль>" \
  --conf "spark.jdbc.db_schema=<схема>" \
  $DQ_APPLICATION \
  -a $DQ_APP_CONFIG_FILE -c $DQ_LOG_CONFIG_FILE -d $REFERENCE_DATE \
  -e "external_db_user=some_db_user,external_db_password=some_db_password" \
  -v "mattermostToken=someToken"
```