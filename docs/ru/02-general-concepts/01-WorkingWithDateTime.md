# Работа с Датами

> Здесь и далее под датой понимается DateTime объект, хранящий как дату, так и время

В фреймворке Checkita существует два основных представления даты, которые используются для идентификации различных
запусков Data Quality пайплайнов:

* `referenceDate` - дата, которая указывает на то, за какой период запускается и выполняется указанный Data Quality пайплайн.
* `executionDate` - дата, которая хранит фактическое время запуска Data Quality пайплайна.

Типовой пример: мы запускаем какой-либо ETL пайплайн (также содержит задачу по расчету метрик качества данных) после
закрытия бизнес дня, например в полночь. Таким образом, `referenceDate` будет указывать на предыдущий день, за который
мы выполняем ETL, а `executionDate` будет хранить текущую дату - дату фактического запуска Data Quality пайплайна.
Вероятно, у нас появится потребность в том, чтобы строковые представления этих дат отличались. И фреймворк Checkita дает
нам такую возможность, позволяя настраивать индивидуальные форматы представления для каждой из этих дат в настройках
приложения.

Так как `referenceDate` может указывать на прошедшие даты, то ее можно явно указывать в при старте приложения.
Если эта дата не указана явно, то она будет также соответствовать фактической дате старта приложения.
См. главу [Запуск Приложений Data Quality](../01-application-setup/02-ApplicationSubmit.md) для более подробной информации об аргументах,
используемых при запуске приложений Data Quality.

Обе эти даты широко используются внутри фреймворка. Поэтому, в любых ситуациях, когда нам требуется их строковое
представление, оно получается в соответствии с тем форматом, который указан в настройках приложения.

Также нужно отметить, что строковое представление формируется с учетом временной зоны, в которой запускается приложение.
Временна зона также задается в настройках приложения. По умолчанию используется зона `UTC`.

В дополнение, но не менее важно: мы сознательно избегаем использования строкового представления дат при сохранении
результатов в базу данных. В этом случае даты конвертируются в тип Timestamp и приводятся к временной зоне `UTC`.
Такой подход позволяет надежно строить запросы к хранилищу результатов, которые не будут зависеть от настроек
представления дат. См. главу [Хранилище Результатов](../01-application-setup/03-ResultsStorage.md) для более подробной информации о хранилище
результатов.

> **ВАЖНО**: Фактические строковые представления `referenceDate` и `exectionDate` всегда добавляются в конфигурационные
> файлы в качестве дополнительных переменны. Для более подробной информации об использовании дополнительных 
> переменных, см. главу 
> [Использование Переменных Окружения и Дополнительных Переменных](02-EnvironmentAndExtraVariables.md).