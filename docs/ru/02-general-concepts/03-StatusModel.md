# Статусная Модель Результатов

Единая статусная модель используется для результатов, которые получаются в процессе работы фреймворка Checkita.
Результаты расчета как метрик, так и проверок имеют общие индикаторы их статусов, а именно:

* `Success` - Вычисление метрики или проверки завершилось без ошибок и условие заданное в метрике или проверке выполнено.
* `Failure` - В ходе вычисления метрики или проверки были получены результаты, которые не удовлетворяют условию
  данной метрики или проверки. Например:
    * `regexMatch` метрика получила на вход значение колонки, которое не совпадает с указанным регулярным выражением.
    * Проверка, которая требует, чтобы значение метрики было равно нулю, получила не нулевое ее значение.
* `Error` - Обнаружена ошибка во время выполнения расчета метрики или проверки. Также перехватывается сообщение об ошибке.

Во всех типах результатов статус сопровождается сообщением, которое его описывает. Однако, есть различия в том,
как статусы могут быть получены пользователем:

* Во время расчета метрик, статус вычисления получается для каждой строки датасета. Если статус вычисления метрики для
  какой-либо строки отличается от `Success`, то для этой строки будет записана ошибка расчета данной метрики.
  Далее, можно запросить отчет об ошибках при расчете метрик, конфигурация которого описана в главе
  [Отчеты об ошибках](???). Подробнее о сборе ошибок по расчету метрик, см. главу 
  [Сбор Ошибок Вычисления Метрик](04-ErrorCollection.md).
* Что касается проверок, то для них статус - это основной индикатор, сигнализирующий об успешности проверки.
  Поэтому, статусы проверок записываются в хранилище результатов, как и подробное сообщения описывающие результаты проверок.