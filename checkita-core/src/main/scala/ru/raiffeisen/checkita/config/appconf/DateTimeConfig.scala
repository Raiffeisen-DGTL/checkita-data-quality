package ru.raiffeisen.checkita.config.appconf

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import ru.raiffeisen.checkita.config.RefinedTypes.DateFormat


/**
 * Application-level configuration describing datetime settings
 * @param timeZone Timezone used to render date and time
 * @param referenceDateFormat Date format used to represent reference date
 * @param executionDateFormat Date format used to represent execution date
 */
case class DateTimeConfig(
                           timeZone: ZoneId = ZoneId.of("UTC"),
                           referenceDateFormat: DateFormat = DateFormat(
                             pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS",
                             formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
                           ),
                           executionDateFormat: DateFormat = DateFormat(
                             pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS",
                             formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
                           )
                         )
