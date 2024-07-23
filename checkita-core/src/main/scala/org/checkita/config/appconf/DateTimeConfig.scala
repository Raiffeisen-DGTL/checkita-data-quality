package org.checkita.config.appconf

import java.time.ZoneId
import org.checkita.config.RefinedTypes.DateFormat


/**
 * Application-level configuration describing datetime settings
 * @param timeZone Timezone used to render date and time
 * @param referenceDateFormat Date format used to represent reference date
 * @param executionDateFormat Date format used to represent execution date
 */
case class DateTimeConfig(
                           timeZone: ZoneId = ZoneId.of("UTC"),
                           referenceDateFormat: DateFormat = DateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                           executionDateFormat: DateFormat = DateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                         )
