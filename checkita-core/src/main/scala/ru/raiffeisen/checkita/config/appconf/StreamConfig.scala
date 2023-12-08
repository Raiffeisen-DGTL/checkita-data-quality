package ru.raiffeisen.checkita.config.appconf

import scala.concurrent.duration.Duration
import java.util.UUID

/**
 * Application-level configuration describing streaming settings
 *
 * @param trigger           Trigger interval: defines time interval for which micro-batches are collected.
 * @param window            Window interval: defines tabbing window size used to accumulate metrics.
 * @param watermark         Watermark level: defines time interval after which late records are no longer processed.
 * @param allowEmptyWindows Boolean flag indicating whether empty windows are allowed. Thus, in situation when
 *                          window is below watermark and for some of the processed streams there are no results then
 *                          all related checks will be skipped if this flag is set to 'true'. Otherwise, checks will
 *                          be processed and return error status with 'metric results were not found' message.
 */
case class StreamConfig(
                         trigger: Duration = Duration("10s"),
                         window: Duration = Duration("10m"),
                         watermark: Duration = Duration("5m"),
                         allowEmptyWindows: Boolean = false
                       ) {
  // random column names are generated to be used for windowing:
  lazy val windowTsCol: String = UUID.randomUUID.toString.replace("-", "")
  lazy val eventTsCol: String = UUID.randomUUID.toString.replace("-", "")
}
