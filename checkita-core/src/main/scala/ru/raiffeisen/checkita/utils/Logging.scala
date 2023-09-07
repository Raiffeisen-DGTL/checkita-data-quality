package ru.raiffeisen.checkita.utils

import org.apache.log4j.Logger

trait Logging {
  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)
}
