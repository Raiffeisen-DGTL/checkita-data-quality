package ru.raiffeisen.checkita.appsettings

import org.json4s.jackson.Serialization.write
import ru.raiffeisen.checkita.utils.ResultUtils._
import ru.raiffeisen.checkita.utils.Common.jsonFormats

import java.util.Properties
import scala.util.Try

/**
 * Case class used to store current application version
 * and config API version.
 */
case class VersionInfo(appVersion: String, configAPIVersion: String) {
  lazy val asJsonString: String = write(this)
}

object VersionInfo {

  /**
   * Loads version information from corresponding properties file located in resources folder.
   *
   * @return Instance of VersionInfo
   */
  def loadVersion: Result[VersionInfo] = Try {
    val versionInfo = getClass.getResourceAsStream("/version-info.properties")
    val verProps = new Properties()
    verProps.load(versionInfo)
    VersionInfo(
      verProps.getProperty("app-version", "<unknown>"),
      verProps.getProperty("config-api-version", "<unknown>")
    )
  }.toResult("Unable to load version information due to following error:")

  /**
   * Returns instance of VersionInfo with all versions set to "unknown".
   * @return Instance of VersionInfo
   */
  def unknown: VersionInfo = VersionInfo("<unknown>", "<unknown>")
}
