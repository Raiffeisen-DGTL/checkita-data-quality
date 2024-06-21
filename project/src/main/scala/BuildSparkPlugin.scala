package src.main.scala

import sbt.Keys.{onLoadMessage, scalaVersion, scalacOptions}
import sbt.plugins.JvmPlugin
import sbt.{AllRequirements, AutoPlugin, Setting, settingKey}

object BuildSparkPlugin extends AutoPlugin {
  object autoImport {
    val sparkVersion = settingKey[String]("The version of Apache Spark used for building")
  }

  import autoImport._

  // make sure it triggers automatically
  override def trigger = AllRequirements
  override def requires = JvmPlugin

  private def getSparkVersion: String = {
    val sparkVer = sys.props.get("SPARK_VERSION")
      .orElse(sys.env.get("SPARK_VERSION"))
      .getOrElse("3.2.0")

    if (sparkVer < "3.2.0") throw new IllegalArgumentException(
      s"Checkita Data Quality works with Spark version >= 3.2.0 but $sparkVer is set"
    ) else sparkVer
  }
  
  private def getScalaVersion(sparkVer: String): String = {
    val scalaFeatVersion = sys.props.get("SCALA_VERSION")
      .orElse(sys.env.get("SCALA_VERSION"))
      .getOrElse("2.12")
    
    if (scalaFeatVersion == "2.12") {
      if (sparkVer <= "3.4.0") "2.12.16" else "2.12.18"
    } else if (scalaFeatVersion == "2.13") {
      if (sparkVer <= "3.3.0") "2.13.5" else "2.13.8"
    } else throw new IllegalArgumentException(
      "Checkita Data Quality can be built either for Scala 2.12 or Scala 2.13 " +
        f"but Scala feature-version of $scalaFeatVersion is set."
    )
  }
  
  override def projectSettings: Seq[Setting[_]] = Seq(
    sparkVersion := getSparkVersion,
    scalaVersion := getScalaVersion(sparkVersion.value),
    onLoadMessage := {
      s"""|${onLoadMessage.value}
          |Current Spark version: ${sparkVersion.value}
          |Current Scala version: ${scalaVersion.value}""".stripMargin
    }
  )
}
