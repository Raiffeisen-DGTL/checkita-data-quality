package src.main.scala

import sbt.Keys.{onLoadMessage, scalaVersion}
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

  override def projectSettings: Seq[Setting[_]] = Seq(
    sparkVersion := sys.props.get("SPARK_VERSION").orElse(sys.env.get("SPARK_VERSION")).getOrElse("2.4.0"),
    scalaVersion := {
      if (sparkVersion.value <= "3.4.0") "2.12.16" else "2.12.18"
    },
    onLoadMessage := {
      if (sparkVersion.value < "2.4.0") throw new IllegalArgumentException(
        s"Checkita Data Quality works with Spark version >= 2.4.0 but ${sparkVersion.value} is set"
      )
      s"""|${onLoadMessage.value}
          |Current Spark version: ${sparkVersion.value}
          |Current Scala version: ${scalaVersion.value}""".stripMargin
    }
  )
}
