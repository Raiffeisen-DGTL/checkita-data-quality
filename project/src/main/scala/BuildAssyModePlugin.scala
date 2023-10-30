package src.main.scala

import sbt.Keys.onLoadMessage
import sbt.plugins.JvmPlugin
import sbt.{AllRequirements, AutoPlugin, Setting, settingKey}

/** sets the build environment */
object BuildAssyModePlugin extends AutoPlugin {

  // make sure it triggers automatically
  override def trigger = AllRequirements

  override def requires = JvmPlugin

  object autoImport {
    object AssyMode extends Enumeration {
      val WithSpark, NoSpark = Value
    }

    val assyMode = settingKey[AssyMode.Value]("the current assembly mode")
  }

  import autoImport._

  override def projectSettings: Seq[Setting[_]] = Seq(
    assyMode := {
      sys.props.get("ASSY_MODE")
        .orElse(sys.env.get("ASSY_MODE"))
        .map(_.toLowerCase)
        .flatMap {
          case "withspark" => Some(AssyMode.WithSpark)
          case "nospark" => Some(AssyMode.NoSpark)
          case _ => None
        }.getOrElse(AssyMode.NoSpark)
    },
    // give feed back
    onLoadMessage := {
      // depend on the old message as well
      val defaultMessage = onLoadMessage.value
      val mode = assyMode.value
      s"""|$defaultMessage
          |Running with assembly mode: $mode""".stripMargin
    }
  )

}
