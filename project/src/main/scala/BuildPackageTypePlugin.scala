package src.main.scala

import sbt.Keys.onLoadMessage
import sbt.plugins.JvmPlugin
import sbt.{AllRequirements, AutoPlugin, Setting, settingKey}

/** Sets the package type to build */
object BuildPackageTypePlugin extends AutoPlugin {

  // make sure it triggers automatically
  override def trigger = AllRequirements

  override def requires = JvmPlugin

  object autoImport {
    object PackageType extends Enumeration {
      val Snapshot, Dev, RC1, RC2, RC3, RC4, RC5, Release = Value
    }

    val packageType = settingKey[PackageType.Value]("the current publish type")
  }

  import autoImport._

  override def projectSettings: Seq[Setting[_]] = Seq(
    packageType := {
      sys.props.get("PKG_TYPE")
        .orElse(sys.env.get("PKG_TYPE"))
        .map(_.toLowerCase)
        .flatMap {
          case "snapshot" => Some(PackageType.Snapshot)
          case "dev" => Some(PackageType.Dev)
          case "rc1" => Some(PackageType.RC1)
          case "rc2" => Some(PackageType.RC2)
          case "rc3" => Some(PackageType.RC3)
          case "rc4" => Some(PackageType.RC4)
          case "rc5" => Some(PackageType.RC5)
          case "release" => Some(PackageType.Release)
          case _ => None
        }.getOrElse(PackageType.Snapshot)
    },
    // give feed back
    onLoadMessage := {
      // depend on the old message as well
      val defaultMessage = onLoadMessage.value
      val pType = packageType.value
      s"""|$defaultMessage
          |[BUILD OPTION] PACKAGE_TYPE\t= $pType""".stripMargin
    }
  )

}
