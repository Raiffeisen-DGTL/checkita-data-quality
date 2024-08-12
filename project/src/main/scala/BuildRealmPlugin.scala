package src.main.scala

import sbt._
import sbt.{AllRequirements, AutoPlugin, Resolver, Setting, settingKey}
import sbt.Keys.onLoadMessage
import sbt.plugins.JvmPlugin
import xerial.sbt.Sonatype.autoImport.sonatypePublishToBundle

/** Sets Realm and repository host for project publishing */
object BuildRealmPlugin extends AutoPlugin {

  // make sure it triggers automatically
  override def trigger = AllRequirements

  override def requires = JvmPlugin

  object autoImport {
    val publishRepo = settingKey[Option[Resolver]]("repository to publish project to")
  }
  
  private def getRepository: Option[Resolver] = {
    val localRepo = Some(Resolver.file("local", file(Path.userHome.getPath + "/.sbt/releases")))
    
    val publishRealm = sys.props.get("PUBLISH_REALM")
      .orElse(sys.env.get("PUBLISH_REALM"))
    val publishUrl = sys.props.get("PUBLISH_URL")
      .orElse(sys.env.get("PUBLISH_URL"))

    val repo = for {
      realm <- publishRealm
      url <- publishUrl
    } yield realm at url
    
    repo.orElse(localRepo)
  }
  
  import autoImport._

  override def projectSettings: Seq[Setting[_]] = Seq(
    
    publishRepo := getRepository.filter{
      case mvn: MavenRepository if mvn.name.toUpperCase == "SONATYPE" => false
      case _ => true
    }.orElse(sonatypePublishToBundle.value),
    
    // give feed back
    onLoadMessage := {
      // depend on the old message as well
      val defaultMessage = onLoadMessage.value
      val resolverStr = publishRepo.value.map(_.toString()).getOrElse("None")
      s"""|$defaultMessage
          |Current Publish Repository: $resolverStr""".stripMargin
    }
  )
}
