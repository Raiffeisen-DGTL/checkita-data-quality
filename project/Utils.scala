import sbt._
import src.main.scala.BuildAssyModePlugin.autoImport.AssyMode
import src.main.scala.BuildPackageTypePlugin.autoImport.PackageType

import scala.util.matching.Regex

object Utils {
  
  def getSparkDependencies(sparkVersion: String, assyMode: AssyMode.Value): Map[String, ModuleID] = {

    lazy val jpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
    lazy val hadoop = ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-client-runtime")

    val hadoopSparkMatching: Map[Regex, String] = Map(
      "2\\.4\\.[0-8]".r -> "2.6.5",
      "3\\.0\\.[0-3]".r -> "2.7.4",
      "3\\.1\\.[0-3]".r -> "3.2.0",
      "3\\.2\\.[0-4]".r -> "3.3.1",
      "3\\.3\\.[0-3]".r -> "3.3.2",
      "3\\.4\\.[0-2]".r -> "3.3.4",
      "3\\.5\\.[0-1]".r -> "3.3.4"
    )

    val sparkDeps: Map[String, ModuleID] = Map(
      "sparkCore" -> "org.apache.spark" %% "spark-core" % sparkVersion,
      "sparkSql" -> "org.apache.spark" %% "spark-sql" % sparkVersion,
      "sparkHive" -> "org.apache.spark" %% "spark-hive" % sparkVersion,
      "sparkStreaming" -> "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "sparkCatalyst" -> "org.apache.spark" %% "spark-catalyst" % sparkVersion
    ).mapValues(m => if (assyMode == AssyMode.WithSpark) m else m % "provided")

    val sparkKafkaDeps: Map[String, ModuleID] = Map(
      "sparkKafkaStreaming" -> "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
      "sparkKafkaSql" -> "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
    ).mapValues(_.excludeAll(jpountz, hadoop))

    // we use log4j2 for logging. For newer versions of spark it comes as a transitive dependency.
    // But for older versions of spark this dependency needs to be explicit.
    val log4j2 = if (sparkVersion < "3.3.0") Map(
      "log4j-core" -> "org.apache.logging.log4j" % "log4j-core" % "2.19.0",
      "log4j-api" -> "org.apache.logging.log4j" % "log4j-api" % "2.19.0",
      "log4j-slf4j" -> "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.19.0"
    ) else Map.empty[String, ModuleID]

    val extraDeps: Map[String, ModuleID] = Map(
      "sparkAvro" -> "org.apache.spark" %% "spark-avro" % sparkVersion,
      "hadoopAws" -> "org.apache.hadoop" % "hadoop-aws" % hadoopSparkMatching.collectFirst {
        case (key, value) if key.findFirstMatchIn(sparkVersion).isDefined => value
      }.get
    )

    sparkDeps ++ sparkKafkaDeps ++ extraDeps ++ log4j2
  }

  def getExcludeDependencies(sparkVersion: String): Seq[ExclusionRule] =
    if (sparkVersion < "3.3.0") Seq(
      ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
    ) else Seq.empty[ExclusionRule]

  def overrideFasterXml(sparkVersion: String): Seq[ModuleID] = {
    val fasterXmlVersion = sparkVersion match {
      case v2 if v2.startsWith("2.") => Some("2.6.7")
      case v30 if v30.startsWith("3.0") => Some("2.10.0")
      case v31 if v31.startsWith("3.1") => Some("2.10.0")
      case v32 if v32.startsWith("3.2") => Some("2.12.3")
      case v330 if v330 == "3.3.0" => Some("2.13.3")
      case v33x if v33x.startsWith("3.3") => Some("2.13.4")
      case v34 if v34.startsWith("3.4") => Some("2.14.2")
      case v35 if v35.startsWith("3.5") => Some("2.15.2")
      case _ => None
    }
    
    fasterXmlVersion.map{ version =>
      Seq(
        "com.fasterxml.jackson.core" % "jackson-core" % version,
        "com.fasterxml.jackson.core" % "jackson-databind" % version,
        "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % version
      )
    }.getOrElse(Seq.empty)
  }
  
  def getVersionString(buildVersion: String, packageType: PackageType.Value): String = packageType match {
    case PackageType.Release => buildVersion
    case otherType => s"$buildVersion-${otherType.toString}"
  }
}
