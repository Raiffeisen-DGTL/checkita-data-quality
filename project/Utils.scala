import sbt._
import src.main.scala.BuildAssyModePlugin.autoImport.AssyMode
import src.main.scala.BuildPackageTypePlugin.autoImport.PackageType

object Utils {
  
  def getSparkDependencies(sparkVersion: String, assyMode: AssyMode.Value): Map[String, ModuleID] = {

    lazy val jpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
    lazy val hadoop = ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-client-runtime")

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

    sparkDeps ++ sparkKafkaDeps + ("sparkAvro" -> "org.apache.spark" %% "spark-avro" % sparkVersion)
  }
  
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
