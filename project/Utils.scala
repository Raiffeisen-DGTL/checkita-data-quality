import sbt._
import src.main.scala.BuildAssyModePlugin.autoImport.AssyMode
import src.main.scala.BuildPackageTypePlugin.autoImport.PackageType

object Utils {
  
  def getSparkDependencies(sparkVersion: String, assyMode: AssyMode.Value): Map[String, ModuleID] = {

    lazy val jpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

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
    ).mapValues(m => m excludeAll jpountz)

    sparkDeps ++ sparkKafkaDeps + ("sparkAvro" -> "org.apache.spark" %% "spark-avro" % sparkVersion)
  }
  
  def getVersionString(buildVersion: String, packageType: PackageType.Value): String = packageType match {
    case PackageType.Release => buildVersion
    case otherType => s"$buildVersion-${otherType.toString}"
  }
}
