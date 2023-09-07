import sbt._

object Dependencies {
  
  // CommandLine Options Parser:
  val scopt  = "com.github.scopt" %% "scopt" % "4.1.0"
  
  // Databases:
  val postgres = "org.postgresql" % "postgresql" % "42.5.4"
  val oracle = "com.oracle.database.jdbc" % "ojdbc8" % "23.2.0.0"
  val sqlite = "org.xerial" % "sqlite-jdbc" % "3.42.0.0"
  
  // Twitter Algebird: Abstract algebra for Scala:
  val algebirdCore = "com.twitter" %% "algebird-core" % "0.13.9"
  
  // TDigest Library:
  val tdigest = "org.isarnproject" %% "isarn-sketches" % "0.3.0"
  
  // Commons:
  val commonText = "org.apache.commons" % "commons-text" % "1.10.0"
  val commonMail = "org.apache.commons" % "commons-email" % "1.5"
  
  // JSON:
  val json = "org.json" % "json" % "20230227"
  
  // TypeSafe Config Library for parsing Hocon files:
  val typesafeConfig  = "com.typesafe" % "config" % "1.4.2"
  
  // ScalaTest
  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15" % Test
  
  val checkita_core = Seq(
    algebirdCore,
    commonText,
    commonMail,
    scopt,
    typesafeConfig,
    tdigest,
    postgres,
    oracle,
    sqlite,
    json,
    scalaTest
  )
}