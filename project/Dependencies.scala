import sbt._

object Dependencies {

  val pureConfig = "com.github.pureconfig" %% "pureconfig" % "0.17.3"
  val enumeratum = "com.beachape" %% "enumeratum" % "1.7.2"
  val isarn = "org.isarnproject" %% "isarn-sketches" % "0.3.0"
  val algebird = "com.twitter" %% "algebird-core" % "0.13.9"
  val commonText = "org.apache.commons" % "commons-text" % "1.10.0"
  val commonMail = "org.apache.commons" % "commons-email" % "1.5"
  val mustache = "com.github.spullara.mustache.java" % "compiler" %"0.9.10"

  // XML support in json4s published in a separate package,
  // and for latests Spark versions (starting from 3.4) using json4x-xml would
  // either be incompatible with json4s-core or with scala-xml packages.
  // Therefore, we will use Java Json library for XML to JSON conversions:
  val jsonJava = "org.json" % "json" % "20231013"

  // refined libraries:
  val refinedVersion = "0.10.3"
  val refined = "eu.timepit" %% "refined" % refinedVersion
  val refinedScopt = "eu.timepit" %% "refined-scopt" % refinedVersion
  val refinedPureConfig = "eu.timepit" %% "refined-pureconfig" % refinedVersion

  // database drivers:
  val postgres = "org.postgresql" % "postgresql" % "42.5.4"
  val oracle = "com.oracle.database.jdbc" % "ojdbc8" % "23.2.0.0"
  val sqlite = "org.xerial" % "sqlite-jdbc" % "3.42.0.0"
  val mysql = "mysql" % "mysql-connector-java" % "8.0.33"
  val mssql = "com.microsoft.sqlserver" % "mssql-jdbc" % "12.2.0.jre8"
  val mssqlJTDS = "net.sourceforge.jtds" % "jtds" % "1.3.1"
  val h2db = "com.h2database" % "h2" % "1.4.196"

  // Slick
  val slickVersion = "3.4.1"
  val slick = "com.typesafe.slick" %% "slick" % slickVersion
  val slickHCP = "com.typesafe.slick" %% "slick-hikaricp" % slickVersion

  // FlyWay
  val flywayVersion = "9.21.1"
  val flyway = "org.flywaydb" % "flyway-core" % flywayVersion
  val flywayMySQL = "org.flywaydb" % "flyway-mysql" % flywayVersion
  val flywayOracle = "org.flywaydb" % "flyway-database-oracle" % flywayVersion
  val flywayMSSQL = "org.flywaydb" % "flyway-sqlserver" % flywayVersion

  // S3
  val hadoopAws = "org.apache.hadoop" % "hadoop-aws" % "3.3.2"

  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15" % Test
  
  val checkita_core: Seq[ModuleID] = Seq(
    pureConfig,
    enumeratum,
    isarn,
    algebird,
    commonText,
    commonMail,
    mustache,
    jsonJava,
    refined,
    refinedScopt,
    refinedPureConfig,
    postgres,
    oracle,
    sqlite,
    mysql,
    mssql,
    mssqlJTDS,
    h2db,
    slick,
    slickHCP,
    flyway,
    flywayMySQL,
    flywayOracle,
    flywayMSSQL,
    hadoopAws,
    scalaTest
  )


//   val refinedCats = "eu.timepit" %% "refined-cats" % Versions.refined
//   val refinedEval = "eu.timepit" %% "refined-eval" % Versions.refined
//   val catsCore = "org.typelevel" %% "cats-core" % Versions.cats
//   val scalate = "org.scalatra.scalate" %% "scalate-core" % Versions.scalate
}
