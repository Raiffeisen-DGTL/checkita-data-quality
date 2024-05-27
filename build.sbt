import sbt._

ThisBuild / version          := Version.releaseVersion
ThisBuild / organization     := "ru.raiffeisen"
ThisBuild / organizationName := "Raiffeisen"
ThisBuild / versionScheme    := Some("semver-spec")
ThisBuild / publishTo        := publishRepo.value
ThisBuild / credentials      += Credentials(Path.userHome / ".sbt" / ".credentials")
ThisBuild / resolvers        += "Confluent IO" at "https://packages.confluent.io/maven/"
ThisBuild / scalacOptions    ++= Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-language:higherKinds"
)

lazy val `checkita-data-quality` = (project in file("."))
  .aggregate(`checkita-core`)
  .settings(publish / skip := true)

lazy val `checkita-core` = (project in file("checkita-core")).settings(
  libraryDependencies ++= {
    Dependencies.checkita_core ++
      Utils.getSparkDependencies(sparkVersion.value, assyMode.value).values
  },

  excludeDependencies ++= Utils.getExcludeDependencies(sparkVersion.value),

  Compile / doc / target := baseDirectory.value / ".." / "docs/api",

  dependencyOverrides ++= Utils.overrideFasterXml(sparkVersion.value),

  version := Utils.getVersionString((ThisBuild / version).value, packageType.value),

  assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}-uber.jar",
  assemblyPackageDependency / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}-deps.jar",

  assemblyPackageDependency / assemblyOption ~= {
    _.withIncludeScala(false)
  },
  assemblyMergeStrategy := {
    case x if Assembly.isConfigFile(x) => MergeStrategy.concat
    case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
      MergeStrategy.rename
    case PathList("models", xs@_*) => MergeStrategy.discard // aws-java-sdk-bundle contains lots of json data
    case PathList("META-INF", xs@_*) => xs map {
      _.toLowerCase
    } match {
      case "manifest.mf" :: Nil | "index.list" :: Nil | "dependencies" :: Nil => MergeStrategy.discard
      case ps@x :: xs if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") => MergeStrategy.discard
      case "plexus" :: xs => MergeStrategy.discard
      case "services" :: xs => MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil => MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
    case _ => MergeStrategy.first
  }
)