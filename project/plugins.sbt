addDependencyTreePlugin
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.4") // Creates fat Jars
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.6.1") // Creates universal packages
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1") // Provides PGP signing
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.11.3") // For sonatype releases
