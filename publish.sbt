import sbt._
import xerial.sbt.Sonatype.sonatypeCentralHost

ThisBuild / organization         := "org.checkita"
ThisBuild / organizationName     := "Checkita"
ThisBuild / organizationHomepage := Some(url("https://www.checkita.org/"))
ThisBuild / description          := "Fast data quality framework for modern data infrastructure."
ThisBuild / licenses             := List("LGPL 3.0" -> new URL("https://www.gnu.org/licenses/lgpl-3.0.txt"))
ThisBuild / homepage             := Some(url("https://www.checkita.org/"))
ThisBuild / publishTo            := publishRepo.value
ThisBuild / publishMavenStyle    := true
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/Raiffeisen-DGTL/checkita-data-quality"),
    "scm:git@github.com:Raiffeisen-DGTL/checkita-data-quality.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "gabb1er",
    name = "Yury Dylko",
    email = "yury.dylko@proton.me",
    url = url("https://github.com/gabb1er")
  ),
  Developer(
    id = "emakhov",
    name = "Egor Makhov",
    email = "e.makhov@protonmail.com",
    url = url("https://github.com/emakhov")
  )
)

// Sonatype Settings:
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
