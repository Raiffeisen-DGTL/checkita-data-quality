addDependencyTreePlugin
addSbtPlugin("com.eed3si9n"   % "sbt-assembly"        % "2.1.4")  // Creates fat Jars
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.1") // Creates universal packages
addSbtPlugin("com.github.sbt" % "sbt-pgp"             % "2.2.1")  // Provides PGP signing

libraryDependencies ++= Seq(
  "org.json4s"               %% "json4s-native" % "4.0.7",
  "org.apache.httpcomponents" % "httpcore"      % "4.4.13",
  "org.apache.httpcomponents" % "httpclient"    % "4.5.13",
  "org.apache.httpcomponents" % "httpmime"      % "4.5.13"
)
