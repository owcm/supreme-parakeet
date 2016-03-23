

name := "surgesparkhandler"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.6"

crossScalaVersions := Seq("2.11.8", "2.10.6")
organization := "com.azavea"
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-Yinline-warnings",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials",
  "-feature",
  "-target:jvm-1.7"
)
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

shellPrompt := { s => Project.extract(s).currentProject.id + " > " }

resolvers += Resolver.bintrayRepo("azavea", "geotrellis")

// We need to bump up the memory for some of the examples working with the landsat image.
javaOptions += "-Xmx8G"

fork in run := true

connectInput in run := true

libraryDependencies ++= Seq(
  "com.azavea.geotrellis" %% "geotrellis-engine" % "0.10.0-RC2",
  "com.azavea.geotrellis" %% "geotrellis-raster" % "0.10.0-RC2",
  "com.azavea.geotrellis" %% "geotrellis-spark" % "0.10.0-RC2",
  "com.azavea.geotrellis" %% "geotrellis-shapefile" % "0.10.0-RC2",
  //"com.azavea.geotrellis" %% "geotrellis-spark" % "0.10.0-2283ec5",
  //"com.azavea.geotrellis" %% "geotrellis-geotools" % "0.10.0-M1",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.10.30",
  "org.scalaz.stream" %% "scalaz-stream" % "0.7.3a",
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-yarn" % "1.5.2",
  "org.apache.hadoop" % "hadoop-client" % "2.7.1",
  "org.apache.hadoop" % "hadoop-yarn-client" % "2.7.1",
  "net.liftweb" %% "lift-webkit" % "2.6",
  "net.liftweb" %% "lift-json" % "2.6",
  "org.scalatest"       %%  "scalatest"      % "2.2.0" % "test"
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)


// META-INF discarding
assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
    