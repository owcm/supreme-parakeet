import sbt.Keys._

resolvers += "Geotools" at "http://download.osgeo.org/webdav/geotools/"

resolvers += Resolver.bintrayRepo("azavea", "geotrellis")

libraryDependencies ++= Seq(
 "io.spray" %% "spray-routing" % "1.3.3",
 "io.spray" %% "spray-can" % "1.3.3",
 "com.amazonaws" % "aws-java-sdk-s3" % "1.10.38",
 "com.amazonaws" % "aws-java-sdk-emr" % "1.10.38",
 "org.apache.camel" % "camel-scala" % "2.10.1",
 "com.azavea.geotrellis" %% "geotrellis-engine" % "0.10.0-RC2",
  "com.azavea.geotrellis" %% "geotrellis-raster" % "0.10.0-RC2",
 "org.scalaz.stream" %% "scalaz-stream" % "0.7.3a",
 "org.apache.spark" %% "spark-core" % "1.5.2",
 "org.apache.spark" %% "spark-yarn" % "1.5.2",
 "org.apache.hadoop" % "hadoop-client" % "2.7.1" % "runtime",
 "org.apache.hadoop" % "hadoop-yarn-client" % "2.7.1" % "runtime",
 "com.azavea.geotrellis" %% "geotrellis-spark" % "0.10.0-RC2",
 "com.azavea.geotrellis" %% "geotrellis-shapefile" % "0.10.0-RC2",
  "net.liftweb" %% "lift-webkit" % "2.6",
 "net.liftweb" %% "lift-json" % "2.6"

)



mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case PathList("reference.conf") => MergeStrategy.concat
    case x => MergeStrategy.first
   }
}

lazy val commonSettings = Seq(
 version := "0.1.0",
 scalaVersion := "2.10.6"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
   name := "Surge Backend"
  )

lazy val surgesparkhandler = (project in file("surgesparkhandler")).
  settings(
   name := "surgesparkhandler"
  )




