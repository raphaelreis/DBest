name := "DBest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.3" % Test

libraryDependencies ++= {
  val sparkVersion = "2.4.6"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
  )
}

libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "1.0",
  "org.scalanlp" %% "breeze-natives" % "1.0",
  "org.scalanlp" %% "breeze-viz" % "1.0",
)

libraryDependencies ++= Seq(
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
)

libraryDependencies += "com.github.haifengl" %% "smile-scala" % "2.5.3"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>

"dbest." + artifact.extension

}
