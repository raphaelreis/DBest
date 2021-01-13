name := "DBest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.3" % Test

libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.9.0"

libraryDependencies += "com.typesafe" % "config" % "1.4.0"

libraryDependencies ++= {
  val sparkVersion = "2.4.6"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
  )
}

libraryDependencies  ++= {
  val breezeVersion = "0.13.2"
  Seq(
  "org.scalanlp" %% "breeze" % "1.0",
  "org.scalanlp" %% "breeze-natives" % "1.0",
  "org.scalanlp" %% "breeze-viz" % "1.0",
  )
}

libraryDependencies ++= Seq(
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
)

libraryDependencies ++= {
  val xgboost4jVersion = "1.1.2"
  Seq(
  "ml.dmlc" %% "xgboost4j" % xgboost4jVersion,
  "ml.dmlc" %% "xgboost4j-spark" % xgboost4jVersion
  )
}

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.4.11"


// libraryDependencies ++= {
//   val jacksonVersion = "2.6.7"
//   Seq(
//   "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
//   "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
//   "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion

//   )
// }

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>

"dbest." + artifact.extension

}
