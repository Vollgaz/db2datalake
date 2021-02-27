name := "db2datalake"
organization := "com.github.vollgaz"
scalaVersion := "2.12.12"

libraryDependencies ++= {
  val sparkVersion = "3.0.2"

  Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.0" % Provided,
    //"com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.7.2.0.0-237" % Provided,
    "com.github.scopt" %% "scopt" % "4.0.0",
    "org.scalatest" %% "scalatest" % "3.2.0" % Test,
    "org.scalatest" %% "scalatest-featurespec" % "3.2.0" % Test,
    "org.xerial" % "sqlite-jdbc" % "3.34.0" % Test
  )
}

// resolvers := List(
//     "Cloudera Release" at "https://repository.cloudera.com/content/repositories/releases/"
// )

parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
// sbt-assembly
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)

// assembly
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)