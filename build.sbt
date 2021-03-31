name := "db2datalake"
organization := "com.github.vollgaz"
scalaVersion := "2.12.12"

libraryDependencies ++= {
  val Vspark = "3.1.1"
  val VscalaTest = "3.2.6"

  Seq(
    "org.apache.spark" %% "spark-sql" % Vspark % Provided,
    "org.apache.spark" %% "spark-core" % Vspark % Provided,
    "com.github.scopt" %% "scopt" % "4.0.0",
    "org.scalatest" %% "scalatest" % VscalaTest % Test,
    "org.scalatest" %% "scalatest-featurespec" % VscalaTest % Test,
    "org.xerial" % "sqlite-jdbc" % "3.34.0" % Test
  )
}

Test / parallelExecution := false
Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

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