resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

name := "FlinkForward2017"

version := "0.1-SNAPSHOT"

organization := "com.bettercloud.flinkforward"

scalaVersion in ThisBuild := "2.11.7"

val flinkVersion = "1.2.0"
val jacksonGroupId        = "com.fasterxml.jackson.module"
val jacksonVersion        = "2.7.3"

val flinkDependencies = Seq(
  "com.jayway.jsonpath" % "json-path" % "2.2.0" ,
  jacksonGroupId     %% "jackson-module-scala"      % jacksonVersion,
  jacksonGroupId     %  "jackson-modules-base"      % jacksonVersion,
  "org.apache.flink" %% "flink-connector-kafka-0.9" % flinkVersion,
  "org.apache.flink" %% "flink-scala"               % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala"     % flinkVersion % "provided"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

mainClass in assembly := Some("com.bettercloud.flinkforward.Job")

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
