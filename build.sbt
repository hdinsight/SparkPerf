// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

name := "spark-benchmark"

organization := "com.microsoft"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "com.twitter" %% "util-jvm" % "6.23.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP5" % "test",
  "org.yaml" % "snakeyaml" % "1.17",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "org.scalatest" % "scalatest_2.11" % "3.0.1",
  "org.scala-lang" % "scala-library" % s"${scalaVersion.value}",
  "org.apache.spark" % "spark-sql_2.11" % s"$sparkVersion",
  "org.apache.spark" % "spark-mllib_2.11" % s"$sparkVersion",
  "org.apache.spark" % "spark-hive_2.11" % s"$sparkVersion",
  "org.apache.hadoop" % "hadoop-yarn-api" % "2.7.3",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
  "com.typesafe" % "config" % "1.3.1")

parallelExecution in Test := false

test in assembly := {}

fork := true

javaOptions in Test ++= Seq("-Xmx4096m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m")

concurrentRestrictions in Global := Seq(Tags.limitAll(1))

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

assemblyMergeStrategy  in assembly <<= (mergeStrategy in assembly) { (old) => {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs@_*) =>
    (xs map {
      _.toLowerCase
    }) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps@(x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}
}
