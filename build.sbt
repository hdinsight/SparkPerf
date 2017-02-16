// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

name := "spark-sql-perf"

organization := "com.databricks"

scalaVersion := "2.11.8"

// sparkPackageName := "databricks/spark-sql-perf"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// sparkVersion := "2.0.2"
// sparkComponents ++= Seq("sql", "hive", "mllib")


initialCommands in console :=
  """
    |import org.apache.spark.sql._
    |import org.apache.spark.sql.functions._
    |import org.apache.spark.sql.types._
    |import org.apache.spark.sql.hive.test.TestHive
    |import TestHive.implicits
    |import TestHive.sql
    |
    |val sqlContext = TestHive
    |import sqlContext.implicits._
  """.stripMargin


libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

libraryDependencies += "com.twitter" %% "util-jvm" % "6.23.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.17"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.1.0"

libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.9.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"

fork := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
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
