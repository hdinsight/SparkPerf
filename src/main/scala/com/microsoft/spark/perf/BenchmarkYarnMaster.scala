package com.microsoft.spark.perf

import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.ShutdownHookManager
import org.spark_project.guava.io.Files

import org.apache.spark.SparkContext

object BenchmarkYarnMaster {

  private def createCommandStr(
      localPath: String,
      sparkParams: String,
      benchmarkParams: String): String = {
    // We have to disable eventLog due to the event directory permission issue
    s"spark-submit --conf spark.eventLog.enabled=false" +
      s" --class com.microsoft.spark.perf.RunBenchmark $sparkParams $localPath $benchmarkParams"
  }

  private def copyFileToLocal(remotePath: String): String = {
    val tempPath = Files.createTempDir().getAbsolutePath
    val remoteFS = new Path(remotePath).getFileSystem(new Configuration())
    remoteFS.copyToLocalFile(new Path(remotePath + "/spark-benchmark.jar"),
      new Path(tempPath))
    remoteFS.copyToLocalFile(new Path(remotePath + "/spark.conf"),
      new Path(tempPath))
    remoteFS.copyToLocalFile(new Path(remotePath + "/benchmark.conf"),
      new Path(tempPath))
    tempPath
  }

  private def loadParams(filePath: String): String = {
    val sb = new StringBuilder()
    for (line <- Source.fromFile(filePath).getLines()) {
      if (line.contains("--master")) {
        throw new Exception("You cannot define --master when running test in cluster mode")
      }
      if (line.contains("--class")) {
        throw new Exception("You cannot define --class when running test in cluster mode")
      }
      sb.append(line + " ")
    }
    sb.toString()
  }

  private def addShutdownHook(localPath: String): Unit = {
    ShutdownHookManager.get().addShutdownHook(new Thread() {
      override def run(): Unit = {
        FileSystem.getLocal(new Configuration()).delete(new Path(localPath), true)
      }
    }, 1)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val remoteWorkingDir = args(0)
    val localPath = copyFileToLocal(remoteWorkingDir)
    val sparkParams = loadParams(localPath + "/spark.conf")
    val benchmarkParams = loadParams(localPath + "/benchmark.conf")

    addShutdownHook(localPath)

    try {
      import scala.collection.JavaConverters._
      val pb = new ProcessBuilder()
      pb.command(
        createCommandStr(
          localPath + "/spark-benchmark.jar",
          sparkParams,
          benchmarkParams).split(" ").toList.asJava
      )
      val process = pb.start()
      println("test exit value: " + process.waitFor())
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}
