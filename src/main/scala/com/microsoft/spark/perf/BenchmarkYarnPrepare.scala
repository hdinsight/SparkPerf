package com.microsoft.spark.perf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object BenchmarkYarnPrepare {

  private def uploadFile(localPath: String, remoteWorkingDir: String): Unit = {
    new Path(remoteWorkingDir).getFileSystem(new Configuration()).
      copyFromLocalFile(new Path(localPath), new Path(remoteWorkingDir))
  }

  private def createRemoteWorkingDir(
      remoteWorkingDir: String,
      localJarPath: String,
      sparkSubmitParamsPath: String,
      benchmarkParamsPath: String): Unit = {
    uploadFile(localJarPath, remoteWorkingDir + "/spark-benchmark.jar")
    uploadFile(sparkSubmitParamsPath, remoteWorkingDir + "/spark.conf")
    uploadFile(benchmarkParamsPath, remoteWorkingDir + "/benchmark.conf")
  }

  def main(args: Array[String]): Unit = {
    val remoteWorkingDir = args(0)
    val localJarPath = args(1)
    val sparkSubmitParamsFilePath = args(2)
    val benchmarkParamsFilePath = args(3)

    createRemoteWorkingDir(remoteWorkingDir, localJarPath, sparkSubmitParamsFilePath,
      benchmarkParamsFilePath)
  }
}
