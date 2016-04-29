package pt.tecnico.spark.util

/**
  * Created by dikei on 4/29/16.
  */
case class AppData(
    var name: String = "",
    var id: String = "",
    var attempId: String = "",
    var start: Long = 0,
    var end: Long = 0,
    var barrier: Boolean = true) {
  def runtime = end - start
}

case class StageData(
    var stageId: Int = -1,
    var stageAttemptId: Int = -1,
    var average: Long = 0,
    var fastest: Long = Long.MaxValue,
    var slowest: Long = -1,
    var standardDeviation: Long = 0,
    var name: String = "",
    var taskCount: Int = 0,
    var percent5: Long = 0,
    var percent25: Long = 0,
    var median: Long = 0,
    var percent75: Long = 0,
    var percent95: Long = 0,
    var totalTaskRuntime: Long = 0,
    var fetchWaitTime: Long = 0,
    var shuffleWriteTime: Long = 0,
    var startTime: Long = 0,
    var completionTime: Long = 0,
    var partialOutputWaitTime: Long = 0,
    var failed: Boolean = false
  ) {
  def runtime = completionTime - startTime
}