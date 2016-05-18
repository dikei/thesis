package pt.tecnico.spark.util

import scala.collection.mutable

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
    var name: String = "",
    var taskCount: Int = 0,
    var startTime: Long = 0,
    var completionTime: Long = 0,
    var failed: Boolean = false,
    var jobId: Int = -1,
    var tasks: Array[TaskData] = Array[TaskData]()
  ) {
  lazy val runtime = completionTime - startTime

  lazy val (
    fetchWaitTime,
    partialOutputWaitTime,
    initialReadTime,
    shuffleWriteTime) = processTasksMetric

  private def processTasksMetric: (Long, Long, Long, Long) = {
    var _fetchWaitTime: Long = 0
    var _partialOutputWaitTime: Long = 0
    var _initialReadTime: Long = 0
    var _shuffleWriteTime: Long = 0

    tasks.foreach { metric =>
      _fetchWaitTime += metric.fetchWaitTime
      _partialOutputWaitTime += metric.waitForPartialOutputTime
      _initialReadTime += metric.initialReadTime
      _shuffleWriteTime += metric.shuffleWriteTime
    }
    (_fetchWaitTime, _partialOutputWaitTime, _initialReadTime, _shuffleWriteTime)
  }

  lazy val (
    totalTaskRuntime,
    fastest,
    slowest,
    average,
    standardDeviation,
    percent5,
    percent25,
    median,
    percent75,
    percent95) = processTaskStats

  private def processTaskStats: (Long, Long, Long, Long, Long, Long, Long, Long, Long, Long) = {
    val durations = tasks.map { taskData =>
      taskData.duration
    }
    var _totalTaskRuntime: Long = 0
    var _fastest: Long = Long.MaxValue
    var _slowest: Long = -1
    durations.foreach { duration =>
      _totalTaskRuntime += duration
      if (duration < _fastest) {
        _fastest = duration
      }
      if (duration > _slowest) {
        _slowest = duration
      }
    }
    val _average = _totalTaskRuntime / taskCount

    val variance = durations.map { duration =>
      val tmp = duration - _average
      tmp * tmp
    }.sum / taskCount
    val _standardDeviation = Math.sqrt(variance).round

    val sortedDurations = durations.sorted
    val _percent5 = sortedDurations((sortedDurations.length * 0.05).toInt)
    val _percent25 = sortedDurations((sortedDurations.length * 0.25).toInt)
    val _median = sortedDurations((sortedDurations.length * 0.5).toInt)
    val _percent75 = sortedDurations((sortedDurations.length * 0.75).toInt)
    val _percent95 = sortedDurations((sortedDurations.length * 0.95).toInt)
    (_totalTaskRuntime, _fastest, _slowest, _average, _standardDeviation,
      _percent5, _percent25, _median, _percent75, _percent95)
  }

  lazy val (memoryInput, hadoopInput, diskInput, networkInput) = processTasksInput

  private def processTasksInput: (Long, Long, Long, Long) = {
    val acc = mutable.HashMap[ReadMethod.Value, Long]()
    tasks.foreach { taskData =>
      acc += taskData.inputSource -> (acc.getOrElse(taskData.inputSource, 0L) + taskData.inputBytesRead)
    }
    (acc.getOrElse(ReadMethod.Memory, 0L),
      acc.getOrElse(ReadMethod.Hadoop, 0L), acc.getOrElse(ReadMethod.Disk, 0L),
      acc.getOrElse(ReadMethod.Network, 0L))
  }
}

object ReadMethod extends Enumeration with Serializable {
  type ReadMethod = Value
  val None, Memory, Disk, Hadoop, Network = Value
}

case class WaitPeriod (start: Long, duration: Long)

case class TaskData (
    id: String,
    index: Int,
    fetchWaitTime: Long,
    initialReadTime: Long,
    waitForPartialOutputTime: Long,
    shuffleWriteTime: Long,
    inputBytesRead: Long,
    inputSource: ReadMethod.Value,
    executor: String,
    host: String,
    startTime: Long,
    endTime: Long,
    waitForParentPeriods: Array[WaitPeriod]) {

  lazy val duration: Long = endTime - startTime
}