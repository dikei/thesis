package pt.tecnico.spark.util

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.supercsv.io.{CsvBeanWriter, ICsvBeanWriter}
import org.supercsv.prefs.CsvPreference

import scala.collection.mutable

/**
  * Listener to calculate the stage runtime
  */
class StageRuntimeReportListener(statisticDir: String) extends SparkListener with Logging{

  private var freeCores = 0
  private var totalCores = 0

  private val timers = mutable.HashMap[Int, Timer]()
  private val taskInfoMetrics = mutable.HashMap[Int, mutable.Buffer[(TaskInfo, TaskMetrics)]]()
  private val executors = mutable.HashMap[String, ExecutorInfo]()
  private val now = Calendar.getInstance()
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
  private val timeStamp = dateFormat.format(now.getTime)
  private val appName = SparkEnv.get.conf.get("spark.app.name")
  private val fileName = s"$appName-$timeStamp.csv"

  private val headers = Array (
    "StageId", "Name", "TaskCount", "TotalTaskRuntime", "StageRuntime", "FetchWaitTime", "ShuffleWriteTime",
    "Average", "Fastest", "Slowest", "StandardDeviation",
    "Percent5", "Percent25", "Median", "Percent75", "Percent95",
    "StartTime", "CompletionTime"
  )

  private val csvWriter = new CsvBeanWriter(new FileWriter(new File(statisticDir, fileName)), CsvPreference.STANDARD_PREFERENCE)
  csvWriter.writeHeader(headers:_*)

  /**
    * Called when a stage is submitted
    */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val timer = new Timer
    taskInfoMetrics += stageSubmitted.stageInfo.stageId -> mutable.Buffer[(TaskInfo, TaskMetrics)]()
    timers += stageSubmitted.stageInfo.stageId -> timer
    timer.start()
  }

  /**
    * Called when a stage completes successfully or fails, with information on the completed stage.
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo

    if (info.failureReason.isDefined) {
      // Skip on failure
      return
    }

    val runtime = info.completionTime.get - info.submissionTime.get

    val stageTaskInfoMetrics = taskInfoMetrics.get(info.stageId).get.toArray
    val durations = stageTaskInfoMetrics.map { case (taskInfo, taskMetric) =>
      taskInfo.duration
    }

    var fetchWaitTime: Long = 0
    var shuffleWriteTime: Long = 0
    var waitForPartialOutputTime: Long = 0
    stageTaskInfoMetrics.foreach { case (taskInfo, taskMetric) =>
      taskMetric.shuffleReadMetrics match {
        case Some(metric) =>
          fetchWaitTime += metric.fetchWaitTime
          waitForPartialOutputTime += metric.waitForPartialOutputTime
        case _ =>
      }
      taskMetric.shuffleWriteMetrics match {
        case Some(metric) =>
          shuffleWriteTime += metric.shuffleWriteTime
        case _ =>
      }
    }

    var totalDuration = 0L
    var min = Long.MaxValue
    var max = 0L
    durations.foreach { duration =>
      totalDuration += duration
      if (duration < min) {
        min = duration
      }
      if (duration > max) {
        max = duration
      }
    }

    val mean = totalDuration / info.numTasks
    val variance = durations.map { duration =>
      val tmp = duration - mean
      tmp * tmp
    }.sum / info.numTasks

    val sortedDurations = durations.sorted
    val percent5 = sortedDurations((sortedDurations.length * 0.05).toInt)
    val percent25 = sortedDurations((sortedDurations.length * 0.25).toInt)
    val median = sortedDurations((sortedDurations.length * 0.5).toInt)
    val percent75 = sortedDurations((sortedDurations.length * 0.75).toInt)
    val percent95 = sortedDurations((sortedDurations.length * 0.95).toInt)

    log.info("Stage completed: {}", info)
    log.info("Number of tasks: {}", info.numTasks)
    log.info("Stage runtime: {} ms", runtime)
    log.info("Stage submission time: {}", info.submissionTime.get)
    log.info("Stage completion time: {}", info.completionTime.get)
    log.info("Total task time: {} ms", totalDuration)
    log.info("Fetch wait time: {} ms", fetchWaitTime)
    log.info("Shuffle write time: {} ms", shuffleWriteTime)
    log.info("Average task runtime: {} ms", mean)
    log.info("Fastest task: {} ms", min)
    log.info("Slowest task: {} ms", max)
    log.info("Standard deviation: {} ms", Math.sqrt(variance))
    log.info("5th percentile: {} ms", percent5)
    log.info("25th percentile: {} ms", percent25)
    log.info("Median: {} ms", median)
    log.info("75th percentile: {} ms", percent75)
    log.info("95th percentile: {} ms", percent95)
    log.info("Time block for partial map output: {} ms", waitForPartialOutputTime)

    val taskRuntimeStats = new StageRuntimeStatistic
    taskRuntimeStats.setStartTime(info.submissionTime.get)
    taskRuntimeStats.setCompletionTime(info.completionTime.get)
    taskRuntimeStats.setName(info.name)
    taskRuntimeStats.setStageId(info.stageId)
    taskRuntimeStats.setTaskCount(info.numTasks)
    taskRuntimeStats.setAverage(mean)
    taskRuntimeStats.setFastest(min)
    taskRuntimeStats.setSlowest(max)
    taskRuntimeStats.setStandardDeviation(math.sqrt(variance).toLong)
    taskRuntimeStats.setPercent25(percent25)
    taskRuntimeStats.setPercent75(percent75)
    taskRuntimeStats.setMedian(median)
    taskRuntimeStats.setPercent5(percent5)
    taskRuntimeStats.setPercent95(percent95)
    taskRuntimeStats.setTotalTaskRuntime(totalDuration)
    taskRuntimeStats.setStageRuntime(runtime)
    taskRuntimeStats.setFetchWaitTime(fetchWaitTime)
    taskRuntimeStats.setShuffleWriteTime(shuffleWriteTime)
    taskRuntimeStats.setPartialOutputWaitTime(waitForPartialOutputTime)
    csvWriter.write(taskRuntimeStats, headers:_*)
    csvWriter.flush()

    val cpuIdle = timers(info.stageId).elapsed
    log.info("Executor idle time: {}", cpuIdle)

    // Clear out the buffer to save memory
    taskInfoMetrics.remove(info.stageId)
    timers(info.stageId).reset()
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    freeCores -= 1
    if (freeCores < 1) {
      timers(taskStart.stageId).pause()
    }
  }

  /**
    * Save each task info and metrics
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    freeCores += 1
    timers(taskEnd.stageId).start()
    for (buffer <- taskInfoMetrics.get(taskEnd.stageId)) {
      if (taskEnd.taskInfo != null && taskEnd.taskMetrics != null) {
        buffer += ((taskEnd.taskInfo, taskEnd.taskMetrics))
      }
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    totalCores += executorAdded.executorInfo.totalCores
    freeCores += executorAdded.executorInfo.totalCores
    executors += executorAdded.executorId -> executorAdded.executorInfo
  }


  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    val removedExecutor = executors(executorRemoved.executorId)
    totalCores -= removedExecutor.totalCores
    executors -= executorRemoved.executorId
  }

  /**
    * Called when the application ends
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    csvWriter.close()
  }
}

class Timer {
  var startTime = 0L
  var started = false
  var elapsed = 0L

  def start(): Unit = {
    started = true
    startTime = System.currentTimeMillis()
  }

  def pause(): Unit = {
    started = false
    elapsed += System.currentTimeMillis() - startTime
  }

  def reset(): Unit = {
    elapsed = 0L
  }
}