package pt.tecnico.spark.util

import java.io.{File, FileWriter, PrintWriter}

import org.apache.spark.executor._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.{Logging, SparkEnv}
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

import scala.collection.mutable


/**
  * Listener to calculate the stage runtime
  */
class StageRuntimeReportListener(statisticDir: String) extends SparkListener with Logging{

  private var freeCores = 0
  private var totalCores = 0

  private val timers = mutable.HashMap[(Int, Int), Timer]()
  private val executors = mutable.HashMap[String, ExecutorInfo]()
  private val appData = new AppData()
  private val stagesData = new mutable.HashMap[(Int, Int), StageData]()
  private val taskInfoMetrics = mutable.HashMap[(Int, Int), mutable.Buffer[(TaskInfo, TaskMetrics)]]()
  private val removeStageBarrier = SparkEnv.get.conf.getBoolean("spark.scheduler.removeStageBarrier", false)
  private val stageIdToJobId = new mutable.HashMap[Int, Int]()

  /**
    * Call when application start
    */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appData.start = applicationStart.time
    appData.name = applicationStart.appName
    appData.barrier = !removeStageBarrier
    appData.id = applicationStart.appId.getOrElse("Nil")
    appData.attempId = applicationStart.appAttemptId.getOrElse("Nil")
  }

  /**
    * Called when the application ends
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val jsonFile = if (removeStageBarrier) {
      s"${appData.id}-${appData.name}-no-barrier.json"
    } else {
      s"${appData.id}-${appData.name}.json"
    }
    val statDirs = new File(statisticDir)
    if (!statDirs.exists()) {
      statDirs.mkdirs()
    }

    val writer = new PrintWriter(new FileWriter(new File(statDirs, jsonFile)))
    appData.end = applicationEnd.time
    implicit val formats = Serialization.formats(NoTypeHints) + new org.json4s.ext.EnumSerializer(ReadMethod)
    try {
      // Write application data
      write(appData, writer)
      writer.println()
      // Write the number of stages so we can read it
      writer.println(stagesData.size)
      // Write stages data separately
      for(data <- stagesData.values) {
        write(data, writer)
        writer.println()
      }
    } finally {
      writer.close()
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageIds.foreach { stageId =>
      stageIdToJobId += stageId -> jobStart.jobId
    }
  }

  /**
    * Called when a stage is submitted
    */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val info = stageSubmitted.stageInfo
    val stage = new StageData()
    stage.stageId = info.stageId
    stage.stageAttemptId = info.attemptId

    stagesData += (info.stageId, info.attemptId) -> stage
    taskInfoMetrics += (info.stageId, info.attemptId) -> mutable.Buffer[(TaskInfo, TaskMetrics)]()

    val timer = new Timer
    timers += (info.stageId, info.attemptId) -> timer
    timer.start()
  }

  /**
    * Called when a stage completes successfully or fails, with information on the completed stage.
    */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    val stageData = stagesData((info.stageId, info.attemptId))

    if (info.failureReason.isDefined) {
      // Don't need to calculate anything on failure,
      // we only use this to skip failed run
      stageData.failed = true
      return
    }

    stageData.taskCount = info.numTasks
    stageData.name = info.name
    stageData.startTime = info.submissionTime.get
    stageData.completionTime = info.completionTime.get
    stageData.jobId = stageIdToJobId.getOrElse(info.stageId, -1)

    val taskData = mutable.ArrayBuffer[TaskData]()
    val stageTaskInfoMetrics = taskInfoMetrics.get((info.stageId, info.attemptId)).get

    // Store the tasks data inside stage data
    stageTaskInfoMetrics.foreach { case (taskInfo, taskMetric) =>
      val (_fetchWaitTime, _partialOutputWaitTime, _initialReadTime, _waitForParentPeriods):
        (Long, Long, Long, Array[WaitPeriod]) =
        taskMetric.shuffleReadMetrics match {
          case Some(metric) =>
            (metric.fetchWaitTime,
              metric.waitForPartialOutputTime,
              metric.initialReadTime,
              metric.waitForParentPeriods.map(p => new WaitPeriod(p._1, p._2)).toArray)
          case _ => (0, 0, 0, Array[WaitPeriod]())
        }
      val _shuffleWriteTime: Long = taskMetric.shuffleWriteMetrics match {
        case Some(metric) =>
          metric.shuffleWriteTime
        case _ => 0
      }
      val (_inputBytesRead, _inputSource): (Long, ReadMethod.Value) = taskMetric.inputMetrics match {
        case Some(metric) =>
          val method = metric.readMethod match {
            case DataReadMethod.Disk => ReadMethod.Disk
            case DataReadMethod.Hadoop => ReadMethod.Hadoop
            case DataReadMethod.Network => ReadMethod.Network
            case DataReadMethod.Memory => ReadMethod.Memory
          }
          (metric.bytesRead, method)
        case _ => (0, ReadMethod.None)
      }

      taskData += new TaskData (
        id = taskInfo.id,
        index = taskInfo.index,
        fetchWaitTime = _fetchWaitTime,
        shuffleWriteTime = _shuffleWriteTime,
        initialReadTime = _initialReadTime,
        waitForPartialOutputTime = _partialOutputWaitTime,
        inputBytesRead = _inputBytesRead,
        inputSource = _inputSource,
        executor = taskInfo.executorId,
        host = taskInfo.host,
        startTime = taskInfo.launchTime,
        endTime = taskInfo.finishTime,
        gcTime = taskMetric.jvmGCTime,
        waitForParentPeriods = _waitForParentPeriods
      )
    }
    stageData.tasks = taskData.toArray[TaskData]

    log.info("Stage completed: {}", stageData.name)
    log.info("Number of tasks: {}", stageData.taskCount)
    log.info("Stage runtime: {} ms", stageData.runtime)
    log.info("Stage submission time: {}", stageData.startTime)
    log.info("Stage completion time: {}", stageData.completionTime)

    // Clear out the buffer to save memory
    taskInfoMetrics.remove((info.stageId, info.attemptId))
    timers((info.stageId, info.attemptId)).reset()
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    freeCores -= 1
    if (freeCores < 1) {
      timers((taskStart.stageId, taskStart.stageAttemptId)).pause()
    }
  }

  /**
    * Save each task info and metrics
    */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    freeCores += 1
    timers((taskEnd.stageId, taskEnd.stageAttemptId)).start()
    for (buffer <- taskInfoMetrics.get((taskEnd.stageId, taskEnd.stageAttemptId))) {
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