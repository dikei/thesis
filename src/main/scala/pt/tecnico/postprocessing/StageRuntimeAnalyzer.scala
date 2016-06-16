package pt.tecnico.postprocessing

import java.awt.{Color, Font}
import java.io.{File, FileReader, FileWriter}
import java.util.Date
import java.util.regex.Pattern

import com.google.common.io.PatternFilenameFilter
import net.stamfest.rrd._
import org.jfree.chart.axis._
import org.jfree.chart.plot._
import org.jfree.chart.renderer.category.StandardBarPainter
import org.jfree.chart.renderer.xy.{StandardXYBarPainter, StandardXYItemRenderer, XYBarRenderer}
import org.jfree.chart.title.TextTitle
import org.jfree.chart.util.RelativeDateFormat
import org.jfree.chart.{ChartFactory, ChartUtilities, JFreeChart}
import org.jfree.data.function.NormalDistributionFunction2D
import org.jfree.data.gantt.{Task, TaskSeries, TaskSeriesCollection, XYTaskDataset}
import org.jfree.data.general.DatasetUtilities
import org.jfree.data.statistics.{SimpleHistogramBin, SimpleHistogramDataset}
import org.jfree.data.time.{Second, SimpleTimePeriod, TimeSeries, TimeSeriesCollection}
import org.jfree.data.xy.DefaultXYDataset
import org.jfree.ui.{RectangleAnchor, RectangleInsets}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.supercsv.cellprocessor._
import org.supercsv.cellprocessor.constraint.NotNull
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.{CsvBeanReader, CsvBeanWriter}
import org.supercsv.prefs.CsvPreference
import pt.tecnico.spark.util.{AppData, ReadMethod, StageData, StageRuntimeStatistic}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.collection.JavaConverters._

/**
  * Created by dikei on 3/15/16.
  */
object StageRuntimeAnalyzer {
  val processors = Array (
    new ParseInt(), // Stage Id
    new NotNull(), // Stage Name
    new ParseInt(), // Task count
    new ParseLong(), // Total task runtime
    new ParseLong(), // Stage runtime
    new ParseLong(), // Fetch wait time
    new ParseLong(), // Shuffle write time
    new ParseLong(), // Wait for partial output time
    new ParseLong(), // Average task time
    new ParseLong(), // Fastest task
    new ParseLong(), // Slowest task
    new ParseLong(), // Standard deviation
    new ParseLong(), // 5 percentile
    new ParseLong(), // 25 percentile
    new ParseLong(), // Median
    new ParseLong(), // 75 percentile
    new ParseLong(), // 95 percentile
    new ParseLong(), // Start time
    new ParseLong() // Completion time
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.StageRuntimeAnalyzer statsDir rrdDir outFile")
      System.exit(-1)
    }

    val statsDir = args(0)
    val rrdDir = args(1)
    val outFile = args(2)

    val data = Utils.parseJsonInput(statsDir, stageFilter = Some(stageFilter))

    println("Generating csv")
    generateCsv(data, rrdDir, outFile)

    println("Plotting stage gantt chart")
    plotStageGanttChart(data)
  }

  def stageFilter(stage: StageData): Boolean = stage.jobId > 0 // stage.stageId >= 7 && stage.stageId <= 9

  def plotStageGanttChart(data: Seq[(AppData, Seq[StageData], String)]): Unit = {
    val jobFont = new Font("Dialog", Font.PLAIN, 40)
    val jobBackgrounds = Array (
      new Color(247,247,247),
      new Color(99,99,99)
    )

    data.foreach { case (appData, stages, outputPrefix) =>
      val collection = new TaskSeriesCollection()
      val markers = mutable.Buffer[IntervalMarker]()
      val stageSeries = new TaskSeries("Stages")
      val executorSeries = mutable.HashMap[String, TaskSeries]()
      var index = 0
      var stageProcessed = 0
      stages.groupBy(_.jobId).toList.sortBy(_._1).foreach { case (jobId, jobStages) =>
        val sortedStages = jobStages.sortBy(_.startTime)
        var startTime = java.lang.Long.MAX_VALUE
        var endTime = -1L
        sortedStages.foreach { stage =>
          stageProcessed += 1
          if (startTime > stage.startTime) {
            startTime = stage.startTime
          }
          if (endTime < stage.completionTime) {
            endTime = stage.completionTime
          }
          val task = new Task(stage.stageId.toString, new SimpleTimePeriod(stage.startTime, stage.completionTime))
          stageSeries.add(task)

          stage.tasks.groupBy(_.host).foreach { case (host, tasks) =>
            val executorStart = tasks.map(_.startTime).min
            val executorEnd = tasks.map(_.endTime).max
            val executorRunPeriod = new SimpleTimePeriod(executorStart, executorEnd)
            val executorRun = new Task(stage.stageId.toString, executorRunPeriod)

            executorSeries.getOrElseUpdate(host, new TaskSeries(host)).add(executorRun)

            val waitPeriods = tasks.flatMap(_.waitForParentPeriods)
            if (waitPeriods.nonEmpty) {
              // Jfreechart doesn't draw properly if the subtask do not filled the parent task
              // Add this as a workaround
              executorRun.addSubtask(new Task(s"Execute-${stage.stageId}-$host", executorRunPeriod))
              waitPeriods.zipWithIndex.foreach { case (waitPeriod, i) =>
                val wait = new Task(
                  s"Wait-${stage.stageId}-$host-$i",
                  new SimpleTimePeriod(waitPeriod.start, waitPeriod.start + waitPeriod.duration)
                )
                executorRun.addSubtask(wait)
              }
            }
          }
          executorSeries.values.foreach { executor =>
            if (executor.getItemCount < stageProcessed) {
                // This executor doesn't run any task in this stage
                executor.add(new Task(stage.stageId.toString, new SimpleTimePeriod(stage.startTime, stage.startTime)))
            }
          }
        }
        val jobMarker = new IntervalMarker(startTime, endTime)
        jobMarker.setLabel(jobId.toString)
        jobMarker.setAlpha(0.2f)
        jobMarker.setLabelFont(jobFont)
        jobMarker.setLabelAnchor(RectangleAnchor.TOP)
        jobMarker.setLabelOffset(new RectangleInsets(20, 0, 0, 0))
        jobMarker.setPaint(jobBackgrounds(index % jobBackgrounds.length))
        index += 1
        markers += jobMarker
      }
      collection.add(stageSeries)
      executorSeries.values.foreach(collection.add)
      val dateFormat = new RelativeSecondFormat(appData.start)
      val timeAxis = new CustomDateAxis("Time")
      timeAxis.setLowerBound(0)
      timeAxis.setDateFormatOverride(dateFormat)
      timeAxis.setRange(new Date(appData.start), new Date(appData.end))
      timeAxis.setTickUnit(new DateTickUnit(DateTickUnitType.SECOND, 10))
      timeAxis.setMinorTickMarksVisible(true)
      timeAxis.setMinorTickCount(2)
      timeAxis.setLowerMargin(0.02)
      timeAxis.setUpperMargin(0.02)

      val categoryAxis = new CategoryAxis("Stages")

      val renderer = new StageGanttRenderer(collection)
      renderer.setBarPainter(new StandardBarPainter)
      renderer.setDrawBarOutline(false)
      renderer.setShadowVisible(false)
      val plot = new CategoryPlot(collection, categoryAxis, timeAxis, renderer)
      plot.setOrientation(PlotOrientation.HORIZONTAL)
      plot.getDomainAxis.setCategoryMargin(0.3)
      markers.foreach { marker =>
        plot.addRangeMarker(marker)
      }
      val chart = new JFreeChart("Stage gantt", JFreeChart.DEFAULT_TITLE_FONT, plot, true)
      val output = new File( s"$outputPrefix-stages.png" )
      val width = 1280 * 2
      val height = 960
      ChartUtilities.saveChartAsPNG(output, chart, width, height)
    }
  }

  def generateCsv(data: Array[(AppData, Seq[StageData], String)], rrdDir: String, outFile: String): Unit = {
    val average = data.flatMap(run => run._2).groupBy(_.stageId)
      .map { case (stageId, runs) =>
        val validRuns = runs.map { case (stageData: StageData) =>
          val startDir = new File(rrdDir)
          val cpuUsage = 100 - Utils.computeClusterAverage(
            Utils.cpuPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            Utils.computeIdleCpuNodeAverage
          )
          val systemLoad = Utils.computeClusterAverage(
            Utils.loadPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            Utils.computeLoadNodeAverage
          )
          val upload = Utils.computeClusterAverage(
            Utils.networkPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            Utils.computeUploadNodeAverage
          )
          val download = Utils.computeClusterAverage(
            Utils.networkPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            Utils.computeDownloadNodeAverage
          )

          new StageRuntimeStatistic(
            stageId,
            stageData.average,
            stageData.fastest,
            stageData.slowest,
            stageData.standardDeviation,
            stageData.name,
            stageData.taskCount,
            stageData.percent5,
            stageData.percent25,
            stageData.median,
            stageData.percent75,
            stageData.percent95,
            stageData.totalTaskRuntime,
            stageData.runtime,
            stageData.fetchWaitTime,
            stageData.shuffleWriteTime,
            cpuUsage,
            systemLoad,
            upload,
            download,
            stageData.partialOutputWaitTime,
            stageData.initialReadTime,
            stageData.memoryInput,
            stageData.hadoopInput,
            stageData.networkInput,
            stageData.diskInput)
        }.filter { s =>
          !s.getCpuUsage.isNaN && !s.getSystemLoad.isNaN && !s.getUpload.isNaN && !s.getDownload.isNaN
        }

        val total = validRuns.reduce[StageRuntimeStatistic] { case (s1: StageRuntimeStatistic, s2: StageRuntimeStatistic) =>
          new StageRuntimeStatistic(
            stageId,
            s1.getAverage + s2.getAverage,
            s1.getFastest + s2.getFastest,
            s1.getSlowest + s2.getSlowest,
            s1.getStandardDeviation + s2.getStandardDeviation,
            s1.getName,
            s1.getTaskCount + s2.getTaskCount,
            s1.getPercent5 + s2.getPercent5,
            s1.getPercent25 + s2.getPercent25,
            s1.getMedian + s2.getMedian,
            s1.getPercent75 + s2.getPercent75,
            s1.getPercent95 + s2.getPercent95,
            s1.getTotalTaskRuntime + s2.getTotalTaskRuntime,
            s1.getStageRuntime + s2.getStageRuntime,
            s1.getFetchWaitTime + s2.getFetchWaitTime,
            s1.getShuffleWriteTime + s2.getShuffleWriteTime,
            s1.getCpuUsage + s2.getCpuUsage,
            s1.getSystemLoad + s2.getSystemLoad,
            s1.getUpload + s2.getUpload,
            s1.getDownload + s2.getDownload,
            s1.getPartialOutputWaitTime + s2.getPartialOutputWaitTime,
            s1.getInitialReadTime + s2.getInitialReadTime,
            s1.getMemoryInput + s2.getMemoryInput,
            s1.getHadoopInput + s2.getHadoopInput,
            s1.getNetworkInput + s2.getNetworkInput,
            s1.getDiskInput + s2.getDiskInput
          )
        }
        val count = validRuns.length
        new StageRuntimeStatistic(
          stageId,
          total.getAverage / count,
          total.getFastest / count,
          total.getSlowest / count,
          total.getStandardDeviation / count,
          total.getName,
          total.getTaskCount / count,
          total.getPercent5 / count,
          total.getPercent25 / count,
          total.getMedian / count,
          total.getPercent75 / count,
          total.getPercent95 / count,
          total.getTotalTaskRuntime / count / 1000,
          total.getStageRuntime / count / 1000,
          total.getFetchWaitTime / count / 1000,
          total.getShuffleWriteTime / count / 1000000000,
          total.getCpuUsage / count,
          total.getSystemLoad / count,
          total.getUpload / count,
          total.getDownload / count,
          total.getPartialOutputWaitTime / count / 1000,
          total.getInitialReadTime / count / 1000,
          total.getMemoryInput / count / 1000000,
          total.getHadoopInput / count / 1000000,
          total.getNetworkInput / count / 1000000,
          total.getDiskInput / count / 1000000
        )
      }.toArray.sortBy(_.getStageId)

    val writer = new CsvBeanWriter(new FileWriter(outFile), CsvPreference.STANDARD_PREFERENCE)
    val headers = Array (
      "StageId", "Name", "TaskCount", "StageRuntime", "TotalTaskRuntime",
      "InitialReadTime", "PartialOutputWaitTime", "FetchWaitTime", "ShuffleWriteTime",
      "MemoryInput", "HadoopInput", "NetworkInput", "DiskInput"
    )

    val sizeFormatter = new FmtNumber("#,### MB")
    val writeProcessors = Array[CellProcessor] (
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      sizeFormatter,
      sizeFormatter,
      sizeFormatter,
      sizeFormatter
    )
    try {
      writer.writeHeader(headers:_*)
      average.foreach { stage =>
        writer.write(stage, headers, writeProcessors)
      }
    } finally {
      writer.close()
    }
  }

}
