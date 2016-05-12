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
import org.jfree.data.gantt.{Task, TaskSeries, TaskSeriesCollection}
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
import pt.tecnico.spark.util.{AppData, StageData, StageRuntimeStatistic}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

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

  val labelFont = new Font("Dialog", Font.PLAIN, 25)
  val colors = Array (
    new Color(230,97,1),
    new Color(253,184,99),
    new Color(178,171,210),
    new Color(94,60,153)
  )

  val rrdParser = new RRDp(".", null)
  val cpuPattern = Pattern.compile("cpu\\/percent-idle\\.rrd")
  val networkPattern = Pattern.compile("interface-eth0\\/if_octets\\.rrd")
  val diskPattern = Pattern.compile("disk-vdb\\/disk_io_time\\.rrd|disk-vda\\/disk_io_time\\.rrd")
  val loadPattern = Pattern.compile("load\\/load\\.rrd")
  // Blacklist the master node from cluster averages
  val blacklisted = Pattern.compile("testinstance-07\\.novalocal")

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.StageRuntimeAnalyzer statsDir rrdDir outFile")
      System.exit(-1)
    }

    val statsDir = args(0)
    val rrdFile = args(1)
    val outFile = args(2)
    val runtimeGraph = args(3)

//    val (averageRuntime: Long, average: Array[StageRuntimeStatistic]) = processCsvInput(statsDir, rrdFile)
    val average = processJsonInput(statsDir, rrdFile, runtimeGraph)
    val writer = new CsvBeanWriter(new FileWriter(outFile), CsvPreference.STANDARD_PREFERENCE)
    val headers = Array (
      "StageId", "Name", "TaskCount", "StageRuntime", "TotalTaskRuntime",
      "InitialReadTime", "PartialOutputWaitTime", "FetchWaitTime", "ShuffleWriteTime"
    )

    val numberFormater = new FmtNumber(".##")
    val networkFormatter = new FmtNumber("#,###")
    val writeProcessors = Array[CellProcessor] (
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull()
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

  def processJsonInput(statsDir: String, rrdFile: String, runtimeGraph: String): Array[StageRuntimeStatistic] = {
    implicit val formats = DefaultFormats

    val files = new File(statsDir).listFiles(new PatternFilenameFilter("(.*)\\.json$"))
    val filesData = files.flatMap { f =>
      val source = Source.fromFile(f)
      try {
        val fileIter = source.getLines()
        val appData = parse(fileIter.next()).extract[AppData]
        val stageCount = fileIter.next().toInt
        val stages = mutable.Buffer[StageData]()
        var i = 0
        var failureDetected = false
        while (i < stageCount) {
          val stage = parse(fileIter.next()).extract[StageData]
          if (stage.failed) {
            failureDetected = false
            i = stageCount
          } else {
            stages += stage
          }
          i += 1
        }
        // Skip over file that contained failed run
        if (!failureDetected) {
          Seq((appData, stages, f.getAbsolutePath))
        } else {
          Seq()
        }
      } catch {
        case e: Exception =>
          println(s"Invalid report: ${f.getAbsolutePath}")
          Seq()
      }
      finally {
        source.close()
      }
    }

    val average = filesData.flatMap(run => run._2).groupBy(_.stageId)
      .map { case (stageId, runs) =>
        val validRuns = runs.map { case (stageData: StageData) =>
          val startDir = new File(rrdFile)
          val cpuUsage = 100 - computeClusterAverage(
            rrdParser,
            cpuPattern,
            blacklisted,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            computeIdleCpuNodeAverage
          )
          val systemLoad = computeClusterAverage(
            rrdParser,
            loadPattern,
            blacklisted,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            computeLoadNodeAverage
          )
          val upload = computeClusterAverage(
            rrdParser,
            networkPattern,
            blacklisted,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            computeUploadNodeAverage
          )
          val download = computeClusterAverage(
            rrdParser,
            networkPattern,
            blacklisted,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            computeDownloadNodeAverage
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
            stageData.initialReadTime
          )
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
            s1.getInitialReadTime + s2.getInitialReadTime
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
          total.getInitialReadTime / count / 1000
        )
      }.toArray.sortBy(_.getStageId)

    // Plot CPU graph
    println("Plotting CPU graph")
    plotGraphJson(rrdFile, filesData, rrdParser, cpuPattern, blacklisted, plotCpuGraphPerRun, clusterAverage = true)

    // Plot network graph
    println("Plotting network graph")
    plotGraphJson(rrdFile, filesData, rrdParser, networkPattern, blacklisted, plotNetworkGraphPerRun, clusterAverage = true)

    // Plot diskgraph
    println("Plotting disk IO graph")
    plotGraphJson(rrdFile, filesData, rrdParser, diskPattern, blacklisted, plotDiskGraphPerRun, clusterAverage = true)

    println("Plotting stage gantt chart")
    plotStageGanttChart(filesData)

    println("Plotting runtime distribution")
    plotRuntimeDistribution(filesData, runtimeGraph)

    average
  }

  def plotGraphJson(
      rrdFile: String,
      runs: Seq[(AppData, mutable.Buffer[StageData], String)],
      rrdParser: RRDp,
      matchPattern: Pattern,
      blacklisted: Pattern,
      plotFunc: (RRDp, Array[File], String, Long, Long, Seq[(Int, Long, Long)], Boolean) => Unit,
      clusterAverage: Boolean = false): Unit = {
    runs.foreach { case (appData, stages, outputPrefix) =>
      val start = appData.start
      val end = appData.end
      val stagesDuration = stages.map(s => (s.stageId, s.startTime, s.completionTime))
      val rrdFiles = findRrd(new File(rrdFile), matchPattern).filter { f =>
        !blacklisted.matcher(f.getAbsolutePath).find()
      }
      plotFunc(rrdParser, rrdFiles, outputPrefix, start, end, stagesDuration, clusterAverage)
    }
  }

  def findRrd(startDir: File, p: Pattern): Array[File] = {
    val these = startDir.listFiles
    val good = these.filter { f =>
      p.matcher(f.getPath).find()
    }
    good ++ these.filter(_.isDirectory).flatMap {
      findRrd(_, p)
    }
  }

  def computeClusterAverage(
      rrdParser: RRDp,
      matchPattern: Pattern,
      blacklisted: Pattern,
      startDir: File,
      startTime: Long,
      endTime: Long,
      procFunc: (RRDp, String, Long, Long) => Double
      ): Double = {
    val files = findRrd(startDir, matchPattern)
    val validResult = files.filter { f =>
      !blacklisted.matcher(f.getAbsolutePath).find()
    }.map { f =>
      procFunc(rrdParser, f.getPath, startTime, endTime)
    }.filter(!_.isNaN)
    val total = validResult.reduce[Double] { case (d1: Double, d2: Double) =>
        d1 + d2
    }
    if (validResult.length > 0)
      total / validResult.length
    else
      Double.NaN
  }

  def computeLoadNodeAverage(rrdParser: RRDp, file: String, startTime: Long, endTime: Long): Double = {
    val command = Array (
      "fetch", file, "AVERAGE",
      "-s", (startTime / 1000).toString,
      "-e", (endTime / 1000).toString
    )
    val rddResult = rrdParser.command(command)
    // filtering unneeded + empty lines
    val lines = rddResult.getOutput.trim().split("\n")
      .filter { line =>
        line.contains(":") && !line.contains("nan")
      }
    if (lines.length > 0) {
      val totalLines = lines.map { line =>
        val tokens = line.split(":|\\s")
        tokens(2).trim().toDouble
      }.reduce[Double] { case (f1: Double, f2: Double) =>
        f1 + f2
      }
      totalLines / lines.length
    } else {
      Double.NaN
    }
  }

  def computeIdleCpuNodeAverage(rrdParser: RRDp, file: String, startTime: Long, endTime: Long): Double = {
    val command = Array[String] (
      "fetch", file, "AVERAGE",
      "-s", (startTime / 1000).toString,
      "-e", (endTime / 1000).toString
    )
    val rddResult = rrdParser.command(command)

    // filtering unneeded + empty lines
    val lines = rddResult.getOutput.trim().split("\n")
      .filter { line =>
        line.contains(":") && !line.contains("nan")
      }
    if (lines.length > 0) {
      val totalLines = lines.map { line =>
        val tokens = line.split(":")
        tokens(1).trim().toDouble
      }.reduce[Double] { case (f1: Double, f2: Double) =>
        f1 + f2
      }
      totalLines / lines.length
    } else {
      Double.NaN
    }
  }

  def computeUploadNodeAverage(rrdParser: RRDp, file: String, startTime: Long, endTime: Long): Double = {
    val command = Array[String] (
      "fetch", file, "AVERAGE",
      "-s", (startTime / 1000).toString,
      "-e", (endTime / 1000).toString
    )
    val rddResult = rrdParser.command(command)
    // filtering unneeded + empty lines
    val lines = rddResult.getOutput.trim().split("\n")
      .filter { line =>
        line.contains(":") && !line.contains("nan")
      }
    if (lines.length > 0) {
      val totalLines = lines.map { line =>
        val tokens = line.split(":|\\s")
        tokens(3).trim().toDouble
      }.reduce[Double] { case (f1: Double, f2: Double) =>
        f1 + f2
      }
      totalLines / lines.length
    } else {
      Double.NaN
    }
  }

  def computeDownloadNodeAverage(rrdParser: RRDp, file: String, startTime: Long, endTime: Long): Double = {
    val command = Array[String] (
      "fetch", file, "AVERAGE",
      "-s", (startTime / 1000).toString,
      "-e", (endTime / 1000).toString
    )
    val rddResult = rrdParser.command(command)

    // filtering unneeded + empty lines
    val lines = rddResult.getOutput.trim().split("\n")
      .filter { line =>
        line.contains(":") && !line.contains("nan")
      }
    if (lines.length > 0) {
      val totalLines = lines.map { line =>
        val tokens = line.split(":|\\s")
        tokens(2).trim().toDouble
      }.reduce[Double] { case (f1: Double, f2: Double) =>
        f1 + f2
      }
      totalLines / lines.length
    } else {
      Double.NaN
    }
  }

  def plotStageGanttChart(runs: Seq[(AppData, mutable.Buffer[StageData], String)]): Unit = {
    val jobFont = new Font("Dialog", Font.PLAIN, 40)
    val jobBackgrounds = Array (
      new Color(247,247,247),
      new Color(99,99,99)
    )

    runs.foreach { case (appData, stages, outputPrefix) =>
      val collection = new TaskSeriesCollection()
      val markers = mutable.Buffer[IntervalMarker]()
      val taskSeries = new TaskSeries("All stages")
      var index = 0
      stages.groupBy(_.jobId).toList.sortBy(_._1).foreach { case (jobId, jobStages) =>
        val sortedStages = jobStages.sortBy(_.startTime)
        var startTime = java.lang.Long.MAX_VALUE
        var endTime = -1L
        sortedStages.foreach { stage =>
          if (startTime > stage.startTime) {
            startTime = stage.startTime
          }
          if (endTime < stage.completionTime) {
            endTime = stage.completionTime
          }
          val task = new Task(stage.stageId.toString, new SimpleTimePeriod(stage.startTime, stage.completionTime))
          val initialReadFinished = stage.startTime + stage.initialReadTime / 24
          val waitFinished = initialReadFinished + stage.partialOutputWaitTime / 24
          val shuffleWriteStart = stage.completionTime - stage.shuffleWriteTime / 1000000 / 24
          task.addSubtask(new Task("Initial read", new SimpleTimePeriod(stage.startTime, initialReadFinished)))
          task.addSubtask(new Task("Wait for data", new SimpleTimePeriod(initialReadFinished, waitFinished)))
          task.addSubtask(new Task("Execution", new SimpleTimePeriod(waitFinished, shuffleWriteStart)))
          task.addSubtask(new Task("Shuffle write", new SimpleTimePeriod(shuffleWriteStart, stage.completionTime)))
          taskSeries.add(task)
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
      collection.add(taskSeries)
      val dateFormat = new RelativeDateFormat(appData.start)
      val timeAxis = new DateAxis("Time")
      timeAxis.setAutoRange(true)
      timeAxis.setDateFormatOverride(dateFormat)
      timeAxis.setMinimumDate(new Date(appData.start))
      timeAxis.setMaximumDate(new Date(appData.end))
      timeAxis.setLowerMargin(0.02)
      timeAxis.setUpperMargin(0.02)

      val categoryAxis = new CategoryAxis("Stages")

      val renderer = new StageGanttRenderer
      renderer.setBarPainter(new StandardBarPainter)
      renderer.setDrawBarOutline(true)
      renderer.setShadowVisible(false)
      val plot = new CategoryPlot(collection, categoryAxis, timeAxis, renderer)
      plot.setOrientation(PlotOrientation.HORIZONTAL)
      markers.foreach { marker =>
        plot.addRangeMarker(marker)
      }
      val chart = new JFreeChart("Stage gantt", JFreeChart.DEFAULT_TITLE_FONT, plot, false)
      val output = new File( s"$outputPrefix-stages.png" )
      val width = 1280
      val height = 960
      ChartUtilities.saveChartAsPNG(output, chart, width, height)
    }
  }

  def plotRuntimeDistribution(runs: Seq[(AppData, mutable.Buffer[StageData], String)], output: String): Unit = {
    var variance: Double = 0
    var totalRuntimes: Long = 0
    var bestRuntime = Long.MaxValue
    var worseRuntime = Long.MinValue
    val appRuntimes = runs.map { case (appData, _, _) =>
      totalRuntimes += appData.runtime
      if (bestRuntime > appData.runtime) {
        bestRuntime = appData.runtime
      }
      if (worseRuntime < appData.runtime) {
        worseRuntime = appData.runtime
      }
      appData.runtime
    }
    val averageRuntime = totalRuntimes / appRuntimes.length

    println(s"Average runtime: $averageRuntime")
    println(s"Best runtime: $bestRuntime")
    println(s"Worse runtime: $worseRuntime")

    val dataset = new SimpleHistogramDataset("Runtime")
    var binCount = 0
    val binsize = 10
    val averageInSecond = averageRuntime / 1000
    while(averageInSecond - binsize * binCount > bestRuntime / 1000 ||
      averageInSecond + binsize * binCount < worseRuntime / 1000 ) {
      dataset.addBin(
        new SimpleHistogramBin(
          averageInSecond - binsize * (binCount + 1),
          averageInSecond - binsize * binCount,
          true,
          false)
      )
      dataset.addBin(
        new SimpleHistogramBin(
          averageInSecond + binsize * binCount,
          averageInSecond + binsize * (binCount + 1),
          true,
          false)
      )
      binCount += 1
    }

    appRuntimes.foreach { runtime =>
      variance += Math.pow(runtime - averageRuntime, 2)
      dataset.addObservation(runtime / 1000)
    }
    val standardDeviation = Math.round(Math.sqrt(variance))

    dataset.setAdjustForBinSize(false)
    val chart = ChartFactory.createHistogram(
      "Execution time graph",
      "Execution Time",
      "Count",
      dataset,
      PlotOrientation.VERTICAL,
      false,
      false,
      false
    )

    val plot = chart.getXYPlot
    plot.setBackgroundPaint(Color.white)
    plot.setDomainGridlinePaint(Color.lightGray)
    plot.setRangeGridlinePaint(Color.lightGray)
    val yAxis = plot.getRangeAxis
    yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits())
    val xAxis = plot.getDomainAxis
    xAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits())

    val renderer = plot.getRenderer.asInstanceOf[XYBarRenderer]
    renderer.setDrawBarOutline(true)
    renderer.setBarPainter(new StandardXYBarPainter())
    renderer.setShadowVisible(false)

    chart.addSubtitle(new TextTitle(s"Average time: $averageInSecond s"))
    chart.addSubtitle(new TextTitle(s"Standard deviation: ${standardDeviation / 1000} s"))
    chart.addSubtitle(new TextTitle(s"Best time: ${bestRuntime / 1000} s"))
    chart.addSubtitle(new TextTitle(s"Worse time: ${worseRuntime / 1000} s"))
    val width = 1280
    val height = 960

    ChartUtilities.saveChartAsPNG(new File(output), chart, width, height)
  }

  def plotCpuGraphPerRun(
      rrdParser: RRDp,
      files: Array[File],
      outputPrefix: String,
      startTime: Long,
      endTime: Long,
      stagesDuration: Seq[(Int, Long, Long)],
      average: Boolean): Unit = {

    val points = files.flatMap { file =>
      val command = Array[String] (
        "fetch", file.getPath, "AVERAGE",
        "-s", (startTime / 1000).toString,
        "-e", (endTime / 1000).toString
      )
      val rddResult = rrdParser.command(command)
      rddResult.getOutput.trim().split("\n")
        .filter { line =>
          line.contains(":") && !line.contains("nan")
        }
        .map { line =>
          val tokens = line.split(":")
          val idle = tokens(1).trim().toDouble
          val time = tokens(0).toLong
          (time * 1000, 100 - idle, file.getParentFile.getParentFile.getName)
        }
    }
    .groupBy(t => t._1)

    val datasets = new ArrayBuffer[TimeSeriesCollection]()
    val seriesMap = new scala.collection.mutable.HashMap[String, TimeSeries]
    points.foreach { case (time: Long, machines: Array[(Long, Double, String)]) =>
      var cpuTotal = 0.0
      machines.foreach { case (_, cpu, machine) =>
        cpuTotal += cpu
        seriesMap.getOrElseUpdate(machine, new TimeSeries(machine)).add(new Second(new Date(time)), cpu)
      }
      if (average) {
        seriesMap.getOrElseUpdate("Average CPU", new TimeSeries("Average CPU"))
          .add(new Second(new Date(time)), cpuTotal / machines.length)
      }
    }
    seriesMap.foreach { case (_, series) =>
      datasets += new TimeSeriesCollection(series)
    }

    val output = new File( s"$outputPrefix-cpu.png" )
    drawChart(output, stagesDuration, datasets.toArray, "CPU", "Time", "Usage (%)", startTime, endTime)
  }

  def plotNetworkGraphPerRun(
      rrdParser: RRDp,
      files: Array[File],
      outputPrefix: String,
      startTime: Long,
      endTime: Long,
      stagesDuration: Seq[(Int, Long, Long)],
      average: Boolean): Unit = {
    val points = files.flatMap { file =>
      val command = Array[String] (
        "fetch", file.getPath, "AVERAGE",
        "-s", (startTime / 1000).toString,
        "-e", (endTime / 1000).toString
      )
      val rddResult = rrdParser.command(command)
      rddResult.getOutput.trim().split("\n")
        .filter { line =>
          line.contains(":") && !line.contains("nan")
        }
        .map { line =>
          val tokens = line.split(":|\\s")
          val download = tokens(2).trim().toDouble
          val upload = tokens(3).trim().toDouble
          val time = tokens(0).toLong
          (time * 1000, upload, download, file.getParentFile.getParentFile.getName)
        }
    }
    .groupBy(t => t._1)

    val datasets = new ArrayBuffer[TimeSeriesCollection]()
    val seriesMap = new scala.collection.mutable.HashMap[String, (TimeSeries, TimeSeries)]
    points.foreach { case (time: Long, machines: Array[(Long, Double, Double, String)]) =>
      var downloadTotal = 0.0
      var uploadTotal = 0.0
      machines.foreach { case (_, upload, download, machine) =>
        uploadTotal += upload
        downloadTotal += download
        val uploadDownload =
          seriesMap.getOrElseUpdate(
            s"$machine",
            (new TimeSeries(s"$machine-Upload"), new TimeSeries(s"$machine-Download"))
          )
        uploadDownload._1.add(new Second(new Date(time)), upload)
        uploadDownload._2.add(new Second(new Date(time)), download)
      }
      if (average) {
        val uploadDownload =
          seriesMap.getOrElseUpdate(
            "Average download",
            (new TimeSeries("Average upload"), new TimeSeries("Average download"))
          )
        uploadDownload._1.add(new Second(new Date(time)), uploadTotal / machines.length)
        uploadDownload._2.add(new Second(new Date(time)), downloadTotal / machines.length)
      }
    }
    seriesMap.foreach { case (_, (uploadSeries, downloadSeries)) =>
      val dataset = new TimeSeriesCollection
      dataset.addSeries(uploadSeries)
      dataset.addSeries(downloadSeries)
      datasets += dataset
    }

    val output = new File( s"$outputPrefix-network.png" )
    drawChart(output, stagesDuration, datasets.toArray, "Network", "Time", "Bandwidth (bytes/s)", startTime, endTime)
  }

  def plotDiskGraphPerRun(
      rrdParser: RRDp,
      files: Array[File],
      outputPrefix: String,
      startTime: Long,
      endTime: Long,
      stagesDuration: Seq[(Int, Long, Long)],
      average: Boolean) {
    val points = files.flatMap { file =>
      val command = Array[String] (
        "fetch", file.getPath, "AVERAGE",
        "-s", (startTime / 1000).toString,
        "-e", (endTime / 1000).toString
      )
      val rddResult = rrdParser.command(command)
      rddResult.getOutput.trim().split("\n")
        .filter { line =>
          line.contains(":") && !line.contains("nan")
        }
        .map { line =>
          val tokens = line.split(":|\\s")
          val ioTime = tokens(2).trim().toDouble
          val weightedIoTime = tokens(3).trim().toDouble
          val time = tokens(0).toLong
          (time * 1000, ioTime, weightedIoTime, file.getParentFile.getParentFile.getName, file.getParentFile.getName)
        }
    }
    .groupBy(t => t._1)

    val datasets = new ArrayBuffer[TimeSeriesCollection]()
    val seriesMap = new scala.collection.mutable.HashMap[(String, String), TimeSeries]

    points.foreach { case (time, machineDisks) =>
      val ioTimeTotal = new mutable.HashMap[String, Double]()
      machineDisks.foreach { case (_, ioTime, weightedIoTime, machine, disk) =>
        ioTimeTotal += disk -> (ioTime + ioTimeTotal.getOrElse(disk, 0.0))
        seriesMap.getOrElseUpdate((machine, disk), new TimeSeries(s"$machine-$disk-IoTime"))
          .add(new Second(new Date(time)), ioTime)
//          seriesMap.getOrElseUpdate(s"$machine-WeightedIoTime", new TimeSeries(s"$machine-WeightedIoTime"))
//            .add(new Second(new Date(time)), weightedIoTime)
      }
      if (average) {
        machineDisks.groupBy(_._5).foreach { case (disk, machines) =>
          seriesMap.getOrElseUpdate(("Average", disk), new TimeSeries(s"Average IO Time - $disk"))
            .add(new Second(new Date(time)), ioTimeTotal(disk) / machines.length)
          //          val totalWeightedIoTime = machines.map(_._3).sum
          //          val averageWeightedIoTIme = totalWeightedIoTime / machines.length
          //          weightedIoTimeSeries.add(new Second(new Date(time)), averageWeightedIoTIme)
        }
      }
    }

    seriesMap.groupBy(_._1._1).foreach { case (machine, map) =>
      val dataset = new TimeSeriesCollection()
      map.foreach { case ((_, disk), series) =>
        dataset.addSeries(series)
      }
      datasets += dataset
    }

    val output = new File( s"$outputPrefix-disk.png" )
    drawChart(output, stagesDuration, datasets.toArray, "Disk", "Time", "IO Time (ms)", startTime, endTime)
  }

  def drawChart(
      output: File,
      stagesDuration: Seq[(Int, Long, Long)],
      datasets: Array[TimeSeriesCollection],
      title: String,
      timeAxisLabel: String,
      valueAxisLabel: String,
      startTime: Long,
      endTime: Long): Unit = {
    val width = 1280
    val height = 960

    val dateFormat = new RelativeDateFormat(startTime)
    val timeAxis = new DateAxis(timeAxisLabel)
    timeAxis.setAutoRange(true)
    timeAxis.setDateFormatOverride(dateFormat)
    timeAxis.setMinimumDate(new Date(startTime))
    timeAxis.setMaximumDate(new Date(endTime))
    timeAxis.setLowerMargin(0.02)
    timeAxis.setUpperMargin(0.02)


    val valueAxis = new NumberAxis(valueAxisLabel)
    valueAxis.setAutoRangeIncludesZero(false)

    val combinedPlot = new CombinedDomainXYPlot(timeAxis)
    combinedPlot.setOrientation(PlotOrientation.VERTICAL)

    datasets.sortBy { tsc =>
      tsc.getSeries(0).getKey.asInstanceOf[String]
    }
    .foreach { dataset =>
      val plot = new XYPlot(dataset, timeAxis, valueAxis, new StandardXYItemRenderer)
      stagesDuration.sortBy(_._2).zipWithIndex.foreach { case ((id, start, end), index) =>
        val intervalMarker = new IntervalMarker(start, end)
        intervalMarker.setLabel(id.toString)
        intervalMarker.setAlpha(0.2f)
        intervalMarker.setLabelFont(labelFont)
        intervalMarker.setPaint(colors(index % colors.length))
        if (index % 2 == 0) {
          intervalMarker.setLabelOffset(new RectangleInsets(15, 0, 0, 0))
        } else {
          intervalMarker.setLabelOffset(new RectangleInsets(50, 0, 0, 0))
        }
        plot.addDomainMarker(intervalMarker)
      }
      combinedPlot.add(plot)
    }
    val chart = new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, combinedPlot, true)
    ChartUtilities.saveChartAsPNG(output, chart, width, height)

    //    val svg = new SVGGraphics2D(width, height)
    //    chartFactory.draw(svg, new Rectangle(0, 0, width, height))
    //    SVGUtils.writeToSVG(lineChart, svg.getSVGElement())

  }

  @Deprecated
  def processCsvInput(statsDir: String, rrdFile: String): (Long, Array[StageRuntimeStatistic]) = {
    val files = new File(statsDir).listFiles(new PatternFilenameFilter("(.*)\\.csv$"))
    val filesData = files.map { f =>
      val reader = new CsvBeanReader(new FileReader(f), CsvPreference.STANDARD_PREFERENCE)
      try {
        val headers = reader.getHeader(true)
        val stages = Iterator.continually[StageRuntimeStatistic](reader.read(classOf[StageRuntimeStatistic], headers, processors: _*))
          .takeWhile(_ != null).toArray[StageRuntimeStatistic]
        (f.getAbsolutePath, stages)
      } finally {
        reader.close()
      }
    }
    val totalRuntimes = filesData.map { case (_, run) =>
      val startTime = run.map(_.getCompletionTime).min
      val endTime = run.map(_.getCompletionTime).max
      endTime - startTime
    }
    val averageRuntime = totalRuntimes.sum / totalRuntimes.length
    println(s"Average runtime: $averageRuntime")

    val average = filesData.flatMap(run => run._2).groupBy(_.getStageId)
      .map { case (stageId, runs) =>
        val validRuns = runs.map { case (s: StageRuntimeStatistic) =>
          val loadPattern = Pattern.compile("load\\/load.rrd")
          val startDir = new File(rrdFile)
          s.setCpuUsage(
            100 - computeClusterAverage(
              rrdParser,
              cpuPattern,
              blacklisted,
              startDir,
              s.getStartTime,
              s.getCompletionTime,
              computeIdleCpuNodeAverage
            )
          )
          s.setSystemLoad(
            computeClusterAverage(
              rrdParser,
              loadPattern,
              blacklisted,
              startDir,
              s.getStartTime,
              s.getCompletionTime,
              computeLoadNodeAverage
            )
          )
          s.setUpload(
            computeClusterAverage(
              rrdParser,
              networkPattern,
              blacklisted,
              startDir,
              s.getStartTime,
              s.getCompletionTime,
              computeUploadNodeAverage
            )
          )
          s.setDownload(
            computeClusterAverage(
              rrdParser,
              networkPattern,
              blacklisted,
              startDir,
              s.getStartTime,
              s.getCompletionTime,
              computeDownloadNodeAverage
            )
          )
          s
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
            s1.getInitialReadTime + s2.getInitialReadTime
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
          total.getShuffleWriteTime / count / 1000,
          total.getCpuUsage / count,
          total.getSystemLoad / count,
          total.getUpload / count,
          total.getDownload / count,
          total.getPartialOutputWaitTime / count / 1000,
          total.getInitialReadTime / count / 1000
        )
      }.toArray.sortBy(_.getStageId)

    // Plot CPU graph
    println("Plotting CPU graph")
    plotGraphCsv(rrdFile, filesData, rrdParser, cpuPattern, plotCpuGraphPerRun, clusterAverage = true)

    // Plot network graph
    println("Plotting network graph")
    plotGraphCsv(rrdFile, filesData, rrdParser, networkPattern, plotNetworkGraphPerRun, clusterAverage = true)

    // Plot diskgraph
    println("Plotting disk IO graph")
    plotGraphCsv(rrdFile, filesData, rrdParser, diskPattern, plotDiskGraphPerRun, clusterAverage = true)

    (averageRuntime, average)
  }

  @Deprecated
  def plotGraphCsv(
      rrdFile: String,
      runs: Seq[(String, Array[StageRuntimeStatistic])],
      rrdParser: RRDp,
      cpuPattern: Pattern,
      plotFunc: (RRDp, Array[File], String, Long, Long, Seq[(Int, Long, Long)], Boolean) => Unit,
      clusterAverage: Boolean = false): Unit = {
    runs.foreach { case (outputPrefix, stages) =>
      val start = stages.map(_.getStartTime).min
      val end = stages.map(_.getCompletionTime).max
      val stagesDuration = stages.map(s => (s.getStageId.toInt, s.getStartTime.toLong, s.getCompletionTime.toLong))
      val rrdFiles = findRrd(new File(rrdFile), cpuPattern)
      plotFunc(rrdParser, rrdFiles, outputPrefix, start, end, stagesDuration, clusterAverage)
    }
  }
}
