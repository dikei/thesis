package pt.tecnico.postprocessing

import java.awt.{Color, Font}
import java.io.{File, FileReader, FileWriter}
import java.util.Date
import java.util.regex.Pattern

import com.google.common.io.PatternFilenameFilter
import net.stamfest.rrd._
import org.jfree.chart.axis._
import org.jfree.chart.plot._
import org.jfree.chart.renderer.xy.StandardXYItemRenderer
import org.jfree.chart.util.RelativeDateFormat
import org.jfree.chart.{ChartFactory, ChartUtilities, JFreeChart}
import org.jfree.data.time.{Second, TimeSeries, TimeSeriesCollection}
import org.jfree.ui.RectangleInsets
import org.supercsv.cellprocessor._
import org.supercsv.cellprocessor.constraint.NotNull
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.{CsvBeanReader, CsvBeanWriter}
import org.supercsv.prefs.CsvPreference
import pt.tecnico.spark.util.{AppData, StageData, StageRuntimeStatistic}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable

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
  val cpuPattern = Pattern.compile("cpu\\/percent-idle.rrd")
  val networkPattern = Pattern.compile("interface-eth0\\/if_octets.rrd")
  val diskPattern = Pattern.compile("disk-vdb\\/disk_io_time.rrd")

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.StageRuntimeAnalyzer statsDir rrdDir outFile")
      System.exit(-1)
    }

    val statsDir = args(0)
    val rrdFile = args(1)
    val outFile = args(2)

    val (averageRuntime: Long, average: Array[StageRuntimeStatistic]) = processCsvInput(statsDir, rrdFile)

    val writer = new CsvBeanWriter(new FileWriter(outFile), CsvPreference.STANDARD_PREFERENCE)
    val headers = Array (
      "StageId", "Name", "TaskCount", "StageRuntime", "TotalTaskRuntime", "CpuUsage", "SystemLoad", "Upload", "Download"
    )

    val numberFormater = new FmtNumber(".##")
    val networkFormatter = new FmtNumber("#,###")
    val writeProcessors = Array[CellProcessor] (
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      new NotNull(),
      numberFormater,
      numberFormater,
      networkFormatter,
      networkFormatter
    )
    try {
      writer.writeHeader(headers:_*)
      average.foreach { stage =>
        writer.write(stage, headers, writeProcessors)
      }
      writer.writeComment("#")
      writer.writeComment(s"#Total runtime: $averageRuntime ms")
      writer.writeComment("#")
    } finally {
      writer.close()
    }
  }

  def processJsonInput(statsDir: String, rrdFile: String): (Long, Array[StageRuntimeStatistic]) = {
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
          Seq((appData, stages))
        } else {
          Seq()
        }
      } finally {
        source.close()
      }
    }

    val appRuntimes = filesData.map { case (appData, _) =>
      appData.runtime
    }

    val averageRuntime = appRuntimes.sum / appRuntimes.length
    println(s"Average runtime: $averageRuntime")

    val average = filesData.flatMap(run => run._2).groupBy(_.stageId)
      .map { case (stageId, runs) =>
        val validRuns = runs.map { case (stageData: StageData) =>
          val loadPattern = Pattern.compile("load\\/load.rrd")
          val startDir = new File(rrdFile)
          val cpuUsage = 100 - computeClusterAverage(
            rrdParser,
            cpuPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            computeIdleCpuNodeAverage
          )
          val systemLoad = computeClusterAverage(
            rrdParser,
            loadPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            computeLoadNodeAverage
          )
          val upload = computeClusterAverage(
            rrdParser,
            networkPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            computeUploadNodeAverage
          )
          val download = computeClusterAverage(
            rrdParser,
            networkPattern,
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
            download
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
            s1.getDownload + s2.getDownload
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
          total.getFetchWaitTime / count,
          total.getShuffleWriteTime / count,
          total.getCpuUsage / count,
          total.getSystemLoad / count,
          total.getUpload / count,
          total.getDownload / count
        )
      }.toArray.sortBy(_.getStageId)

    // Plot CPU graph
    println("Plotting CPU graph")
    plotGraphJson(rrdFile, filesData, rrdParser, cpuPattern, plotCpuGraphPerRun, clusterAverage = true)

    // Plot network graph
    println("Plotting network graph")
    plotGraphJson(rrdFile, filesData, rrdParser, networkPattern, plotNetworkGraphPerRun, clusterAverage = true)

    // Plot diskgraph
    println("Plotting disk IO graph")
    plotGraphJson(rrdFile, filesData, rrdParser, diskPattern, plotDiskGraphPerRun, clusterAverage = true)

    (averageRuntime, average)
  }

  def plotGraphJson(
    rrdFile: String,
    runs: Seq[(AppData, mutable.Buffer[StageData])],
    rrdParser: RRDp,
    cpuPattern: Pattern,
    plotFunc: (RRDp, Array[File], String, Long, Long, Seq[(Int, Long, Long)], Boolean) => Unit,
    clusterAverage: Boolean = false): Unit = {
    runs.foreach { case (appData, stages) =>
      val start = appData.start
      val end = appData.end
      val stagesDuration = stages.map(s => (s.stageId, s.startTime, s.completionTime))
      val rrdFiles = findRrd(new File(rrdFile), cpuPattern)
      plotFunc(rrdParser, rrdFiles, s"${appData.id}-${appData.name}", start, end, stagesDuration, clusterAverage)
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
      cpuPattern: Pattern,
      startDir: File,
      startTime: Long,
      endTime: Long,
      procFunc: (RRDp, String, Long, Long) => Double
      ): Double = {
    val files = findRrd(startDir, cpuPattern)
    val validResult = files.map { f =>
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
    if (average) {
      val series = new TimeSeries("Average Cluster")
      points.foreach { case (time: Long, machines: Array[(Long, Double, String)]) =>
        val totalCpu = machines.map(_._2).sum
        val averageCpu = totalCpu / machines.length
        series.add(new Second(new Date(time)), averageCpu)
      }
      datasets += new TimeSeriesCollection(series)
    }
    val seriesMap = new scala.collection.mutable.HashMap[String, TimeSeries]
    points.foreach { case (time: Long, machines: Array[(Long, Double, String)]) =>
      machines.foreach { case (_, cpu, machine) =>
        seriesMap.getOrElseUpdate(machine, new TimeSeries(machine)).add(new Second(new Date(time)), cpu)
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
    if (average) {
      val downloadSeries = new TimeSeries("Download")
      val uploadSeries = new TimeSeries("Upload")
      points.foreach { case (time: Long, machines: Array[(Long, Double, Double, String)]) =>
        val totalUpload = machines.map(_._2)
        val totalDownload = machines.map(_._3)
        val averageUpload = totalUpload.sum / totalUpload.length
        val averageDownload = totalDownload.sum / totalDownload.length
        downloadSeries.add(new Second(new Date(time)), averageDownload)
        uploadSeries.add(new Second(new Date(time)), averageUpload)
      }
      val dataset = new TimeSeriesCollection
      dataset.addSeries(uploadSeries)
      dataset.addSeries(downloadSeries)
      datasets += dataset
    }

    val seriesMap = new scala.collection.mutable.HashMap[String, (TimeSeries, TimeSeries)]
    points.foreach { case (time: Long, machines: Array[(Long, Double, Double, String)]) =>
      machines.foreach { case (_, upload, download, machine) =>
        val uploadDownload =
          seriesMap.getOrElseUpdate(
            s"$machine",
            (new TimeSeries(s"$machine-Upload"), new TimeSeries(s"$machine-Download"))
          )
        uploadDownload._1.add(new Second(new Date(time)), upload)
        uploadDownload._2.add(new Second(new Date(time)), download)
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
          (time * 1000, ioTime, weightedIoTime, file.getParentFile.getParentFile.getName)
        }
    }
    .groupBy(t => t._1)

    val datasets = new ArrayBuffer[TimeSeriesCollection]()
    if (average) {
      val ioTimeSeries = new TimeSeries("Average IO Time")
//      val weightedIoTimeSeries = new TimeSeries("Average Weighted IO Write")
      points.foreach { case (time, machines) =>
          val totalIoTime = machines.map(_._2).sum
          val totalWeightedIoTime = machines.map(_._3).sum
          val averageIoTime = totalIoTime / machines.length
//          val averageWeightedIoTIme = totalWeightedIoTime / machines.length
          ioTimeSeries.add(new Second(new Date(time)), averageIoTime)
//          weightedIoTimeSeries.add(new Second(new Date(time)), averageWeightedIoTIme)
      }
      val dataset = new TimeSeriesCollection
      dataset.addSeries(ioTimeSeries)
//      dataset.addSeries(weightedIoTimeSeries)
      datasets += dataset
    }
    val seriesMap = new scala.collection.mutable.HashMap[String, TimeSeries]
    points.foreach { case (time, machines) =>
      machines.foreach { case (_, ioTime, weightedIoTime, machine) =>
        seriesMap.getOrElseUpdate(s"$machine-IoTime", new TimeSeries(s"$machine-IoTime"))
          .add(new Second(new Date(time)), ioTime)
//          seriesMap.getOrElseUpdate(s"$machine-WeightedIoTime", new TimeSeries(s"$machine-WeightedIoTime"))
//            .add(new Second(new Date(time)), weightedIoTime)
      }
    }
    seriesMap.foreach { case (_, series) =>
      datasets += new TimeSeriesCollection(series)
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
        intervalMarker.setAlpha(0.1f)
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
            s1.getDownload + s2.getDownload
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
          total.getFetchWaitTime / count,
          total.getShuffleWriteTime / count,
          total.getCpuUsage / count,
          total.getSystemLoad / count,
          total.getUpload / count,
          total.getDownload / count
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
