package pt.tecnico.postprocessing

import java.io.{File, FileReader, FileWriter}
import java.util.regex.Pattern

import com.google.common.io.PatternFilenameFilter
import org.jfree.chart.{ChartFactory, ChartUtilities}
import org.supercsv.cellprocessor.constraint.NotNull
import org.supercsv.cellprocessor._
import org.supercsv.io.{CsvBeanReader, CsvBeanWriter}
import org.supercsv.prefs.CsvPreference
import pt.tecnico.spark.util.StageRuntimeStatistic
import net.stamfest.rrd._
import org.jfree.data.category.DefaultCategoryDataset
import org.supercsv.cellprocessor.ift.CellProcessor


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
    val rrdFile = args(1)
    val outFile = args(2)
    val files = new File(statsDir).listFiles(new PatternFilenameFilter("(.*)\\.csv$"))
    val rrdParser = new RRDp(".", null)
    val cpuPattern = Pattern.compile("cpu\\/percent-idle.rrd")
    val networkPattern = Pattern.compile("interface-eth0\\/if_octets.rrd")

    val average : Array[StageRuntimeStatistic] = files.flatMap { f =>
      val reader = new CsvBeanReader(new FileReader(f), CsvPreference.STANDARD_PREFERENCE)
      try {
        val headers = reader.getHeader(true)
        Iterator.continually[StageRuntimeStatistic](reader.read(classOf[StageRuntimeStatistic], headers, processors:_*))
          .takeWhile(_ != null).toArray[StageRuntimeStatistic]
      } finally {
        reader.close()
      }
    }
    .groupBy(_.getStageId)
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
    } finally {
      writer.close()
    }

    // Plot CPU graph
    plotCpuGraph(rrdFile, files, rrdParser, cpuPattern)

    // Plot network graph
    plotNetworkGraph(rrdFile, files, rrdParser, networkPattern)
  }

  def plotCpuGraph(rrdFile: String, files: Array[File], rrdParser: RRDp, cpuPattern: Pattern): Unit = {
    files.foreach { f =>
      val reader = new CsvBeanReader(new FileReader(f), CsvPreference.STANDARD_PREFERENCE)
      val stages = try {
        val headers = reader.getHeader(true)
        Iterator.continually[StageRuntimeStatistic](reader.read(classOf[StageRuntimeStatistic], headers, processors: _*))
          .takeWhile(_ != null).toArray[StageRuntimeStatistic]
      } finally {
        reader.close()
      }

      val start = stages.map(_.getStartTime).min
      val end = stages.map(_.getCompletionTime).max
      val rrdFiles = findRrd(new File(rrdFile), cpuPattern)
      plotCpuGraphPerRun(rrdParser, rrdFiles, f.getAbsolutePath, start, end)
    }
  }

  def plotNetworkGraph(rrdFile: String, files: Array[File], rrdParser: RRDp, networkPattern: Pattern): Unit = {
    files.foreach { f =>
      val reader = new CsvBeanReader(new FileReader(f), CsvPreference.STANDARD_PREFERENCE)
      val stages = try {
        val headers = reader.getHeader(true)
        Iterator.continually[StageRuntimeStatistic](reader.read(classOf[StageRuntimeStatistic], headers, processors: _*))
          .takeWhile(_ != null).toArray[StageRuntimeStatistic]
      } finally {
        reader.close()
      }

      val start = stages.map(_.getStartTime).min
      val end = stages.map(_.getCompletionTime).max
      val rrdFiles = findRrd(new File(rrdFile), networkPattern)
      plotNetworkGraphPerRun(rrdParser, rrdFiles, f.getAbsolutePath, start, end)
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

  def plotCpuGraphPerRun(rrdParser: RRDp, files: Array[File], outputPrefix: String, startTime: Long, endTime: Long): Unit = {
    val dataset = new DefaultCategoryDataset
    files.flatMap { file =>
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
          (time - startTime, 100 - idle)
        }
    }
    .groupBy(t => t._1)
    .filter(t => t._2.length > 6)
    .foreach { case (time: Long, idle: Array[(Long, Double)]) =>
      val totalCpu = idle.map(_._2)
      val averageCpu = totalCpu.sum / totalCpu.length
      dataset.addValue(averageCpu, "Usage", time)
    }

    val lineChart = new File( s"$outputPrefix-cpu.png" )
    val width = 1280
    val height = 960
    val chartFactory = ChartFactory.createLineChart(
      "CPU",
      "Time",
      "Usage",
      dataset
    )
    ChartUtilities.saveChartAsPNG(lineChart , chartFactory, width ,height)
  }

  def plotNetworkGraphPerRun(rrdParser: RRDp, files: Array[File], outputPrefix: String, startTime: Long, endTime: Long): Unit = {
    val dataset = new DefaultCategoryDataset
    files.flatMap { file =>
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
          (time - startTime, upload, download)
        }
    }
    .groupBy(t => t._1)
    .filter(t => t._2.length > 6)
    .foreach { case (time: Long, idle: Array[(Long, Double, Double)]) =>
      val totalUpload = idle.map(_._2)
      val totalDownload = idle.map(_._3)
      val averageUpload = totalUpload.sum / totalUpload.length
      val averageDownload = totalDownload.sum / totalDownload.length
      dataset.addValue(averageUpload, "Upload", time)
      dataset.addValue(averageDownload, "Download", time)
    }

    val lineChart = new File( s"$outputPrefix-network.png" )
    val width = 1280
    val height = 960
    val chartFactory = ChartFactory.createLineChart(
      "Network",
      "Time",
      "Average speed (byte/s)",
      dataset
    )
    ChartUtilities.saveChartAsPNG(lineChart , chartFactory, width ,height)
  }
}
