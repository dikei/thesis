package pt.tecnico.postprocessing

import java.awt.{Color, Font}
import java.io.File
import java.util.Date
import java.util.regex.Pattern

import com.google.common.io.PatternFilenameFilter
import net.stamfest.rrd.RRDp
import org.jfree.chart.{ChartUtilities, JFreeChart}
import org.jfree.chart.axis.{DateAxis, NumberAxis}
import org.jfree.chart.plot.{CombinedDomainXYPlot, IntervalMarker, PlotOrientation, XYPlot}
import org.jfree.chart.renderer.xy.StandardXYItemRenderer
import org.jfree.chart.util.RelativeDateFormat
import org.jfree.data.time.TimeSeriesCollection
import org.jfree.ui.RectangleInsets
import org.json4s.{DefaultFormats, ShortTypeHints}
import org.json4s.native.JsonMethods._
import pt.tecnico.spark.util.{AppData, ReadMethod, StageData}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by dikei on 5/17/16.
  */
object Utils {

  val blacklisted = Pattern.compile("master")
  val cpuPattern = Pattern.compile("cpu\\/percent-idle\\.rrd")
  val diskPattern = Pattern.compile("disk-vda\\/disk_octets\\.rrd")
  val networkPattern = Pattern.compile("interface-eth0\\/if_octets\\.rrd")
  val loadPattern = Pattern.compile("load\\/load\\.rrd")
  val rrdParser = new RRDp(".", null)

  def parseJsonInput(statsDir: String, stageFilter: Option[(StageData) => Boolean] = None): Array[(AppData, Seq[StageData], String)] = {
    implicit val formats = DefaultFormats + new org.json4s.ext.EnumSerializer(ReadMethod)

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
            if (stageFilter.isEmpty) {
              stages += stage
            } else {
              stageFilter.foreach { func =>
                if (func(stage)) {
                  stages += stage
                }
              }
            }
          }
          i += 1
        }
        appData.start = stages.filter(_.taskCount > 0).map(_.startTime).min
        appData.end = stages.filter(_.taskCount > 0).map(_.completionTime).max

        // Skip over file that contained failed run
        if (!failureDetected) {
          Seq((appData, stages.toSeq, f.getAbsolutePath))
        } else {
          Seq.empty
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
    filesData
  }

  def plotGraphJson(
      rrdDir: String,
      runs: Seq[(AppData, Seq[StageData], String)],
      matchPattern: Pattern,
      plotFunc: (RRDp, Array[File], String, Long, Long, Seq[(Int, Long, Long)], Boolean) => Unit,
      clusterAverage: Boolean = false): Unit = {
    runs.foreach { case (appData, stages, outputPrefix) =>
      val start = appData.start
      val end = appData.end
      val stagesDuration = stages.map(s => (s.stageId, s.startTime, s.completionTime))
      val rrdFiles = findRrd(new File(rrdDir), matchPattern).filter { f =>
        !blacklisted.matcher(f.getAbsolutePath).find()
      }
      plotFunc(rrdParser, rrdFiles, outputPrefix, start, end, stagesDuration, clusterAverage)
    }
  }

  def drawChart(
     output: File,
     stagesDuration: Seq[(Int, Long, Long)],
     datasets: Seq[TimeSeriesCollection],
     title: String,
     timeAxisLabel: String,
     valueAxisLabel: String,
     startTime: Long,
     endTime: Long): Unit = {
    val width = 1280
    val height = 960
    val labelFont = new Font("Dialog", Font.PLAIN, 25)
    val colors = Array (
      new Color(230,97,1),
      new Color(253,184,99),
      new Color(178,171,210),
      new Color(94,60,153)
    )

    val dateFormat = new RelativeSecondFormat(startTime)
    val timeAxis = new CustomDateAxis(timeAxisLabel)
    timeAxis.setAutoRange(true)
    timeAxis.setDateFormatOverride(dateFormat)
    timeAxis.setMinimumDate(new Date(startTime))
    timeAxis.setMaximumDate(new Date(endTime))
    timeAxis.setLowerMargin(0.02)
    timeAxis.setUpperMargin(0.02)


    val valueAxis = new NumberAxis(valueAxisLabel)
    valueAxis.setAutoRangeIncludesZero(true)

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
      matchPattern: Pattern,
      rrdDir: File,
      startTime: Long,
      endTime: Long,
      procFunc: (RRDp, String, Long, Long) => Double): Double = {
    val files = findRrd(rrdDir, matchPattern)
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

  def computeDiskReadNodeAverage(rrdParser: RRDp, file: String, startTime: Long, endTime: Long): Double = {
    val command = Array[String] (
      "fetch", file, "AVERAGE",
      "-s", (startTime / 1000).toString,
      "-e", (endTime / 1000).toString
    )
    val rrdResult = rrdParser.command(command)
    val lines = rrdResult.getOutput.trim.split("\n")
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
      totalLines
    } else {
      Double.NaN
    }
  }

  def computeDiskWriteNodeAverage(rrdParser: RRDp, file: String, startTime: Long, endTime: Long): Double = {
    val command = Array[String] (
      "fetch", file, "AVERAGE",
      "-s", (startTime / 1000).toString,
      "-e", (endTime / 1000).toString
    )
    val rrdResult = rrdParser.command(command)
    val lines = rrdResult.getOutput.trim.split("\n")
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
      totalLines
    } else {
      Double.NaN
    }
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
      totalLines
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
      totalLines
    } else {
      Double.NaN
    }
  }

  def trimRuns(runs: Seq[(AppData, Seq[StageData], String)], percent: Int): Seq[(AppData, Seq[StageData], String)] = {
    val runCount = runs.length

    val lower = runCount * percent / 100
    val upper = runCount * (100 - percent) / 100
    println(s"Trimming: $lower, $upper")
    runs.sortBy { case (appData, _, _) =>
      appData.runtime
    }.slice(lower, upper + 1)
  }

  def stageFilter(stage: StageData): Boolean =  stage.jobId > 0 // stage.stageId >= 7 && stage.stageId <= 9
}
