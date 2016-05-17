package pt.tecnico.postprocessing

import java.io.File
import java.util.Date
import java.util.regex.Pattern

import net.stamfest.rrd.RRDp
import org.jfree.data.time.{Second, TimeSeries, TimeSeriesCollection}
import pt.tecnico.spark.util.{AppData, StageData}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by dikei on 5/17/16.
  */
object NetworkAnalyzer {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.NetworkAnalyzer statsDir rrdDir")
      System.exit(-1)
    }

    val statsDir = args(0)
    val rrdDir = args(1)
    val data = Utils.parseJsonInput(statsDir)
    // Plot network graph
    println("Plotting network graph")
    Utils.plotGraphJson(rrdDir, data, Utils.networkPattern, plotNetworkGraphPerRun, clusterAverage = true)
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
    Utils.drawChart(
      output,
      stagesDuration,
      datasets,
      "Network", "Time", "Bandwidth (bytes/s)",
      startTime,
      endTime)
  }

}
