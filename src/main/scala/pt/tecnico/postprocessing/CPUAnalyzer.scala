package pt.tecnico.postprocessing

import java.io.File
import java.util.Date
import java.util.regex.Pattern

import net.stamfest.rrd.RRDp
import org.jfree.data.time.{Second, TimeSeries, TimeSeriesCollection}
import pt.tecnico.spark.util.{AppData, StageData}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by dikei on 5/17/16.
  */
object CPUAnalyzer {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.CPUAnalyzer statsDir rrdDir")
      System.exit(-1)
    }

    val statsDir = args(0)
    val rrdDir = args(1)
    val data = Utils.parseJsonInput(statsDir)

    // Plot CPU graph
    println("Plotting CPU graph")
    Utils.plotGraphJson(rrdDir, data, Utils.cpuPattern, plotCpuGraphPerRun, clusterAverage = true)
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
    Utils.drawChart(output, stagesDuration, datasets, "CPU", "Time", "Usage (%)", startTime, endTime)
  }
}
