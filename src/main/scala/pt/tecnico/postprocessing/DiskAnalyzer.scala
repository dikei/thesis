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
object DiskAnalyzer {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.DiskAnalyzer statsDir rrdDir")
      System.exit(-1)
    }

    val statsDir = args(0)
    val rrdDir = args(1)
    val data = Utils.parseJsonInput(statsDir)

    // Plot disk graph
    println("Plotting disk IO graph")
    Utils.plotGraphJson(rrdDir, data, Utils.diskPattern, plotDiskGraphPerRun, clusterAverage = true)
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
          val read = tokens(2).trim().toDouble
          val write = tokens(3).trim().toDouble
          val time = tokens(0).toLong
          (time * 1000, read, write, file.getParentFile.getParentFile.getName, file.getParentFile.getName)
        }
    }
      .groupBy(t => t._1)

    val datasets = new ArrayBuffer[TimeSeriesCollection]()
    val seriesMap = new scala.collection.mutable.HashMap[(String, String, String), TimeSeries]

    points.foreach { case (time, machineDisks) =>
      val readTotal = new mutable.HashMap[String, Double]()
      val writeTotal = new mutable.HashMap[String, Double]()
      machineDisks.foreach { case (_, read, write, machine, disk) =>
        readTotal += disk -> (read + readTotal.getOrElse(disk, 0.0))
        writeTotal += disk -> (write + writeTotal.getOrElse(disk, 0.0))
        seriesMap.getOrElseUpdate((machine, disk, "read"), new TimeSeries(s"$machine-$disk-Read"))
          .add(new Second(new Date(time)), read)
        seriesMap.getOrElseUpdate((machine, disk, "write"), new TimeSeries(s"$machine-$disk-Write"))
          .add(new Second(new Date(time)), write)
      }
      if (average) {
        machineDisks.groupBy(_._5).foreach { case (disk, machines) =>
          seriesMap.getOrElseUpdate(("Average", disk, "read"), new TimeSeries(s"Average Read - $disk"))
            .add(new Second(new Date(time)), readTotal(disk) / machines.length)
          seriesMap.getOrElseUpdate(("Average", disk, "write"), new TimeSeries(s"Average Write - $disk"))
            .add(new Second(new Date(time)), writeTotal(disk) / machines.length)
          //          val totalWeightedIoTime = machines.map(_._3).sum
          //          val averageWeightedIoTIme = totalWeightedIoTime / machines.length
          //          weightedIoTimeSeries.add(new Second(new Date(time)), averageWeightedIoTIme)
        }
      }
    }

    seriesMap.groupBy(_._1._1).foreach { case (machine, map) =>
      val dataset = new TimeSeriesCollection()
      map.foreach { case ((_, _, _), series) =>
        dataset.addSeries(series)
      }
      datasets += dataset
    }

    val output = new File( s"$outputPrefix-disk.png" )
    Utils.drawChart(output, stagesDuration, datasets, "Disk", "Time", "IO Time (ms)", startTime, endTime)
  }


}
