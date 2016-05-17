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
    Utils.drawChart(output, stagesDuration, datasets, "Disk", "Time", "IO Time (ms)", startTime, endTime)
  }


}
