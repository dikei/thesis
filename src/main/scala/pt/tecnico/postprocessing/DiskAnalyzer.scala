package pt.tecnico.postprocessing

import java.io.{File, FileWriter}
import java.util.Date
import java.util.regex.Pattern

import net.stamfest.rrd.RRDp
import org.jfree.data.time.{Second, TimeSeries, TimeSeriesCollection}
import org.supercsv.cellprocessor.FmtNumber
import org.supercsv.cellprocessor.constraint.NotNull
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvBeanWriter
import org.supercsv.prefs.CsvPreference
import pt.tecnico.spark.util.{AppData, StageData, StageDiskStatistic, StageRuntimeStatistic}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by dikei on 5/17/16.
  */
object DiskAnalyzer {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.DiskAnalyzer statsDir rrdDir csvFile")
      System.exit(-1)
    }

    val statsDir = args(0)
    val rrdDir = args(1)
    val data = Utils.parseJsonInput(statsDir, Some(Utils.stageFilter))

    // Plot disk graph
    println("Plotting disk IO graph")
    Utils.plotGraphJson(rrdDir, data, Utils.diskPattern, plotDiskGraphPerRun, clusterAverage = true)

    if (args.length > 2) {
      diskUsagePerStage(Utils.trimRuns(data, 10), rrdDir, args(2))
    }
  }

  def diskUsagePerStage(data: Seq[(AppData, Seq[StageData], String)], rrdDir: String, outFile: String): Unit = {
    val average = data.flatMap(run => run._2).groupBy(_.stageId)
      .map { case (stageId, stages) =>
        val validRuns = stages.filter(_.taskCount > 0).map { case (stageData: StageData) =>
          val startDir = new File(rrdDir)
          val diskRead = Utils.computeClusterAverage(
            Utils.diskPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            Utils.computeDiskReadNodeAverage
          )
          val diskWrite = Utils.computeClusterAverage(
            Utils.diskPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            Utils.computeDiskWriteNodeAverage
          )

          val ret = new StageDiskStatistic(
            stageId,
            stageData.name,
            diskRead,
            diskWrite)
          (ret, 1)
        }.filter { case (s, _) =>
          !s.getDiskRead.isNaN && !s.getDiskWrite.isNaN
        }

        val (total, count) = validRuns.reduce[(StageDiskStatistic, Int)] {
          case ((s1: StageDiskStatistic, c1: Int), (s2: StageDiskStatistic, c2: Int)) =>
            val ret = new StageDiskStatistic(
              stageId,
              s1.getStageName,
              s1.getDiskRead + s2.getDiskRead,
              s1.getDiskWrite + s2.getDiskWrite)
            (ret, c1 + c2)
        }

        new StageDiskStatistic(
          stageId,
          total.getStageName,
          total.getDiskRead / count,
          total.getDiskWrite / count
        )
      }.toArray.sortBy(_.getStageId)

    val writer = new CsvBeanWriter(new FileWriter(outFile), CsvPreference.STANDARD_PREFERENCE)
    val headers = Array (
      "StageId", "StageName", "DiskRead", "DiskWrite"
    )

    val sizeFormatter = new FmtNumber("#,###")
    val writeProcessors = Array[CellProcessor] (
      new NotNull(),
      new NotNull(),
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
