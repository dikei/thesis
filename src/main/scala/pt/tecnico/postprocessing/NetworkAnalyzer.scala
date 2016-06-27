package pt.tecnico.postprocessing

import java.io.{File, FileWriter}
import java.util.Date

import net.stamfest.rrd.RRDp
import org.jfree.data.time.{Second, TimeSeries, TimeSeriesCollection}
import org.supercsv.cellprocessor.FmtNumber
import org.supercsv.cellprocessor.constraint.NotNull
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvBeanWriter
import org.supercsv.prefs.CsvPreference
import pt.tecnico.spark.util.{AppData, StageData, NetworkStatistic}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by dikei on 5/17/16.
  */
object NetworkAnalyzer {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.NetworkAnalyzer statsDir rrdDir csvFile")
      System.exit(-1)
    }

    val statsDir = args(0)
    val rrdDir = args(1)
    val data = Utils.parseJsonInput(statsDir, Some(Utils.stageFilter))

    // Plot network graph
    println("Plotting network graph")
    Utils.plotGraphJson(rrdDir, data, Utils.networkPattern, plotNetworkGraphPerRun, clusterAverage = true)

    if (args.length > 2) {
      networkUsagePerStage(Utils.trimRuns(data, 10), rrdDir, args(2))
    }
  }

  def networkUsagePerStage(data: Seq[(AppData, Seq[StageData], String)], rrdDir: String, outFile: String): Unit = {
    val startDir = new File(rrdDir)
    val average = data.flatMap(run => run._2).groupBy(_.stageId)
      .map { case (stageId, stages) =>
        val validRuns = stages.filter(_.taskCount > 0).map { case (stageData: StageData) =>
          val download = Utils.computeClusterAverage(
            Utils.networkPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            Utils.computeDownloadNodeAverage
          )
          val upload = Utils.computeClusterAverage(
            Utils.networkPattern,
            startDir,
            stageData.startTime,
            stageData.completionTime,
            Utils.computeUploadNodeAverage
          )

          val ret = new NetworkStatistic(
            stageId,
            stageData.name,
            download,
            upload)
          (ret, 1)
        }.filter { case (s, _) =>
          !s.getDownload.isNaN && !s.getUpload.isNaN
        }

        val (total, count) = validRuns.reduce[(NetworkStatistic, Int)] {
          case ((s1: NetworkStatistic, c1: Int), (s2: NetworkStatistic, c2: Int)) =>
            val ret = new NetworkStatistic(
              stageId,
              s1.getStageName,
              s1.getDownload + s2.getDownload,
              s1.getUpload + s2.getUpload)
            (ret, c1 + c2)
        }

        new NetworkStatistic(
          stageId,
          total.getStageName,
          total.getDownload / count,
          total.getUpload / count
        )
      }.toArray.sortBy(_.getStageId)

    val validRuns = data.map(_._1).map { appData =>
      val download = Utils.computeClusterAverage(
        Utils.networkPattern,
        startDir,
        appData.start,
        appData.end,
        Utils.computeDownloadNodeAverage
      )
      val upload = Utils.computeClusterAverage(
        Utils.networkPattern,
        startDir,
        appData.start,
        appData.end,
        Utils.computeUploadNodeAverage
      )
      val ret = new NetworkStatistic(
        -1,
        "App",
        download,
        upload)
      (ret, 1)
    }.filter { case (s, _) =>
      !s.getDownload.isNaN && !s.getUpload.isNaN
    }

    val (total, count) = validRuns.reduce [(NetworkStatistic, Int)] {
      case ((s1: NetworkStatistic, c1: Int), (s2: NetworkStatistic, c2: Int)) =>
        val ret = new NetworkStatistic(
          -1,
          s1.getStageName,
          s1.getDownload + s2.getDownload,
          s1.getUpload + s2.getUpload)
        (ret, c1 + c2)
    }

    val allStages = new NetworkStatistic(
      -1,
      total.getStageName,
      total.getDownload / count,
      total.getUpload / count
    )

    val writer = new CsvBeanWriter(new FileWriter(outFile), CsvPreference.STANDARD_PREFERENCE)
    val headers = Array (
      "StageId", "StageName", "Download", "Upload"
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
      writer.write(allStages, headers, writeProcessors)
    } finally {
      writer.close()
    }
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
