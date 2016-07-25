package pt.tecnico.postprocessing

import java.awt.Color
import java.io.File

import org.jfree.chart.{ChartFactory, ChartUtilities}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.renderer.xy.{StandardXYBarPainter, XYBarRenderer}
import org.jfree.chart.title.TextTitle
import org.jfree.data.statistics.{SimpleHistogramBin, SimpleHistogramDataset}
import pt.tecnico.spark.util.{AppData, StageData}


/**
  * Created by dikei on 5/17/16.
  */
object TotalRuntimeAnalyzer {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.TotalRuntimeAnalyzer statsDir output")
      System.exit(-1)
    }

    val statsDir = args(0)
    val output = args(1)
    val data = Utils.parseJsonInput(statsDir, Some(Utils.stageFilter))

    println("Plotting runtime distribution")
    plotRuntimeDistribution(Utils.trimRuns(data, 10), output)
  }



  def plotRuntimeDistribution(runs: Seq[(AppData, Seq[StageData], String)], output: String): Unit = {
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
    while(averageInSecond - binsize * binCount >= bestRuntime / 1000 ||
      averageInSecond + binsize * binCount <= worseRuntime / 1000) {
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
    variance = variance / appRuntimes.length

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

    chart.addSubtitle(new TextTitle(s"Average time: $averageRuntime ms"))
    chart.addSubtitle(new TextTitle(s"Standard deviation: $standardDeviation ms"))
    chart.addSubtitle(new TextTitle(s"Best time: $bestRuntime ms"))
    chart.addSubtitle(new TextTitle(s"Worse time: $worseRuntime ms"))
    val width = 1280
    val height = 960

    ChartUtilities.saveChartAsPNG(new File(output), chart, width, height)
  }
}
