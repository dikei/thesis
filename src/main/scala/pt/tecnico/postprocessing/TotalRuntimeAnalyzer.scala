package pt.tecnico.postprocessing

import java.awt.Font
import java.io.{File, FileWriter}

import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.inference.TTest
import org.jfree.chart.axis.{CategoryAxis, NumberAxis}
import org.jfree.chart.plot.CategoryPlot
import org.jfree.chart.renderer.category.StatisticalBarRenderer
import org.jfree.chart.{ChartUtilities, JFreeChart}
import org.jfree.data.statistics.DefaultStatisticalCategoryDataset
import org.supercsv.cellprocessor.FmtNumber
import org.supercsv.cellprocessor.constraint.NotNull
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.io.CsvBeanWriter
import org.supercsv.prefs.CsvPreference
import pt.tecnico.spark.util.{AppData, AppRuntimeStatistic, StageData}


/**
  * Created by dikei on 5/17/16.
  */
object TotalRuntimeAnalyzer {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.TotalRuntimeAnalyzer barrierDir noBarrierDir output")
      System.exit(-1)
    }

    val barrierDirs = new File(args(0)).listFiles.filter(_.isDirectory)
    val noBarrierDirs = new File(args(1)).listFiles.filter(_.isDirectory)
    val output = args(2)

    val barrierDatas = barrierDirs.map { dir =>
      (dir.getName, Utils.trimRuns(Utils.parseJsonInput(dir.getAbsolutePath, Some(Utils.stageFilter)), 10))
    }.toMap
    val noBarrierDatas = noBarrierDirs.map { dir =>
      (dir.getName, Utils.trimRuns(Utils.parseJsonInput(dir.getAbsolutePath, Some(Utils.stageFilter)), 10))
    }.toMap

//    println("Plotting runtime distribution")
//    plotExecutionTime(barrierDatas, noBarrierDatas, output)

    println("Dumping statistic")
    dumpExecutionStatistic(barrierDatas, noBarrierDatas, output)
  }

  def plotExecutionTime(
      barrierDatas: Map[String, Seq[(AppData, Seq[StageData], String)]],
      noBarrierDatas: Map[String, Seq[(AppData, Seq[StageData], String)]],
      outputFile: String): Unit = {

    val dataset = new DefaultStatisticalCategoryDataset
    barrierDatas.filterKeys(k => noBarrierDatas.contains(k)).toSeq.sortBy(_._1).
      foreach { case (name, barrierData) =>
        val barrierStatistic = new DescriptiveStatistics
        barrierData.foreach { case (appData, _, _) =>
          barrierStatistic.addValue(appData.runtime)
        }

        val noBarrierStatistic = new DescriptiveStatistics
        val noBarrierData = noBarrierDatas(name)
        noBarrierData.foreach { case (appData, _, _) =>
          noBarrierStatistic.addValue(appData.runtime)
        }
        println(s"$name: ${barrierStatistic.getMean}, ${barrierStatistic.getStandardDeviation}")
        println(s"$name: ${noBarrierStatistic.getMean}, ${noBarrierStatistic.getStandardDeviation}")
        dataset.add(barrierStatistic.getMean, barrierStatistic.getStandardDeviation, "Barrier", name)
        dataset.add(noBarrierStatistic.getMean, noBarrierStatistic.getStandardDeviation, "NoBarrier", name)
      }

    val xAxis = new CategoryAxis("Input size")
    xAxis.setLowerMargin(0.02d) // percentage of space before first bar
    xAxis.setUpperMargin(0.02d) // percentage of space after last bar
    xAxis.setCategoryMargin(0.1d) // percentage of space between categories
    val yAxis = new NumberAxis("Time")
    yAxis.setAutoRange(true)

    // define the plot
    val renderer = new StatisticalBarRenderer()
    val plot = new CategoryPlot(dataset, xAxis, yAxis, renderer)

    val chart = new JFreeChart("Statistical Bar Chart Demo",
      new Font("Helvetica", Font.BOLD, 14),
      plot,
      true)

    val output = new File(outputFile)
    val width = 1280 * 2
    val height = 960 * 4
    ChartUtilities.saveChartAsPNG(output, chart, width, height)
  }

  def dumpExecutionStatistic(
      barrierDatas: Map[String, Seq[(AppData, Seq[StageData], String)]],
      noBarrierDatas: Map[String, Seq[(AppData, Seq[StageData], String)]],
      outputFile: String): Unit = {
    val appRuntimeCompare = barrierDatas.filterKeys(k => noBarrierDatas.contains(k)).toSeq.sortBy(_._1).
      map { case (name, barrierData) =>
        val barrierStatistic = new DescriptiveStatistics
        barrierData.foreach { case (appData, _, _) =>
          barrierStatistic.addValue(appData.runtime)
        }

        val noBarrierStatistic = new DescriptiveStatistics
        val noBarrierData = noBarrierDatas(name)
        noBarrierData.foreach { case (appData, _, _) =>
          noBarrierStatistic.addValue(appData.runtime)
        }

        val barrierStats = RuntimeStatistic(barrierStatistic)
        val noBarrierStats = RuntimeStatistic(noBarrierStatistic)

        val tTest = new TTest
        val pValue = tTest.tTest(noBarrierStats.stats, barrierStats.stats)

        // Test if we can reject no barrier = barrier with confident interval 0.95
        val significant = tTest.tTest(noBarrierStats.stats, barrierStats.stats, 0.05)

        val meanDiff = (barrierStats.avg - noBarrierStats.avg).toDouble
        val sse = barrierStats.sse + noBarrierStats.sse
        val df =  degreeOfFreedom(barrierStats.variance, barrierStats.samples, noBarrierStats.variance, noBarrierStats.samples)
        val mse = sse / df
        val harmonicN = barrierStats.samples * noBarrierStats.samples / barrierStats.samples + noBarrierStats.samples
        val S = Math.sqrt(mse * 2 / harmonicN)
        val criticalValue = new TDistribution(df).inverseCumulativeProbability(1 - 0.05 / 2)
        val (meanDiffLower, meanDiffUpper) = (meanDiff - criticalValue * S, meanDiff + criticalValue * S)

        val speedup = meanDiff * 100 / barrierStats.avg
        val speedupLower = meanDiffLower * 100 / barrierStats.avg
        val speedupUpper = meanDiffUpper * 100 / barrierStats.avg

        new AppRuntimeStatistic(
          name,
          barrierStats.avg, barrierStats.lower, barrierStats.upper, barrierStats.stdDev, barrierStats.samples,
          noBarrierStats.avg, noBarrierStats.lower, noBarrierStats.upper, noBarrierStats.stdDev, noBarrierStats.samples,
          significant,
          speedup, speedupLower, speedupUpper)
      }

    val writer = new CsvBeanWriter(new FileWriter(outputFile), CsvPreference.STANDARD_PREFERENCE)
    val headers = Array (
      "Name",
      "Barrier", "BarrierLower", "BarrierUpper", "BarrierStdDev", "BarrierSample",
      "NoBarrier", "NoBarrierLower", "NoBarrierUpper", "NoBarrierStdDev", "NoBarrierSample",
      "SignificantDifferent",
      "SpeedUp", "SpeedUpLower", "SpeedUpUpper"
    )

    val numFormatter = new FmtNumber("#.##")
    val writeProcessors = Array[CellProcessor] (
      new NotNull(),
      numFormatter, numFormatter, numFormatter, numFormatter, numFormatter,
      numFormatter, numFormatter, numFormatter, numFormatter, numFormatter,
      new NotNull(),
      numFormatter, numFormatter, numFormatter
    )
    try {
      writer.writeHeader(headers:_*)
      appRuntimeCompare.foreach { row =>
        writer.write(row, headers, writeProcessors)
      }
    } finally {
      writer.close()
    }
  }

  def degreeOfFreedom(v1: Double, n1: Long, v2: Double, n2: Long): Double = {
    (((v1 / n1) + (v2 / n2)) * ((v1 / n1) + (v2 / n2))) /
      ((v1 * v1) / (n1 * n1 * (n1 - 1d)) + (v2 * v2) / (n2 * n2 * (n2 - 1d)))
  }
}
