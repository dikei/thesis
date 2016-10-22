package pt.tecnico.postprocessing

import java.awt.{BasicStroke, Color, Font}
import java.io.File
import java.util.Date

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.stat.inference.TTest
import org.jfree.chart.axis.{CategoryAxis, DateTickUnit, DateTickUnitType}
import org.jfree.chart.plot._
import org.jfree.chart.renderer.category.StandardBarPainter
import org.jfree.chart.title.TextTitle
import org.jfree.chart.{ChartUtilities, JFreeChart}
import org.jfree.data.gantt.{Task, TaskSeries, TaskSeriesCollection}
import org.jfree.data.time.SimpleTimePeriod
import org.jfree.ui.{RectangleAnchor, RectangleInsets}
import pt.tecnico.spark.util.{AppData, StageData}

import scala.collection.mutable

case class RuntimeStatistic(stats: DescriptiveStatistics) {
  lazy val (lower, upper) = calculateCI
  def avg = stats.getMean
  def median = stats.getPercentile(50)
  def percent90 = stats.getPercentile(90)
  def stdDev = stats.getStandardDeviation
  def samples = stats.getN
  def variance = stats.getVariance

  def calculateCI : (Double, Double) = {
    // Calculate 95% confident interval using Student t's distribution
    val tDist = new TDistribution(stats.getN - 1)
    val criticalValue = tDist.inverseCumulativeProbability(1.0 - 0.05 / 2)
    val ci = criticalValue * stats.getStandardDeviation / Math.sqrt(stats.getN)

    (avg - ci, avg + ci)
  }

  def sse : Double = {
    var ret = 0.0
    (0 until samples.toInt).foreach { i =>
      ret += Math.pow(stats.getElement(i) - stats.getMean, 2)
    }
    ret
  }

  override def toString: String = {
    s"Avg: $avg,    Median: $median,    90-th Percentile: $percent90,    StdDev: $stdDev,    Samples: $samples    " +
    s"95% Confidence Interval: $lower - $upper"
  }
}

/**
  * Created by dikei on 6/17/16.
  */
object StageRuntimeComparer {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.StageRuntimeComparer barrierDir noBarrierDir outfile")
      System.exit(-1)
    }

    val barrierDir = args(0)
    val noBarrierDir = args(1)
    val outFile = args(2)

    val barrierData = Utils.parseJsonInput(barrierDir, stageFilter = Some(Utils.stageFilter))
    val noBarrierData = Utils.parseJsonInput(noBarrierDir, stageFilter = Some(Utils.stageFilter))

    val barrierAverage = computeAverageStageData(Utils.trimRuns(barrierData, 10))
    val noBarrierAverage = computeAverageStageData(Utils.trimRuns(noBarrierData, 10))

    println("Plotting average stage gantt chart")
    plotStageGanttChartAverage(barrierAverage, noBarrierAverage, outFile)
  }

  def plotStageGanttChartAverage(
      barrierData: (Seq[(Int, Seq[StageData])], RuntimeStatistic),
      noBarrierData: (Seq[(Int, Seq[StageData])], RuntimeStatistic),
      outputFile: String): Unit = {


    val collection = new TaskSeriesCollection()
    val markers = mutable.Buffer[Marker]()
    val barrierSeries = new TaskSeries("Batch Execution")
    val noBarrierSeries = new TaskSeries("Pipeline Execution")

    val jobFont = new Font("Dialog", Font.PLAIN, 40)
    val jobBackgrounds = Array (
      new Color(247,247,247),
      new Color(99,99,99)
    )


    val barrierStagesData = barrierData._1
    val barrierJobCount = barrierStagesData.length
    barrierStagesData.zipWithIndex.foreach { case ((jobId, jobStages), index) =>
      val sortedStages = jobStages.sortBy(_.startTime)
      var startTime = java.lang.Long.MAX_VALUE
      var endTime = -1L

      sortedStages.foreach { stage =>
        if (startTime > stage.startTime) {
          startTime = stage.startTime
        }
        if (endTime < stage.completionTime) {
          endTime = stage.completionTime
        }
        val task = new Task(stage.stageId.toString, new SimpleTimePeriod(stage.startTime, stage.completionTime))
        barrierSeries.add(task)
      }
      if (index < barrierJobCount - 1) {
        val jobMarker = new ValueMarker(endTime)
        jobMarker.setAlpha(1.0f)
        jobMarker.setPaint(Color.RED)
        jobMarker.setStroke(new BasicStroke(
          0.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
          1.0f, Array(20.0f, 10.0f), 0.0f
        ))
        markers += jobMarker
      }
    }

    val noBarrierStageData = noBarrierData._1
    val noBarrierJobCount = noBarrierStageData.length
    noBarrierStageData.zipWithIndex.foreach { case ((jobId, jobStages), index) =>
      val sortedStages = jobStages.sortBy(_.startTime)
      var startTime = java.lang.Long.MAX_VALUE
      var endTime = -1L

      sortedStages.foreach { stage =>
        if (startTime > stage.startTime) {
          startTime = stage.startTime
        }
        if (endTime < stage.completionTime) {
          endTime = stage.completionTime
        }
        val task = new Task(stage.stageId.toString, new SimpleTimePeriod(stage.startTime, stage.completionTime))
        noBarrierSeries.add(task)
      }
      if (index < noBarrierJobCount - 1) {
        val jobMarker = new ValueMarker(endTime)
        jobMarker.setAlpha(1.0f)
        jobMarker.setPaint(Color.BLUE)
        jobMarker.setStroke(new BasicStroke(
          0.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
          1.0f, Array(20.0f, 10.0f), 0.0f
        ))
        markers += jobMarker
      }
    }

    collection.add(barrierSeries)
    collection.add(noBarrierSeries)
    val dateFormat = new RelativeSecondFormat(0)
//    val axisFont = new Font("SansSerif", Font.PLAIN, 50)

    val timeAxis = new CustomDateAxis("Time (s)")
//    timeAxis.setLabelFont(axisFont)
    timeAxis.setLowerBound(0)
    timeAxis.setDateFormatOverride(dateFormat)
    timeAxis.setAutoRange(true)

    timeAxis.setRange(new Date(0), new Date(Math.max(barrierData._2.avg, noBarrierData._2.avg).round))
    timeAxis.setTickUnit(new DateTickUnit(DateTickUnitType.SECOND, 30))
    timeAxis.setTickMarksVisible(true)
    timeAxis.setLowerMargin(0.02)
    timeAxis.setUpperMargin(0.02)

    val categoryAxis = new CategoryAxis("Stage (id)")
//    categoryAxis.setLabelFont(axisFont)

    val renderer = new StageGanttRenderer(collection)
    renderer.setBarPainter(new StandardBarPainter)
    renderer.setDrawBarOutline(false)
    renderer.setShadowVisible(false)
    val plot = new CategoryPlot(collection, categoryAxis, timeAxis, renderer)
    plot.setRangeGridlinesVisible(false)
    plot.setOrientation(PlotOrientation.HORIZONTAL)
    plot.getDomainAxis.setCategoryMargin(0.3)
    markers.foreach { marker =>
      plot.addRangeMarker(marker)
    }
    val chart = new JFreeChart("", JFreeChart.DEFAULT_TITLE_FONT, plot, true)
    chart.setBackgroundPaint(Color.WHITE)

//    val legendFont = new Font("SansSerif", Font.PLAIN, 50)
//    chart.getLegend.setItemFont(legendFont)

    val barrierStats = barrierData._2
    val noBarrierStats = noBarrierData._2

    val barrierTitle = new TextTitle(barrierStats.toString)
    barrierTitle.setPaint(Color.RED)
//    barrierTitle.getHeight
//    chart.addSubtitle(barrierTitle)

    val noBarrierTitle = new TextTitle(noBarrierStats.toString)
    noBarrierTitle.setPaint(Color.BLUE)
//    chart.addSubtitle(noBarrierTitle)

    val tTest = new TTest
    val pValue = tTest.tTest(noBarrierStats.stats, barrierStats.stats)
    println(s"P: $pValue")

    // Test if we can reject no barrier = barrier with confident interval 0.95
    val significant = tTest.tTest(noBarrierStats.stats, barrierStats.stats, 0.05)

    val meanDiff = (barrierStats.avg - noBarrierStats.avg).toDouble
    val sse = barrierStats.sse + noBarrierStats.sse
    val df =  degreeOfFreedom(barrierStats.variance, barrierStats.samples, noBarrierStats.variance, noBarrierStats.samples)
    val mse = sse / df
    val harmonicN = barrierStats.samples * noBarrierStats.samples / barrierStats.samples + noBarrierStats.samples
    val S = Math.sqrt(mse * 2 / harmonicN)
    val criticalValue = new TDistribution(df).inverseCumulativeProbability(1 - 0.05 / 2)
    println(s"DF: $df, T: $criticalValue")

    val (meanLower, meanUpper) = (meanDiff - criticalValue * S, meanDiff + criticalValue * S)

//    chart.addSubtitle(new TextTitle(s"Significant: %s, improvement: %.2f %%, 95%% Confident interval: %.2f %% - %.2f %%"
//      .format(
//        significant,
//        meanDiff * 100 / barrierStats.avg,
//        meanLower * 100/ barrierStats.avg,
//        meanUpper * 100/ barrierStats.avg)
//    ))

    val output = new File(outputFile)
    val width = 600
    val height = 360
    Utils.saveChartAsPDF(output, chart, width, height)
  }

  def computeAverageStageData(data: Seq[(AppData, Seq[StageData], String)]):
      (Seq[(Int, Seq[StageData])], RuntimeStatistic) = {
    val stageBuffer = mutable.HashMap[Int, mutable.HashMap[Int, mutable.Buffer[StageData]]]()
    val stats = new DescriptiveStatistics
    data.foreach { case (appData, stages, _) =>
      stages.groupBy(_.jobId).toList.sortBy(_._1).foreach { case (jobId, jobStages) =>
        val stages = stageBuffer.getOrElseUpdate(jobId, mutable.HashMap[Int, mutable.Buffer[StageData]]())
        jobStages.filter(_.taskCount > 0).foreach { stage =>
          val tmp = new StageData
          tmp.completionTime = stage.completionTime - appData.start
          tmp.startTime = stage.startTime - appData.start
          stages.getOrElseUpdate(stage.stageId, mutable.Buffer[StageData]()) += tmp
        }
      }
      stats.addValue(appData.runtime)
    }

    val stagesData = stageBuffer.mapValues { stagesByIds =>
      stagesByIds.map { case (id, stages) =>
        val avgStart = stages.map(_.startTime).sum / stages.length
        val avgCompletion = stages.map(_.completionTime).sum / stages.length
        val ret = new StageData
        ret.stageId = id
        ret.startTime = avgStart
        ret.completionTime = avgCompletion
        ret
      }.toSeq
    }.toSeq.sortBy(_._1)

    (stagesData, RuntimeStatistic(stats))
  }

  def degreeOfFreedom(v1: Double, n1: Long, v2: Double, n2: Long): Double = {
    (((v1 / n1) + (v2 / n2)) * ((v1 / n1) + (v2 / n2))) /
      ((v1 * v1) / (n1 * n1 * (n1 - 1d)) + (v2 * v2) / (n2 * n2 * (n2 - 1d)))
  }
}