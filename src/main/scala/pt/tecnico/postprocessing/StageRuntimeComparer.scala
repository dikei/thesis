package pt.tecnico.postprocessing

import java.awt.{Color, Font}
import java.io.File
import java.util.Date

import org.jfree.chart.{ChartUtilities, JFreeChart}
import org.jfree.chart.axis.{CategoryAxis, DateTickUnit, DateTickUnitType}
import org.jfree.chart.plot.{CategoryPlot, IntervalMarker, PlotOrientation}
import org.jfree.chart.renderer.category.StandardBarPainter
import org.jfree.chart.title.TextTitle
import org.jfree.data.gantt.{Task, TaskSeries, TaskSeriesCollection}
import org.jfree.data.time.SimpleTimePeriod
import org.jfree.ui.{RectangleAnchor, RectangleInsets}
import pt.tecnico.spark.util.{AppData, StageData}

import scala.collection.{Map, mutable}

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
      barrierData: (Seq[(Int, Seq[StageData])], Long, Long),
      noBarrierData: (Seq[(Int, Seq[StageData])], Long, Long),
      outputFile: String): Unit = {


    val collection = new TaskSeriesCollection()
    val markers = mutable.Buffer[IntervalMarker]()
    val barrierSeries = new TaskSeries("Barrier")
    val noBarrierSeries = new TaskSeries("NoBarrier")

    var index = 0
    val jobFont = new Font("Dialog", Font.PLAIN, 40)
    val jobBackgrounds = Array (
      new Color(247,247,247),
      new Color(99,99,99)
    )


    val barrierStagesData = barrierData._1
    barrierStagesData.foreach { case (jobId, jobStages) =>
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
      val jobMarker = new IntervalMarker(startTime, endTime)
      jobMarker.setLabel(jobId.toString)
      jobMarker.setAlpha(0.2f)
      jobMarker.setLabelFont(jobFont)
      jobMarker.setLabelAnchor(RectangleAnchor.BOTTOM)
      jobMarker.setLabelOffset(new RectangleInsets(20, 0, 0, 0))
      jobMarker.setPaint(jobBackgrounds(index % jobBackgrounds.length))
      index += 1
      markers += jobMarker
    }

    val noBarrierStageData = noBarrierData._1
    noBarrierStageData.foreach { case (jobId, jobStages) =>
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
    }

    collection.add(barrierSeries)
    collection.add(noBarrierSeries)
    val dateFormat = new RelativeSecondFormat(0)
    val timeAxis = new CustomDateAxis("Time")
    timeAxis.setLowerBound(0)
    timeAxis.setDateFormatOverride(dateFormat)
    timeAxis.setAutoRange(true)

    timeAxis.setRange(new Date(0), new Date(Math.max(barrierData._2, noBarrierData._2)))
    timeAxis.setTickUnit(new DateTickUnit(DateTickUnitType.SECOND, 10))
    timeAxis.setMinorTickMarksVisible(true)
    timeAxis.setMinorTickCount(2)
    timeAxis.setLowerMargin(0.02)
    timeAxis.setUpperMargin(0.02)

    val categoryAxis = new CategoryAxis("Stages")

    val renderer = new StageGanttRenderer(collection)
    renderer.setBarPainter(new StandardBarPainter)
    renderer.setDrawBarOutline(false)
    renderer.setShadowVisible(false)
    val plot = new CategoryPlot(collection, categoryAxis, timeAxis, renderer)
    plot.setOrientation(PlotOrientation.HORIZONTAL)
    plot.getDomainAxis.setCategoryMargin(0.3)
    markers.foreach { marker =>
      plot.addRangeMarker(marker)
    }
    val chart = new JFreeChart("Stage gantt", JFreeChart.DEFAULT_TITLE_FONT, plot, true)
    val barrierDuration = barrierData._2
    val barrierStdDev = barrierData._3
    val noBarrierDuration = noBarrierData._2
    val noBarrierStdDev = noBarrierData._3
    val improvement = (barrierDuration - noBarrierDuration).toDouble * 100/ barrierDuration
    chart.addSubtitle(new TextTitle(s"Barrier: Avgtime: $barrierDuration, StdDev: $barrierStdDev "))
    chart.addSubtitle(new TextTitle(s"No barrier: Avgtime: $noBarrierDuration, StdDev: $noBarrierStdDev "))
    chart.addSubtitle(new TextTitle(s"Improvement: $improvement %"))
    val output = new File(outputFile)
    val width = 1280 * 2
    val height = 960
    ChartUtilities.saveChartAsPNG(output, chart, width, height)
  }

  def computeAverageStageData(data: Seq[(AppData, Seq[StageData], String)]): (Seq[(Int, Seq[StageData])], Long, Long) = {
    val stageBuffer = mutable.HashMap[Int, mutable.HashMap[Int, mutable.Buffer[StageData]]]()
    val durations = mutable.Buffer[Long]()
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
      durations += appData.runtime
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
    val averageDuration = durations.sum / durations.length
    var variance = 0.0
    durations.foreach { d =>
      variance += Math.pow(d.toDouble - averageDuration, 2)
    }
    val stdDev = Math.sqrt(variance).toLong
    (stagesData, averageDuration, stdDev)
  }
}
