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

    val barrierData = Utils.parseJsonInput(barrierDir, stageFilter = Some(stageFilter))
    val noBarrierData = Utils.parseJsonInput(noBarrierDir, stageFilter = Some(stageFilter))

    val barrierAverage = computeAverageStageData(Utils.trimRuns(barrierData, 10))
    val noBarrierAverage = computeAverageStageData(Utils.trimRuns(noBarrierData, 10))

    println("Plotting average stage gantt chart")
    plotStageGanttChartAverage(barrierAverage, noBarrierAverage, outFile)
  }

  def stageFilter(stage: StageData): Boolean = true // stage.jobId > 0 // stage.stageId >= 7 && stage.stageId <= 9

  def plotStageGanttChartAverage(
      barrierData: Seq[(Int, Seq[StageData])],
      noBarrierData: Seq[(Int, Seq[StageData])],
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

    var barrierEnd = -1L
    barrierData.foreach { case (jobId, jobStages) =>
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
        if (barrierEnd < stage.completionTime) {
          barrierEnd = stage.completionTime
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

    index = 0
    var noBarrierEnd = -1L
    noBarrierData.foreach { case (jobId, jobStages) =>
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
        if (noBarrierEnd < stage.completionTime) {
          noBarrierEnd = stage.completionTime
        }
        val task = new Task(stage.stageId.toString, new SimpleTimePeriod(stage.startTime, stage.completionTime))
        noBarrierSeries.add(task)
      }
//      val jobMarker = new IntervalMarker(startTime, endTime)
//      jobMarker.setLabel(jobId.toString)
//      jobMarker.setAlpha(0.2f)
//      jobMarker.setLabelFont(jobFont)
//      jobMarker.setLabelAnchor(RectangleAnchor.TOP)
//      jobMarker.setLabelOffset(new RectangleInsets(20, 0, 0, 0))
//      jobMarker.setPaint(jobBackgrounds(index % jobBackgrounds.length))
//      index += 1
//      markers += jobMarker
    }

    collection.add(barrierSeries)
    collection.add(noBarrierSeries)
    val dateFormat = new RelativeSecondFormat(0)
    val timeAxis = new CustomDateAxis("Time")
    timeAxis.setLowerBound(0)
    timeAxis.setDateFormatOverride(dateFormat)
    timeAxis.setAutoRange(true)

    timeAxis.setRange(new Date(0), new Date(Math.max(barrierEnd, noBarrierEnd)))
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
    chart.addSubtitle(new TextTitle(s"Barrier time: ${barrierEnd / 1000} "))
    chart.addSubtitle(new TextTitle(s"No barrier time: ${noBarrierEnd / 1000}"))
    val output = new File(outputFile)
    val width = 1280 * 2
    val height = 960
    ChartUtilities.saveChartAsPNG(output, chart, width, height)
  }

  def computeAverageStageData(data: Seq[(AppData, Seq[StageData], String)]): Seq[(Int, Seq[StageData])] = {
    val stageBuffer = mutable.HashMap[Int, mutable.HashMap[Int, mutable.Buffer[StageData]]]()
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
    }
    stageBuffer.mapValues { stagesByIds =>
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
  }
}
