package pt.tecnico.postprocessing

import java.awt.Color
import java.io.File

import org.jfree.chart.axis.{NumberAxis, SubCategoryAxis}
import org.jfree.chart.plot.{CategoryPlot, DatasetRenderingOrder, PlotOrientation}
import org.jfree.chart.renderer.category._
import org.jfree.chart.{ChartFactory, ChartUtilities, JFreeChart, LegendItemCollection}
import org.jfree.data.KeyToGroupMap
import org.jfree.data.category.{CategoryDataset, DefaultCategoryDataset}
import pt.tecnico.spark.util.{AppData, StageData}

case class AggregatedStageData(
    stageId: Int,
    memoryInput: Long,
    hadoopInput: Long,
    networkInput: Long,
    diskInput: Long,
    totalTaskTime: Long)

/**
  * Created by dikei on 5/17/16.
  */
object ExecutionComparer {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.StageRuntimeAnalyzer barrierDir noBarrierDir outFile")
      System.exit(-1)
    }

    val barrierDir = args(0)
    val noBarrierDir = args(1)
    val outFile = args(2)

    val barrierData = Utils.parseJsonInput(barrierDir)
    val noBarrierData = Utils.parseJsonInput(noBarrierDir)

    compareExecution(barrierData, noBarrierData, outFile)
  }

  def compareExecution(
    barrierData: Array[(AppData, Seq[StageData], String)],
    noBarrierData: Array[(AppData, Seq[StageData], String)],
    outFile: String): Unit = {
    val totalTaskTimeDataset = new DefaultCategoryDataset
    val inputSizeDataset = new DefaultCategoryDataset
    val map = new KeyToGroupMap("W/Barrier")



    val averageBarrier = getStageDataAverages(barrierData)
    averageBarrier.foreach { stage =>
      totalTaskTimeDataset.addValue(stage.totalTaskTime, "Total task time with Barrier", stage.stageId)
      inputSizeDataset.addValue(stage.memoryInput, "Memory Input Size w/Barrier", stage.stageId)
      map.mapKeyToGroup("Memory Input Size w/Barrier", "W/Barrier")
      inputSizeDataset.addValue(stage.diskInput, "Disk Input Size w/Barrier", stage.stageId)
      map.mapKeyToGroup("Disk Input Size w/Barrier", "W/Barrier")
      inputSizeDataset.addValue(stage.networkInput, "Network Input Size w/Barrier", stage.stageId)
      map.mapKeyToGroup("Network Input Size w/Barrier", "W/Barrier")
      inputSizeDataset.addValue(stage.hadoopInput, "Hadoop Input Size w/Barrier", stage.stageId)
      map.mapKeyToGroup("Hadoop Input Size w/Barrier", "W/Barrier")
    }

    val averageNoBarrier = getStageDataAverages(noBarrierData)
    averageNoBarrier.foreach { stage =>
      totalTaskTimeDataset.addValue(stage.totalTaskTime, "Total task time no/Barrier", stage.stageId)
      inputSizeDataset.addValue(stage.memoryInput, "Memory Input Size no/Barrier", stage.stageId)
      map.mapKeyToGroup("Memory Input Size no/Barrier", "No/Barrier")
      inputSizeDataset.addValue(stage.diskInput, "Disk Input Size no/Barrier", stage.stageId)
      map.mapKeyToGroup("Disk Input Size no/Barrier", "No/Barrier")
      inputSizeDataset.addValue(stage.networkInput, "Network Input Size no/Barrier", stage.stageId)
      map.mapKeyToGroup("Network Input Size no/Barrier", "No/Barrier")
      inputSizeDataset.addValue(stage.hadoopInput, "Hadoop Input Size no/Barrier", stage.stageId)
      map.mapKeyToGroup("Hadoop Input Size no/Barrier", "No/Barrier")
    }

    val chart = ChartFactory.createLineChart(
      "Dual Axis Chart",
      "Category",
      "Total task time (s)",
      totalTaskTimeDataset,
      PlotOrientation.VERTICAL,
      true,
      true,
      false
    )
    val domainAxis = new SubCategoryAxis("Stages")
    domainAxis.setCategoryMargin(0.25)
    domainAxis.addSubCategory("W/Barrier")
    domainAxis.addSubCategory("No/Barrier")

    val plot = chart.getCategoryPlot
    plot.setDomainAxis(domainAxis)

    // 1st Axis
    val renderer1 = new LineAndShapeRenderer
    plot.setRenderer(0, renderer1)

    // 2nd Axis
    val renderer2 = new GroupedStackedBarRenderer
    renderer2.setSeriesToGroupMap(map)
    // Flat bar
    renderer2.setBarPainter(new StandardBarPainter)
    // Set color
    val colors = Array (
      new Color(77,175,74),
      new Color(55,126,184),
      new Color(152,78,163),
      new Color(228,26,28)
    )
    for (i <- 0 until inputSizeDataset.getColumnCount) {
      for (j <- 0 until inputSizeDataset.getRowCount / 2) {
        renderer2.setSeriesPaint(i * 4 + j, colors(j))
      }
    }
    plot.setDataset(1, inputSizeDataset)
    plot.mapDatasetToRangeAxis(1, 1)
    val inputSizeAxis = new NumberAxis("Input size (MB)")
    plot.setRangeAxis(1, inputSizeAxis)
    plot.setRenderer(1, renderer2)

    val legend = new LegendItemCollection

    // Reverse rendering order
//    plot.setDatasetRenderingOrder(DatasetRenderingOrder.REVERSE)

    val width = 2560
    val height = 960
    ChartUtilities.saveChartAsPNG(new File(outFile), chart, width, height)
  }

  def getStageDataAverages(data: Array[(AppData, Seq[StageData], String)]): Seq[AggregatedStageData] = {
    data.flatMap(_._2).groupBy(_.stageId).map { case (stageId, runs) =>
      var totalMemoryInput = 0L
      var totalHadoopInput = 0L
      var totalNetworkInput = 0L
      var totalDiskInput = 0L
      var averageTaskTime = 0L
      runs.foreach { run =>
        totalMemoryInput += run.memoryInput
        totalHadoopInput += run.hadoopInput
        totalNetworkInput += run.networkInput
        totalDiskInput += run.diskInput
        averageTaskTime += (run.totalTaskRuntime - run.partialOutputWaitTime)
      }
      new AggregatedStageData(stageId,
        totalMemoryInput / runs.length / 1000000,
        totalHadoopInput / runs.length / 1000000,
        totalNetworkInput / runs.length / 1000000,
        totalDiskInput / runs.length / 1000000,
        averageTaskTime / runs.length / 1000)
    }.toSeq.sortBy(_.stageId)
  }
}

