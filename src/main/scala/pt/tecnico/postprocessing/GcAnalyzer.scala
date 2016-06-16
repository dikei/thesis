package pt.tecnico.postprocessing

import java.io.{FileWriter, PrintWriter}

import pt.tecnico.spark.util.StageData

import scala.collection.mutable

/**
  * Created by dikei on 6/15/16.
  */
object GcAnalyzer {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.GcAnalyzer barrierDir noBarrierDir output")
      System.exit(-1)
    }

    val barrierDir = args(0)
    val noBarrierDir = args(1)
    val output = args(2)

    val barrierData = Utils.parseJsonInput(barrierDir, Some(stageFilter))
    val noBarrierData = Utils.parseJsonInput(noBarrierDir, Some(stageFilter))

    val gcCompare = mutable.HashMap[Int, (Long, Long)]()
    var barrierRuntime = 0L
    barrierData.foreach { case (appData, stages, _) =>
      stages.sortBy(_.startTime).foreach { stage =>
        val gcTime = stage.tasks.map(_.gcTime).sum
        barrierRuntime += stage.tasks.map { t =>
          t.duration - t.waitForPartialOutputTime
        }.sum
        val prevResult = gcCompare.getOrElse(stage.stageId, (0L, 0L))
        val barrierTime = prevResult._1
        val noBarrierTime = prevResult._2
        gcCompare += stage.stageId -> (barrierTime + gcTime, noBarrierTime)
      }
    }
    val avgBarrierRuntime = barrierRuntime / barrierData.length

    var noBarrierRuntime = 0L
    noBarrierData.foreach { case (appData, stages, _) =>
      stages.sortBy(_.startTime).foreach { stage =>
        val gcTime = stage.tasks.map(_.gcTime).sum
        noBarrierRuntime += stage.tasks.map { t =>
          t.duration - t.waitForPartialOutputTime
        }.sum
        val prevResult = gcCompare.getOrElse(stage.stageId, (0L, 0L))
        val barrierTime = prevResult._1
        val noBarrierTime = prevResult._2
        gcCompare += stage.stageId -> (barrierTime, noBarrierTime + gcTime)
      }
    }
    val avgNoBarrierRuntime = noBarrierRuntime / barrierData.length

    val printer = new PrintWriter(new FileWriter(output, true))
    printer.println(s"Comparing $barrierDir and $noBarrierDir")

    var totalGCBarrier = 0L
    var totalGCNoBarrier = 0L
    gcCompare.toArray.sortBy(_._1).foreach { case (stageId, (barrier, noBarrier)) =>
      val avgBarrierTime = barrier / barrierData.length
      val avgNoBarrierTime = noBarrier / noBarrierData.length
      totalGCBarrier += avgBarrierTime
      totalGCNoBarrier += avgNoBarrierTime
      printer.println(s"Stage $stageId, gcTime: $avgBarrierTime \t $avgNoBarrierTime")
    }
    printer.println(s"Total GC Time: $totalGCBarrier \t $totalGCNoBarrier")
    printer.println(s"GC Timer Percentage: " +
      s" ${totalGCBarrier.toDouble/avgBarrierRuntime * 100} \t ${totalGCNoBarrier.toDouble/avgNoBarrierRuntime * 100}")
    printer.println()
    printer.close()
  }

  def stageFilter(stage: StageData): Boolean = stage.jobId > 0
}
