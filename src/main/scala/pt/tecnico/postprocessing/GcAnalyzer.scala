package pt.tecnico.postprocessing

import java.io.{File, FileWriter, PrintWriter}

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

    val barrierData = Utils.parseJsonInput(barrierDir, 0)
    val noBarrierData = Utils.parseJsonInput(noBarrierDir, 0)

    val gcCompare = mutable.HashMap[Int, (Long, Long)]()
    var barrierRuntime = 0L
    barrierData.foreach { case (appData, stages, _) =>

      stages.sortBy(_.startTime).foreach { stage =>
        val gcTime = stage.tasks.map(_.gcTime).sum
        barrierRuntime += stage.tasks.map { t =>
          t.duration - t.waitForPartialOutputTime
        }.sum
        gcCompare += stage.stageId -> (gcTime, 0L)
      }
    }

    val printer = new PrintWriter(new FileWriter(output, true))

    var noBarrierRuntime = 0L
    noBarrierData.foreach { case (appData, stages, _) =>
      stages.sortBy(_.startTime).foreach { stage =>
        val gcTime = stage.tasks.map(_.gcTime).sum
        noBarrierRuntime += stage.tasks.map { t =>
          t.duration - t.waitForPartialOutputTime
        }.sum
        val barrierTime = gcCompare.getOrElse(stage.stageId, (0L, 0L))._1
        gcCompare += stage.stageId -> (barrierTime, gcTime)
      }
    }

    printer.println(s"Comparing $barrierDir and $noBarrierDir")
    var totalGCBarrier = 0L
    var totalGCNoBarrier = 0L
    gcCompare.toArray.sortBy(_._1).foreach { case (stageId, (barrier, noBarrier)) =>
      totalGCBarrier += barrier
      totalGCNoBarrier += noBarrier
      printer.println(s"Stage $stageId, gcTime: $barrier \t $noBarrier")
    }
    printer.println(s"Total GC Time: $totalGCBarrier \t $totalGCNoBarrier")
    printer.println(s"GC Timer Percentage: ${totalGCBarrier.toDouble/barrierRuntime * 100} \t ${totalGCNoBarrier.toDouble/noBarrierRuntime * 100}")
    printer.println()
    printer.close()
  }
}
