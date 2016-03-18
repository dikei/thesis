package pt.tecnico.postprocessing

import java.io.{FileWriter, FileReader, File}

import org.supercsv.cellprocessor.constraint.NotNull
import org.supercsv.cellprocessor.{ParseLong, ParseDate, ParseInt}
import org.supercsv.io.{CsvBeanWriter, CsvBeanReader}
import org.supercsv.prefs.CsvPreference
import pt.tecnico.spark.util.TaskRuntimeStatistic

import scala.collection.mutable

/**
  * Created by dikei on 3/15/16.
  */
object StageRuntimeAnalyzer {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("java pt.tecnico.postprocessing.StageRuntimeAnalyzer statsDir outFile")
      System.exit(-1)
    }

    val statsDir = args(0)
    val outFile = args(1)
    val files = new File(statsDir).listFiles()

    val processors = Array (
      new ParseInt(), // Stage Id
      new NotNull(), // Stage Name
      new ParseInt(), // Task count
      new ParseLong(), // Total task runtime
      new ParseLong(), // Stage runtime
      new ParseLong(), // Fetch wait time
      new ParseLong(), // Shuffle write time
      new ParseLong(), // Average task time
      new ParseLong(), // Fastest task
      new ParseLong(), // Slowest task
      new ParseLong(), // Standard deviation
      new ParseLong(), // 5 percentile
      new ParseLong(), // 25 percentile
      new ParseLong(), // Median
      new ParseLong(), // 75 percentile
      new ParseLong() // 95 percentile
    )

    var headers: Array[String] = null
    val average : Array[TaskRuntimeStatistic] = files.flatMap { f =>
      val reader = new CsvBeanReader(new FileReader(f), CsvPreference.STANDARD_PREFERENCE)
      try {
        headers = reader.getHeader(true)
        Iterator.continually[TaskRuntimeStatistic](reader.read(classOf[TaskRuntimeStatistic], headers, processors:_*))
          .takeWhile(_ != null).toArray[TaskRuntimeStatistic]
      } finally {
        reader.close()
      }
    }
    .groupBy(_.getStageId)
    .map { case (stageId, stages) =>
      val count = stages.length
      val total = stages.reduce[TaskRuntimeStatistic] { case (s1: TaskRuntimeStatistic, s2: TaskRuntimeStatistic) =>
        new TaskRuntimeStatistic(
          stageId,
          s1.getAverage + s2.getAverage,
          s1.getFastest + s2.getFastest,
          s1.getSlowest + s2.getSlowest,
          s1.getStandardDeviation + s2.getStandardDeviation,
          s1.getName,
          s1.getTaskCount + s2.getTaskCount,
          s1.getPercent5 + s2.getPercent5,
          s1.getPercent25 + s2.getPercent25,
          s1.getMedian + s2.getMedian,
          s1.getPercent75 + s2.getPercent75,
          s1.getPercent95 + s2.getPercent95,
          s1.getTotalTaskRuntime + s2.getTotalTaskRuntime,
          s1.getStageRuntime + s2.getStageRuntime,
          s1.getFetchWaitTime + s2.getFetchWaitTime,
          s1.getShuffleWriteTime + s2.getShuffleWriteTime)
      }
      new TaskRuntimeStatistic(
        stageId,
        total.getAverage / count,
        total.getFastest / count,
        total.getSlowest / count,
        total.getStandardDeviation / count,
        total.getName,
        total.getTaskCount / count,
        total.getPercent5 / count,
        total.getPercent25 / count,
        total.getMedian / count,
        total.getPercent75 / count,
        total.getPercent95 / count,
        total.getTotalTaskRuntime / count,
        total.getStageRuntime / count,
        total.getFetchWaitTime / count,
        total.getShuffleWriteTime / count)
    }.toArray.sortBy(_.getStageId)

    val writer = new CsvBeanWriter(new FileWriter(outFile), CsvPreference.STANDARD_PREFERENCE)
    try {
      writer.writeHeader(headers:_*)
      average.foreach { stage =>
        writer.write(stage, headers:_*)
      }
    } finally {
      writer.close()
    }

  }
}
