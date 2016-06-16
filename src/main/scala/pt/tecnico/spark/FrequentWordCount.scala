package pt.tecnico.spark

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Word count program to test Spark
  */
object FrequentWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FreqWordCount")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val appearance = args(1).toInt
    val partitions = args(2).toInt
    val statisticDir = if (args.length > 3) args(3) else "stats"

    // Do the word count and save output
    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => a + b
    val mergeCombiners = (a: Int, b: Int) => {
      a + b
    }

    val reportListener = new StageRuntimeReportListener(statisticDir)
    sc.addSparkListener(reportListener)
    val startTime = System.currentTimeMillis()
    val out = sc.textFile(inputFile)
      .coalesce(partitions)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))

    out.combineByKeyWithInterval(
        createCombiner,
        mergeValue,
        mergeCombiners,
        Partitioner.defaultPartitioner(out)
      )
      .foreachPartition { case resultIterator =>
        val printed = new scala.collection.mutable.HashSet[String]()
        var counter = 0
        def elapsedTime = System.currentTimeMillis() - startTime
        val writeStart = elapsedTime
        while(resultIterator.hasNext && printed.size < 10) {
          counter += 1
          val (word, count) = resultIterator.next()
          if (!printed.contains(word) && count > appearance) {
            printed += word
          }
        }
        val writeEnd = elapsedTime
        println(s"Record processed: $counter, Write start: $writeStart, Write end: $writeEnd")
      }
  }
}
