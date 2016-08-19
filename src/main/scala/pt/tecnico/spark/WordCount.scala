package pt.tecnico.spark

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Word count program to test Spark
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val outputFile = args(1)
    val statisticDir = if (args.length > 2) args(2) else "stats"

    val reportListener = new StageRuntimeReportListener(statisticDir)
    sc.addSparkListener(reportListener)

    val out = sc.textFile(inputFile)
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    if (outputFile.isEmpty) {
      out.count()
    } else {
      out.saveAsTextFile(outputFile)
    }

  }
}
