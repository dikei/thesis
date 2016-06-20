package pt.tecnico.spark

import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 6/16/16.
  */
object Shuffle {
  def main(args: Array[String]): Unit = {

    val input = args(0)
    val iteration = args(1).toInt
    val partitionCount = args(2).toInt
    val statsDir = if (args.length > 3) args(3) else "stats"

    val conf = new SparkConf()
    conf.setAppName(s"Shuffle-$iteration")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[(String, Long)]))

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val initialRDD = sc.textFile(input).zipWithUniqueId()

    var i = 0
    var currentRDD = initialRDD
    while (i < iteration) {
      currentRDD = currentRDD.repartition(partitionCount).mapValues { v =>
        val s = v % 1000007
        Math.pow(Math.cbrt(Math.pow(s, 3)), 3).toLong
      }
      i += 1
    }

    // Calculate line count
    val linesCount = currentRDD.count()
    println(s"Lines count: $linesCount")
  }
}
