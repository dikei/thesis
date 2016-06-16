package pt.tecnico.spark

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dikei on 6/16/16.
  */
object Shuffle {
  def main(args: Array[String]): Unit = {

    val input = args(0)
    val iteration = args(1).toInt
    val partitionCount = args(2).toInt

    val conf = new SparkConf()
    conf.setAppName(s"Shuffle-$iteration")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val initialRDD = sc.textFile(input).zipWithUniqueId()

    var i = 0
    var currentRDD = initialRDD
    while (i < iteration) {
      currentRDD = currentRDD.repartition(partitionCount).mapValues(_ * 2)
      i += 1
    }

    // Calculate line count
    val linesCount = currentRDD.count()
    println(s"Lines count: $linesCount")
  }
}
