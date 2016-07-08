package pt.tecnico.spark

import java.util.Collections

import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
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
      .registerKryoClasses(Array(classOf[String]))

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val initialRDD = sc.textFile(input).repartition(partitionCount).cache()

    // Force-load the file into memory to avoid the variant in reading speed from HDFS
    val initialCount = initialRDD.count()

    // Warm up JIT
//    doShuffle(iteration, initialRDD, partitionCount)

    // Second shuffle to interference
    val linesCount: Long = doShuffle(iteration, initialRDD, partitionCount)

    println(s"Initial lines count: $initialCount")
    println(s"Final lines count: $linesCount")
  }

  def doShuffle(iter: Int, initialRDD: RDD[String], partitionCount: Int): Long = {
    var i = 0
    var currentRDD = initialRDD
    while (i < iter) {
      currentRDD = currentRDD
        .map { line =>
          val charsArray = java.util.Arrays.asList(line.toCharArray: _*)
          Collections.shuffle(charsArray)
          charsArray.asScala.mkString("")
        }
        .repartition(partitionCount)
      i += 1
    }

    // Calculate line count
    val linesCount = currentRDD.count()
    linesCount
  }
}
