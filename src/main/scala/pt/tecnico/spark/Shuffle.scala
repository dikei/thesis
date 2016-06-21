package pt.tecnico.spark

import java.util.Collections

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

    val initialRDD = sc.textFile(input).repartition(partitionCount)

    // Force-load the file into memory to avoid the variant in reading speed from HDFS
    val initialCount = initialRDD.count()

    var i = 0
    var currentRDD = initialRDD
    while (i < iteration) {
      currentRDD = currentRDD
        .map { line =>
          val charsArray = java.util.Arrays.asList(line.toCharArray:_*)
          Collections.shuffle(charsArray)
          charsArray.asScala.mkString("")
        }
        .repartition(partitionCount)
      i += 1
    }

    // Calculate line count
    val linesCount = currentRDD.count()

    println(s"Initial lines count: $initialCount")
    println(s"Final lines count: $linesCount")
  }
}
