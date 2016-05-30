package pt.tecnico.spark.kmean

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.KMeansDataGenerator
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 5/30/16.
  */
object KMeanGen {
  def main(args: Array[String]): Unit = {

    val output = args(0)
    val numPoint = args(1).toInt
    val numCluster = args(2).toInt
    val numDimension = args(3).toInt
    val scaleFactor = args(4).toDouble
    val numPartitions = args(5).toInt
    val statsDirs = if (args.length > 6) args(6) else "stats"

    val conf = new SparkConf
    conf.setAppName("KMeanGen")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDirs))

    KMeansDataGenerator.generateKMeansRDD(sc, numPoint, numCluster, numDimension, scaleFactor, numPartitions)
      .map(_.mkString(" "))
      .saveAsTextFile(output)
  }
}
