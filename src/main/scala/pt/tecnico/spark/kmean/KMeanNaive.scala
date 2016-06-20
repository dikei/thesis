package pt.tecnico.spark.kmean

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 5/30/16.
  */
object KMeanNaive {
  def main(args: Array[String]): Unit = {

    val input = args(0)
    val numClusters = args(1).toInt
    val numIterations = args(2).toInt
    val statsDir = if (args.length > 3) args(3) else "stats"

    val conf = new SparkConf
    conf.setAppName("KMeanNaive")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Array[Double]], classOf[Array[Array[Double]]]))

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    // Load and parse the data
    val data = sc.textFile(input)
    val points = data.map(s => s.split(' ').map(_.toDouble)).cache()

    // Initialize the center
    var centers = points.takeSample(withReplacement = true, num = numClusters)
    var bcCurrentCenters = sc.broadcast(centers)
    var activeClusters = computeClusters(points, bcCurrentCenters)
    activeClusters.cache()
    bcCurrentCenters.unpersist(false)

    val initialWcss = computeWCSS(activeClusters, centers, sc)

    var i = 0
    while (i < numIterations) {
      // Calculate new center
      val centersRDD = activeClusters
        .reduceByKey { case ((p1, c1), (p2, c2)) =>
          (sumPoint(p1, p2), c1 + c2)
        }
        .mapValues { case (p, c) =>
          p.map(_ / c)
        }
        .values
      // Collect then rebroadcast the center
      centers = centersRDD.collect()
      // Uncache the active cluster
      activeClusters.unpersist(false)
      bcCurrentCenters = sc.broadcast(centers)
      // Calculate the new center
      activeClusters = computeClusters(points, bcCurrentCenters)
      // Delete the broadcast to save memory
      bcCurrentCenters.unpersist(false)
      i += 1
    }

    val finalWcss = computeWCSS(activeClusters, centers, sc)

    println(s"Initial WCSS: $initialWcss")
    println(s"Final WCSS: $finalWcss")
  }

  def computeWCSS(
      clusters: RDD[(Int, (Array[Double], Int))],
      centers: Array[Array[Double]],
      sc: SparkContext): Double = {
    val bcCenters = sc.broadcast(centers)
    val accumulator = sc.accumulator[Double](0)
    clusters.mapValues(_._1).foreach { case (index, point) =>
      val center = bcCenters.value(index)
      accumulator += squareDistance(center, point)
    }
    accumulator.value
  }

  def computeClusters(points: RDD[Array[Double]],
                      bcCenters: Broadcast[Array[Array[Double]]]): RDD[(Int, (Array[Double], Int))] = {
    points.mapPartitions ({ points =>
      points.map { p =>
        (findClosest(p, bcCenters), (p, 1))
      }
    }, preservesPartitioning = true)
  }

  def findClosest(v1: Array[Double], v2: Broadcast[Array[Array[Double]]]): Int = {
    val centers = v2.value
    var minDistance = Double.PositiveInfinity
    var minIndex = -1
    centers.zipWithIndex.foreach { case (center, index) =>
      val sd = squareDistance(v1, center)
      if (sd < minDistance) {
        minDistance = sd
        minIndex = index
      }
    }
    minIndex
  }

  def sumPoint(v1: Array[Double], v2: Array[Double]): Array[Double] = {
    assert(v1.length == v2.length)
    val length = v1.length
    val out = Array.fill[Double](length)(0)
    for (i <- 0 until length) {
      out(i) = v1(i) + v2(i)
    }
    out
  }

  def squareDistance(v1: Array[Double], v2: Array[Double]): Double = {
    assert(v1.length == v2.length)
    var total: Double = 0
    val length = v1.length
    for (i <- 0 until length) {
      total += Math.pow(v1(i) - v2(i), 2)
    }
    total
  }
}
