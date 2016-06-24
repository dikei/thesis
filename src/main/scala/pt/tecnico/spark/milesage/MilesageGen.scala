package pt.tecnico.spark.milesage

import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 6/21/16.
  */
object MilesageGen {

  val MAX_SCORE = 1000

  def main(args: Array[String]): Unit = {

    val passengerCount = args(0).toInt
    val flightsCount = args(1).toInt
    val flightsPerPassengerMin = args(2).toInt
    val flightsPerPassengerMax = args(3).toInt
    val partitions = args(4).toInt
    val passengerOut = args(5)
    val flightsOut = args(6)

    val conf = new SparkConf()
    conf.setAppName(s"MilesageGen-$passengerCount-$flightsCount")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val rangeLength = flightsPerPassengerMax - flightsPerPassengerMin

    // Generate passenger data
    println("Generating passenger data")
    sc.parallelize(1 to passengerCount, partitions).map { passengerId =>
      val flightsPerPassenger = flightsPerPassengerMin + scala.util.Random.nextInt(rangeLength + 1)
      val flights = Array.fill(flightsPerPassenger)(0)
      for (i <- 0 until flightsPerPassenger) {
        flights(i) = scala.util.Random.nextInt(flightsCount)
      }
      s"$passengerId ${flights.mkString("|")}"
    }.saveAsTextFile(passengerOut)

    // Generate flight data
    println("Generating flight data")
    sc.parallelize(0 until flightsCount, partitions).map { flightId =>
      val score = 1 + scala.util.Random.nextInt(MAX_SCORE)
      s"$flightId $score"
    }.saveAsTextFile(flightsOut)
  }

}
