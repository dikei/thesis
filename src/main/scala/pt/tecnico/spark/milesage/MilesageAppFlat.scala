package pt.tecnico.spark.milesage

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by dikei on 6/22/16.
  */
object MilesageAppFlat {

  def main(args: Array[String]): Unit = {
    val passengersFile = args(0)
    val flightsFile = args(1)
    val outputFile = args(2)
    val numPartitions = args(3).toInt

    val conf = new SparkConf()
    conf.setAppName(s"MilesageAppFlat")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(
        classOf[(Int, Array[Int])],
        classOf[(Int, Long)]))

    val sc = new SparkContext(conf)

    val passengersRDD = sc.textFile(passengersFile, numPartitions).map { line =>
      val lineSplit = line.split(' ')

      val passenger = lineSplit(0).toInt
      val flights = lineSplit(1).split('|').map(_.toInt)
      (passenger, flights)
    }.cache()

    val passengerCount = passengersRDD.count()

    val flightsRDD = sc.textFile(flightsFile, numPartitions).map { line =>
      val lineSplit = line.split(' ')

      val flightId = lineSplit(0).toInt
      val flightScores = lineSplit(1).toLong
      (flightId, flightScores)
    }.cache()

    val flightCount = flightsRDD.count()

    val resultRDD = passengersRDD
      .flatMap { case (passenger, flights) =>
        flights.map { flightId =>
          (flightId, passenger)
        }
      }
      .join(flightsRDD)
      .values
      .reduceByKey(_ + _)

    println(s"Passengers: $passengerCount")
    println(s"Flights: $flightCount")

    if (outputFile.nonEmpty) {
      resultRDD.repartitionAndSortWithinPartitions(new HashPartitioner(numPartitions)).saveAsTextFile(outputFile)
    } else {
      val total = resultRDD.values.fold(0)(_ + _)
      println(s"Total miles: $total")
    }
  }
}
