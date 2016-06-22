package pt.tecnico.spark.milesage

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by dikei on 6/22/16.
  */
object MilesageAppLoop {

  def main(args: Array[String]): Unit = {

    val passengersFile = args(0)
    val flightsFile = args(1)
    val outputFile = args(2)
    val numPartitions = args(3).toInt

    val conf = new SparkConf()
    conf.setAppName(s"MilesageAppLoop")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(
        classOf[(Int, Array[Int], Long)],
        classOf[(Int, (Int, Array[Int], Long))],
        classOf[(Int, Long)]))

    val sc = new SparkContext(conf)

    val passengersRDD = sc.textFile(passengersFile, numPartitions).map { line =>
      val lineSplit = line.split(' ')

      val passenger = lineSplit(0).toInt
      val flights = lineSplit(1).split('|').map(_.toInt)
      (passenger, flights, 0L)
    }.cache()

    val passengerCount = passengersRDD.count()

    val flightsRDD = sc.textFile(flightsFile, numPartitions).map { line =>
      val lineSplit = line.split(' ')

      val flightId = lineSplit(0).toInt
      val flightScores = lineSplit(1).toLong
      (flightId, flightScores)
    }.cache()

    val flightCount = flightsRDD.count()

    var activePassenger = passengersRDD.filter(_._2.nonEmpty).cache()
    var resultRDD : RDD[(Int, Long)] = sc.emptyRDD[(Int, Long)].cache()
    // Force materialization of active passenger and current result,
    // so we can throw away the passengersRDD
    var activeCount = activePassenger.count()
    passengersRDD.unpersist(false)

    while (activeCount > 0) {
      val prevActive = activePassenger
      val prevActiveCount = activeCount

      val tmp = activePassenger
        .map { case (passengerId, flights, score) =>
          val leftFlights = flights.splitAt(1)._2
          (flights(0), (passengerId, leftFlights, score))
        }
        .join(flightsRDD)
        .mapValues { case ((passengerId, leftFlights, oldScore), flightScore) =>
          (passengerId, leftFlights, oldScore + flightScore)
        }
        .values
        .cache()

      activePassenger = tmp.filter(_._2.nonEmpty).cache()
      // Force materialization so we can throw away the previous version
      activeCount = activePassenger.count()
      prevActive.unpersist(false)

      if (prevActiveCount > activeCount) {
        val prevResult = resultRDD
        val update = tmp.filter(_._2.isEmpty).map { case (passenger, flights, score) =>
          (passenger, score)
        }
        resultRDD = resultRDD.union(update).cache()
        // Force materialization so we can throw away the previous version
        val resultCount = resultRDD.count()
        println(s"Passenger complete: $resultCount")
        prevResult.unpersist(false)
      }

      tmp.unpersist(false)
    }

    flightsRDD.unpersist(false)

    println(s"Passengers: $passengerCount")
    println(s"Flights: $flightCount")

    if (outputFile.nonEmpty) {
      resultRDD.repartitionAndSortWithinPartitions(new HashPartitioner(numPartitions)).saveAsTextFile(outputFile)
    } else {
      val total = resultRDD.values.fold(0L)(_ + _)
      println(s"Total miles: $total")
    }
  }

}
