package pt.tecnico.spark.milesage

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 6/22/16.
  */
object MilesageAppLoop {

  def main(args: Array[String]): Unit = {

    val passengersFile = args(0)
    val flightsFile = args(1)
    val outputFile = args(2)
    val numPartitions = args(3).toInt
    val statDir = if (args.length > 4) args(4) else "stats"

    val conf = new SparkConf()
    conf.setAppName(s"MilesageAppLoop")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(
        classOf[(Int, (Array[Int], Long))],
        classOf[(Int, Long)],
        classOf[(Int, Int)]))

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statDir))

    val partitioner = new HashPartitioner(numPartitions)
    val passengersRDD = sc.textFile(passengersFile, numPartitions)
      .map { line =>
        val lineSplit = line.split(' ')

        val passenger = lineSplit(0).toInt
        val flights = lineSplit(1).split('|').map(_.toInt)
        (passenger, (flights, 0L))
      }
      .filter(_._2._1.nonEmpty)
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val passengerCount = passengersRDD.count()

    val flightsRDD = sc.textFile(flightsFile, numPartitions).map { line =>
      val lineSplit = line.split(' ')

      val flightId = lineSplit(0).toInt
      val flightScores = lineSplit(1).toLong
      (flightId, flightScores)
    }.persist(StorageLevel.MEMORY_AND_DISK)

    val flightCount = flightsRDD.count()

    var resultRDD : RDD[(Int, Long)] = sc.emptyRDD[(Int, Long)]

    var activePassenger = passengersRDD
    var activeCount = passengerCount

    var iteration = 0
    while (activeCount > 0) {
      val prevActive = activePassenger
      val prevActiveCount = activeCount

      val updateScores = activePassenger
        .map { case (passengerId, (flights, score)) =>
          (flights(iteration), passengerId)
        }
        .join(flightsRDD)
        .map { case (_, ((passengerId), flightScore)) =>
          (passengerId, flightScore)
        }

      // Update score into current round active passenger
      val newScores = activePassenger
        .join(updateScores)
        .mapValues { case ((flights, oldScore), newScore) =>
          (flights, oldScore + newScore)
        }
        .persist(StorageLevel.MEMORY_AND_DISK)

      // Update active passenger for next round
      activePassenger = newScores
        .filter { case (passengerId, (flights, score)) =>
          iteration < flights.length
        }
        .persist(StorageLevel.MEMORY_AND_DISK)
      // Force materialization so we can throw away the previous version
      // Count the active vertex in the next round
      activeCount = activePassenger.count()
      prevActive.unpersist(false)

      if (prevActiveCount > activeCount) {
        val prevResult = resultRDD
        val update = newScores
          .filter { case (passengerId, (flights, score)) =>
            iteration == flights.length - 1
          }
          .mapValues { case (flights, score) => score }
        resultRDD = resultRDD.union(update).persist(StorageLevel.MEMORY_AND_DISK)
        // Force materialization so we can throw away the previous version
        val resultCount = resultRDD.count()
        println(s"Passenger complete: $resultCount")
        prevResult.unpersist(false)
      }

      newScores.unpersist(false)
      iteration += 1
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
