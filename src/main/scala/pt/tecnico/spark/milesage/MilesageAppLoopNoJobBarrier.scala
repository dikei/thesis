package pt.tecnico.spark.milesage

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 6/22/16.
  */
object MilesageAppLoopNoJobBarrier {

  def main(args: Array[String]): Unit = {

    val passengersFile = args(0)
    val flightsFile = args(1)
    val outputFile = args(2)
    val numPartitions = args(3).toInt
    val statDir = if (args.length > 4) args(4) else "stats"

    val conf = new SparkConf()
    conf.setAppName(s"MilesageAppLoopNoJobBarrier")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(
        classOf[(Int, (Array[Int], Long, Int))],
        classOf[(Int, Long)],
        classOf[(Int, Int)]))

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statDir))

    val passengersRDD = sc.textFile(passengersFile, numPartitions)
      .map { line =>
        val lineSplit = line.split(' ')

        val passenger = lineSplit(0).toInt
        val flights = lineSplit(1).split('|').map(_.toInt)
        (passenger, (flights, 0L, 0))
      }
      .repartition(numPartitions)
      .cache()

    val passengerCount = passengersRDD.count()

    val flightsRDD = sc.textFile(flightsFile, numPartitions).map { line =>
      val lineSplit = line.split(' ')

      val flightId = lineSplit(0).toInt
      val flightScores = lineSplit(1).toLong
      (flightId, flightScores)
    }.repartition(numPartitions).cache()

    val flightCount = flightsRDD.count()

    var resultRDD : RDD[(Int, Long)] = sc.emptyRDD[(Int, Long)]

    var activePassenger = passengersRDD

    val maxIteration = passengersRDD
      .map { case (passengerId, (flights, _, _)) => flights.length }
      .max()

    println(s"Max iteration $maxIteration")
    var iteration = 0
    while (iteration < maxIteration) {
      val updateScores = activePassenger
        .map { case (passengerId, (flights, score, iter)) =>
          (flights(iter), passengerId)
        }
        .join(flightsRDD)
        .map { case (_, (passengerId, flightScore)) =>
          (passengerId, flightScore)
        }

      // Update score into current round active passenger
      val newScores = activePassenger
        .join(updateScores)
        .mapValues { case ((flights, oldScore, iter), newScore) =>
          (flights, oldScore + newScore, iter + 1)
        }
        .cache()

      // Update active passenger for next round
      activePassenger = newScores
        .filter { case (passengerId, (flights, score, iter)) =>
          iter < flights.length
        }
        .cache()

      val update = newScores
        .filter { case (passengerId, (flights, score, iter)) =>
          iter >= flights.length
        }
        .mapValues { case (flights, score, iter) => score }
      resultRDD = resultRDD.union(update).cache()

      iteration += 1
    }

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
