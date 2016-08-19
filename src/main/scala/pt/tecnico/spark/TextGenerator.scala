package pt.tecnico.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.util.Random

/**
  * Created by dikei on 7/21/16.
  */
object TextGenerator {

  def main(args: Array[String]): Unit = {

    val bytesPerPartition = args(0).toInt
    val numPartitions = args(1).toInt
    val output = args(2)
    var seed = args(3).toLong

    val conf = new SparkConf().setAppName("TextGenerator")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val corpus = Source.fromFile("/usr/share/dict/words").getLines().toArray
    val wordsBc = sc.broadcast(corpus)

    if (seed == -1) {
      seed = System.currentTimeMillis()
    }

    sc.parallelize(0 until numPartitions, numPartitions).mapPartitionsWithIndex { (index, _) =>
      val words = wordsBc.value
      // Use same seed for each partition
      new PartitionGenerator(bytesPerPartition, seed, words)
    }.saveAsTextFile(output)
  }

  class PartitionGenerator(maxBytes: Long, seed: Long, corpus: Array[String]) extends Iterator[String] {
    val corpusLength = corpus.length
    val maxBytesPerLine = 80
    var randomGenerator = new Random(seed)

    var outputBytes = 0L

    override def hasNext: Boolean = outputBytes < maxBytes

    override def next(): String = {
      val stringBuilder = new StringBuilder(maxBytesPerLine * 2)
      var lineOutputBytes = 0L
      while (lineOutputBytes <= maxBytesPerLine) {
        val nextWord = corpus(randomGenerator.nextInt(corpusLength))
        stringBuilder.append(nextWord).append(" ")
        lineOutputBytes += nextWord.length + 1
        outputBytes += nextWord.length + 1
        stringBuilder
      }
      stringBuilder.toString()
    }
  }
}
