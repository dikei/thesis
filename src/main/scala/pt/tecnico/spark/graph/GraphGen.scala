package pt.tecnico.spark.graph

import org.apache.spark.graphx.{GraphXUtils, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.storage.StorageLevel

/**
  * Created by dikei on 2/12/16.
  */
object GraphGen {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.graph.GraphGen [output] [#vertices] [#randomseed] [#partition]")
      System.exit(0)
    }

    val output = args(0)
    val noVertices = args(1).toInt
    val randomSeed = if (args.length > 2) args(2).toInt else 13290
    val noPartition = if (args.length > 3) args(3).toInt else 0
    val conf = new SparkConf().setAppName("GraphGen")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    GraphXUtils.registerKryoClasses(conf)

    val sc = new SparkContext(conf)

    val graph = GraphGenerators
      .logNormalGraph(sc, noVertices, seed = randomSeed, numEParts = noPartition)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    graph.edges.map { edge =>
      edge.srcId + " " + edge.dstId
    }.saveAsTextFile(output)
  }

}
