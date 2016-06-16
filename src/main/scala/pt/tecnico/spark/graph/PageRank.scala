package pt.tecnico.spark.graph

import org.apache.spark.graphx.{GraphLoader, GraphXUtils, PartitionStrategy}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 2/12/16.
  */
object PageRank {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.graph.PageRankSimple [jar] [input] [output] [#iteration] [statsDir]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val iteration = if (args.length > 2) args(2).toInt else 10
    val numPartitions = if (args.length > 3) args(3).toInt else -1
    val statisticDir = if (args.length > 4) args(4) else "stats"

    val conf = new SparkConf().setAppName("PageRankGraph")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    GraphXUtils.registerKryoClasses(conf)

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statisticDir))

    val graph = GraphLoader
      .edgeListFile(sc, input, numEdgePartitions = numPartitions)

    // Run page rank algorithm and save the result
    if (output.isEmpty) {
      val totalPagerank = graph.staticPageRank(iteration).vertices.values.sum()
      println(s"Total pagerank: $totalPagerank")
    } else {
      graph.staticPageRank(iteration).vertices.saveAsTextFile(output)
    }

  }
}
