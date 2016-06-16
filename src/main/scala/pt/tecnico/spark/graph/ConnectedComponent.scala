package pt.tecnico.spark.graph

import org.apache.spark.graphx.{GraphLoader, GraphXUtils, PartitionStrategy}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Created by dikei on 2/12/16.
  */
object ConnectedComponent {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: ")
      println("spark-submit --class pt.tecnico.spark.graph.ConnectedComponent [jar] [input] [output] [#numOfPartitions] [statDir]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val partitionCount = if (args.length > 2) args(2).toInt else -1
    val statsDir = if (args.length > 3) args(3) else "stats"
    val conf = new SparkConf().setAppName("ConnectedComponent")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext(conf)

    val listener = new StageRuntimeReportListener(statsDir)
    sc.addSparkListener(listener)

    val graph = GraphLoader
      .edgeListFile(sc, input,
        vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER,
        edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER,
        numEdgePartitions = partitionCount)
//      .partitionBy(PartitionStrategy.EdgePartition2D, partitionCount)

    // Calculate and save the connected components
    if (output.isEmpty) {
      val componentCount = graph.connectedComponents().vertices.values.distinct().count()
      println(s"Component count: $componentCount")
    } else {
      graph.connectedComponents().vertices.saveAsTextFile(output)
    }
  }
}
