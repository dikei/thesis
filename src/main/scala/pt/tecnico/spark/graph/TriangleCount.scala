package pt.tecnico.spark.graph

import org.apache.spark.graphx.{GraphLoader, GraphXUtils, PartitionStrategy}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Counting triangle in the graph
  */
object TriangleCount {

  def main(args: Array[String]): Unit = {

    val input = args(0)
    val output = args(1)
    val noPartitions = if (args.length > 2) args(2).toInt else -1
    val statsDir = if (args.length > 3) args(3) else "stats"

    val conf = new SparkConf().setAppName("TriangleCount")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    GraphXUtils.registerKryoClasses(conf)

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val graph = GraphLoader.edgeListFile(sc, input, canonicalOrientation = true,
      vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER, edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER)
      .partitionBy(PartitionStrategy.RandomVertexCut, noPartitions)

    // Find the triangle count for each vertex
    if (output.isEmpty) {
      // No output, we just call foreachPartition to force materialization
      val totalTriangle = graph.triangleCount().vertices.values.sum()
      println(s"Total triangle: $totalTriangle")
    } else {
      graph.triangleCount().vertices.saveAsTextFile(output)
    }
  }
}
