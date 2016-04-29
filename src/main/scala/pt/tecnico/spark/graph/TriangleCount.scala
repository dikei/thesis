package pt.tecnico.spark.graph

import org.apache.spark.graphx.{PartitionStrategy, GraphLoader}
import org.apache.spark.{SparkContext, SparkConf}
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
//    conf.set("spark.scheduler.removeStageBarrier", "true")

    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val graph = GraphLoader.edgeListFile(sc, input, canonicalOrientation = true, numEdgePartitions = noPartitions)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // Find the triangle count for each vertex
    graph.triangleCount().vertices.saveAsTextFile(output)
  }
}
