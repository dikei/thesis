/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Copy from org.apache.spark.examples.SparkPageRank
 */
package pt.tecnico.spark.graph

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * Computes the PageRank of URLs from an input file. Input file should
  * be in format of:
  * URL         neighbor URL
  * URL         neighbor URL
  * URL         neighbor URL
  * ...
  * where URL and their neighbors are separated by space(s).
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.graphx.lib.PageRank
  */
object PageRankSimple {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: spark-submit --class pt.tecnico.spark.graph.PageRankSimple [jar] [input] [output] [iteration] [statsDir]")
      System.exit(0)
    }

    val input = args(0)
    val output = args(1)
    val iters = if (args.length > 2) args(2).toInt else 10
    val numPartitions = args(3).toInt
    val statsDir = if (args.length > 4) args(4) else "stats"

    val sparkConf = new SparkConf().setAppName("PageRankSimple")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ctx = new SparkContext(sparkConf)
    ctx.addSparkListener(new StageRuntimeReportListener(statsDir))

    // Do the word count and save output
    val createCombiner = (v: Double) => v
    val mergeValue = (a: Double, b: Double) => a + b
    val mergeCombiners = (a: Double, b: Double) => {
      a + b
    }

    val lines = ctx.textFile(input)
    val links = lines.map { s =>
        val parts = s.split("\\s+")
        (parts(0).toInt, parts(1).toInt)
      }
      .distinct(numPartitions)
      .groupByKey()
      .persist(StorageLevel.MEMORY_ONLY_SER)
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
//      ranks = contribs.combineByKeyWithInterval(
//        createCombiner,
//        mergeValue,
//        mergeCombiners,
//        Partitioner.defaultPartitioner(contribs)
//      )
//      .mapValues {
//        0.15 + 0.85 * _
//      }
//      .reduceByKey((a, b) => b)
    }

    if (output.isEmpty) {
      val totalPagerank = ranks.map(_._2).sum()
      println(s"Total pagerank: $totalPagerank")
    } else {
      ranks.saveAsTextFile(output)
    }

    ctx.stop()
  }
}