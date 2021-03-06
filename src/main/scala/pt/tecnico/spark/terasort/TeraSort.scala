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

package pt.tecnico.spark.terasort
/*
 * Copy from com.github.ehiggs.spark.terasort
 */

import com.google.common.primitives.UnsignedBytes
import org.apache.spark.{SparkConf, SparkContext}
import pt.tecnico.spark.util.StageRuntimeReportListener

/**
  * This is a great example program to stress test Spark's shuffle mechanism.
  *
  * See http://sortbenchmark.org/
  */
object TeraSort {

  implicit val caseInsensitiveOrdering = UnsignedBytes.lexicographicalComparator

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage:")
      println("spark-submit --class pt.tecnico.spark.terasort.TeraSort [jar] [input-file] [output-file] [statsDir]")
      System.exit(0)
    }

    // Process command line arguments
    val inputFile = args(0)
    val outputFile = args(1)
    val statsDir = if (args.length > 2) args(2) else "stats"

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName(s"TeraSort")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new StageRuntimeReportListener(statsDir))

    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile)
    val sorted = dataset.repartitionAndSortWithinPartitions(new TeraSortPartitioner(dataset.partitions.length))
    if (outputFile.isEmpty) {
      sorted.count()
    } else {
      sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)
    }
  }
}
