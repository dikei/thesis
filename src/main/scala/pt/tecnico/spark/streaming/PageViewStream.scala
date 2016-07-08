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

package pt.tecnico.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PageViewStream {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: PageViewStream <metric> <host> <port> <output>")
      System.err.println("<metric> must be one of pageCounts, slidingPageCounts," +
        " errorRatePerZipCode, activeUserCount, popularUsersSeen")
      System.exit(1)
    }
    val metric = args(0)
    val host = args(1)
    val port = args(2).toInt
    val output = args(3)

    // Create the context
    val conf = new SparkConf().setAppName("PageViewStream")
    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a ReceiverInputDStream on target host:port and convert each line to a PageView
    val pageViews = ssc.socketTextStream(host, port)
      .flatMap(_.split("\n"))
      .map(PageView.fromString)

    // Return a count of views per URL seen in each batch
    val pageCounts = pageViews.map(view => view.url).countByValue()

    // Return a sliding window of page views per URL in the last ten seconds
    val slidingPageCounts = pageViews.map(view => view.url)
      .countByValueAndWindow(Seconds(10), Seconds(2))


    // Return the rate of error pages (a non 200 status) in each zip code over the last 30 seconds
    val statusesPerZipCode = pageViews.window(Seconds(30), Seconds(2))
      .map(view => (view.zipCode, view.status))
      .groupByKey()
    val errorRatePerZipCode = statusesPerZipCode.map{
      case(zip, statuses) =>
        val normalCount = statuses.count(_ == 200)
        val errorCount = statuses.size - normalCount
        val errorRatio = errorCount.toFloat / statuses.size
        if (errorRatio > 0.05) {
          "%s: **%s**".format(zip, errorRatio)
        } else {
          "%s: %s".format(zip, errorRatio)
        }
    }

    // Return the number unique users in last 15 seconds
    val activeUserCount = pageViews.window(Seconds(15), Seconds(2))
      .map(view => (view.userID, 1))
      .groupByKey()
      .count()
      .map("Unique active users: " + _)

    // An external dataset we want to join to this stream
    val userList = ssc.sparkContext.parallelize(
      Map(1 -> "Patrick Wendell", 2 -> "Reynold Xin", 3 -> "Matei Zaharia").toSeq)

    metric match {
      case "pageCounts" => pageCounts.saveAsTextFiles(output)
      case "slidingPageCounts" => slidingPageCounts.saveAsTextFiles(output)
      case "errorRatePerZipCode" => errorRatePerZipCode.saveAsTextFiles(output)
      case "activeUserCount" => activeUserCount.saveAsTextFiles(output)
      case "popularUsersSeen" =>
        // Look for users in our existing dataset and print it out if we have a match
        pageViews.map(view => (view.userID, 1))
          .foreachRDD((rdd, time) => rdd.join(userList)
            .map(_._2._2)
            .take(10)
            .foreach(u => println("Saw user %s at time %s".format(u, time))))
      case _ => println("Invalid metric entered: " + metric)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
