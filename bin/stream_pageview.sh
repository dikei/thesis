#!/bin/bash

spark-submit --class pt.tecnico.spark.streaming.PageViewStream \
    target/scala-2.10/thesis-assembly-1.0.jar \
    errorRatePerZipCode \
    localhost \
    9999 \
    /home/dikei/Tools/tmp/spark-testing/out/pageview/streaming/

