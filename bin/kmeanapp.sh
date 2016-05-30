#!/bin/bash

spark-submit --class pt.tecnico.spark.kmean.KMeanApp \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/kmean \
    10 \
    4
