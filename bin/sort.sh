#!/bin/bash

spark-submit --class pt.tecnico.spark.Sort \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/pride.txt \
    /home/dikei/Tools/tmp/spark-testing/out/pride \
    /home/dikei/Programming/Scala/spark/thesis/stats
