#!/bin/bash

spark-submit --class pt.tecnico.spark.graph.GraphGen \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/graph \
    120000 \
    11111 \
    8
