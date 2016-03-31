#!/bin/bash

spark-submit --class pt.tecnico.spark.graph.TriangleCount \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/graph \
    /home/dikei/Tools/tmp/spark-testing/out/triangle-count \
    12
