#!/bin/bash

spark-submit --class pt.tecnico.spark.graph.ConnectedComponent \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/web-Google.txt \
    /home/dikei/Tools/tmp/spark-testing/out/connectedcomponent \
    8
