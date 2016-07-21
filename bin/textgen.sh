#!/bin/bash

spark-submit --class pt.tecnico.spark.TextGenerator \
    target/scala-2.10/thesis-assembly-1.0.jar \
    `expr 1024 \* 1024 \* 2`  \
    4 \
    /home/dikei/Tools/tmp/spark-testing/data/text \
    -1
