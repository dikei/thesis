#!/bin/bash

spark-submit --class pt.tecnico.spark.Shuffle \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/pride.short.txt \
    5 \
    8
