#!/bin/bash

spark-submit --conf "spark.scheduler.removeStageBarrier=true" \
    --class pt.tecnico.spark.FrequentWordCount \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/pride.txt \
    300 \
    4
