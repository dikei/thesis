#!/bin/bash

spark-submit --class pt.tecnico.spark.milesage.MilesageAppLoopNoJobBarrier \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/out/passenger \
    /home/dikei/Tools/tmp/spark-testing/out/flight \
    "" \
    8
