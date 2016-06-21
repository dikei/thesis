#!/bin/bash

spark-submit --class pt.tecnico.spark.milesage.MilesageGen \
    target/scala-2.10/thesis-assembly-1.0.jar \
    10000000 \
    100000 \
    20 \
    8 \
    /home/dikei/Tools/tmp/spark-testing/out/passenger \
    /home/dikei/Tools/tmp/spark-testing/out/flight
