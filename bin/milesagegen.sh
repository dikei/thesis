#!/bin/bash

spark-submit --class pt.tecnico.spark.milesage.MilesageGen \
    target/scala-2.10/thesis-assembly-1.0.jar \
    2500000 \
    100000 \
    10 \
    8 \
    /home/dikei/Tools/tmp/spark-testing/out/passenger \
    /home/dikei/Tools/tmp/spark-testing/out/flight
