#!/bin/bash

spark-submit --class pt.tecnico.spark.matrix.MatrixGen \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/matrix \
    2000 \
    2000 \
    200 \
    0.9 \
    false \
    0.1 \
    false \
    0.1