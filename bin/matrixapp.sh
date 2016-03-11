#!/bin/bash

spark-submit --class pt.tecnico.spark.matrix.MatrixFactorApp \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/matrix \
    /home/dikei/Tools/tmp/spark-testing/out/matrix \
    100 \
    10