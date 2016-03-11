#!/bin/bash

spark-submit --class pt.tecnico.spark.svm.SvmGen \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/svm \
    10000000 \
    10 \
    4