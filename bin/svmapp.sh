#!/bin/bash

spark-submit --class pt.tecnico.spark.svm.SvmApp \
    target/scala-2.10/thesis-assembly-1.0.jar \
    /home/dikei/Tools/tmp/spark-testing/data/svm \
    /home/dikei/Tools/tmp/spark-testing/out/svm \
    4 \
    10