#!/bin/bash

spark-submit --class pt.tecnico.spark.TopKCount target/scala-2.10/spark-testing-assembly-1.0.jar /home/dikei/Tools/tmp/spark-testing/data/pride.txt 10
