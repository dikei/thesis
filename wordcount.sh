#!/bin/bash

spark-submit --deploy-mode cluster --class pt.tecnico.spark.WordCount target/scala-2.10/spark-testing_2.10-1.0.jar /home/dikei/Programming/Scala/spark-testing/data/pride.txt /home/dikei/Programming/Scala/spark-testing/out/pride
