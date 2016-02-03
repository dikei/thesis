#!/bin/bash

spark-submit --deploy-mode cluster --class pt.tecnico.spark.terasort.TeraGen target/scala-2.10/spark-testing-assembly-1.0.jar 1G /home/dikei/Programming/Scala/spark-testing/data/tera
