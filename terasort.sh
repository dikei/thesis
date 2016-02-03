#!/bin/bash

spark-submit --deploy-mode cluster --class pt.tecnico.spark.terasort.TeraSort target/scala-2.10/spark-testing-assembly-1.0.jar /home/dikei/Programming/Scala/spark-testing/data/tera /home/dikei/Programming/Scala/spark-testing/out/tera
