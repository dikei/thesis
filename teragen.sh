#!/bin/bash

spark-submit --class pt.tecnico.spark.terasort.TeraGen target/scala-2.10/spark-testing-assembly-1.0.jar 5G /home/dikei/Tools/tmp/spark-testing/data/tera
