#!/bin/bash

echo "Packaging distribution"
sbt assembly
mkdir -p thesis/target/scala-2.10
cp -rf bin thesis/
cp -rf target/scala-2.10/thesis-assembly-1.0.jar thesis/target/scala-2.10
tar czf thesis.tar.gz thesis

echo "Upload to gateway"
scp thesis.tar.gz grid5k:grenoble/spark-hadoop-ansible/thesis.tar.gz
