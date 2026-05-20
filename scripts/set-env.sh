#!/bin/bash
# Energy Platform Environment Variables
# Source this file: source ~/energy-platform/scripts/set-env.sh

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=~/hadoop-3.3.6
export SPARK_HOME=~/spark-3.5.7-bin-hadoop3
export HADOOP_CONF_DIR=~/hdfs-conf
export FLINK_HOME=~/flink-1.20.0

export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$FLINK_HOME/bin:$PATH

# Python environment
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

echo "✅ Environment variables set:"
echo "   JAVA_HOME=$JAVA_HOME"
echo "   HADOOP_HOME=$HADOOP_HOME"
echo "   SPARK_HOME=$SPARK_HOME"
echo "   HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "   PYTHONPATH includes Spark Python libraries"
