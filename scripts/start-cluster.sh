#!/bin/bash
# Energy Platform Cluster Startup Script
# Usage: bash ~/energy-platform/scripts/start-cluster.sh

set -e

# Load environment variables
source ~/energy-platform/scripts/set-env.sh

NODE1_IP="192.168.0.94"
NODE2_IP="192.168.1.87"
NODE3_IP="192.168.1.19"
SPARK_MASTER_URL="spark://${NODE2_IP}:7077"

echo "🚀 Starting Energy Platform Cluster..."
echo ""

# Start HDFS
echo "📁 Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh 2>&1 | grep -v "log4j" || true
sleep 5

# Start Spark with explicit private IPs. Node2's hostname can resolve to
# 127.0.1.1 on the VM itself, so relying on spark-env.sh makes port 7077
# unreachable from Node1/Node3.
echo "⚡ Starting Spark..."
ssh student@node2 "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3 && \$SPARK_HOME/sbin/stop-master.sh >/dev/null 2>&1 || true; \$SPARK_HOME/sbin/stop-worker.sh >/dev/null 2>&1 || true" 2>&1 | grep -v "log4j" || true
ssh student@node1 "export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3 && \$SPARK_HOME/sbin/stop-worker.sh >/dev/null 2>&1 || true" 2>&1 | grep -v "log4j" || true
ssh student@node3 "export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3 && \$SPARK_HOME/sbin/stop-worker.sh >/dev/null 2>&1 || true" 2>&1 | grep -v "log4j" || true
sleep 3
ssh student@node2 "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3 && export SPARK_LOCAL_IP=${NODE2_IP} && export SPARK_MASTER_HOST=${NODE2_IP} && \$SPARK_HOME/sbin/start-master.sh --host ${NODE2_IP} --port 7077 --webui-port 8080" 2>&1 | grep -v "log4j" || true
ssh student@node1 "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3 && export SPARK_LOCAL_IP=${NODE1_IP} && \$SPARK_HOME/sbin/start-worker.sh ${SPARK_MASTER_URL}" 2>&1 | grep -v "log4j" || true
ssh student@node2 "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3 && export SPARK_LOCAL_IP=${NODE2_IP} && \$SPARK_HOME/sbin/start-worker.sh ${SPARK_MASTER_URL}" 2>&1 | grep -v "log4j" || true
ssh student@node3 "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3 && export SPARK_LOCAL_IP=${NODE3_IP} && \$SPARK_HOME/sbin/start-worker.sh ${SPARK_MASTER_URL}" 2>&1 | grep -v "log4j" || true
sleep 5

# Start Kafka realtime path
echo "📨 Starting Kafka realtime path..."
bash ~/energy-platform/scripts/start-kafka-realtime.sh
sleep 5

# Start Kafka to Bronze streaming job
echo "🌊 Starting Kafka -> Bronze streaming job..."
bash ~/energy-platform/scripts/start-kafka-to-bronze.sh
sleep 5

echo ""
echo "✅ Cluster startup complete!"
echo ""
echo "📊 Cluster Status:"
echo "   Node1: $(jps | grep -E 'NameNode|DataNode|Worker' | wc -l) processes"
echo "   Node2: $(ssh student@node2 'jps' | grep -E 'Master|DataNode|Worker' | wc -l) processes"
echo "   Node3: $(ssh student@node3 'jps' | grep -E 'DataNode|Worker' | wc -l) processes"
echo ""
echo "🌐 Web UIs:"
echo "   HDFS NameNode:  http://node1:9870"
echo "   Spark Master:   http://${NODE2_IP}:8080"
echo "   Kafka UI:       http://node2:8083"
echo ""
echo "📝 Streaming log:"
echo "   /tmp/kafka_to_bronze_streaming.log"
