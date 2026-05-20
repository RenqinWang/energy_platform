#!/bin/bash
# Energy Platform Cluster Startup Script
# Usage: bash ~/energy-platform/scripts/start-cluster.sh

set -e

# Load environment variables
source ~/energy-platform/scripts/set-env.sh

echo "🚀 Starting Energy Platform Cluster..."
echo ""

# Start HDFS
echo "📁 Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh 2>&1 | grep -v "log4j" || true
sleep 5

# Start Spark (on Node2)
echo "⚡ Starting Spark..."
ssh student@node2 "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && ~/spark-3.5.7-bin-hadoop3/sbin/start-all.sh" 2>&1 | grep -v "log4j" || true
sleep 5

# Start Kafka
echo "📨 Starting Kafka..."
cd ~ && docker-compose start 2>&1 | grep -v "warning" || true
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
echo "   Spark Master:   http://node2:8080"
echo "   Kafka UI:       http://node1:8083"
