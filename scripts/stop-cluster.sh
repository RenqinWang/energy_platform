#!/bin/bash
# Energy Platform Cluster Shutdown Script
# Usage: bash ~/energy-platform/scripts/stop-cluster.sh

set -e

# Load environment variables
source ~/energy-platform/scripts/set-env.sh

echo "🛑 Stopping Energy Platform Cluster..."
echo ""

# Stop Kafka
echo "📨 Stopping Kafka..."
cd ~ && docker-compose stop 2>&1 | grep -v "warning" || true

# Stop Spark (on Node2)
echo "⚡ Stopping Spark..."
ssh student@node2 "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && ~/spark-3.5.7-bin-hadoop3/sbin/stop-all.sh" 2>&1 | grep -v "log4j" || true

# Stop HDFS
echo "📁 Stopping HDFS..."
$HADOOP_HOME/sbin/stop-dfs.sh 2>&1 | grep -v "log4j" || true

echo ""
echo "✅ Cluster shutdown complete!"
