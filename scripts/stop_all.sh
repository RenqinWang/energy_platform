#!/bin/bash

#############################################
# 智慧能源网监测平台 - 停止所有服务
#############################################

export HADOOP_HOME=/home/student/hadoop-3.3.6
export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=========================================="
echo "  停止所有服务"
echo "=========================================="
echo ""

# 停止前端服务
echo -e "${YELLOW}[1/5]${NC} 停止前端服务..."
pkill -f "python3 -m http.server 8080" 2>/dev/null || echo "  前端服务未运行"
echo -e "${GREEN}✓${NC} 前端服务已停止"

# 停止后端API
echo -e "${YELLOW}[2/5]${NC} 停止后端API..."
pkill -f "uvicorn main:app" 2>/dev/null || echo "  后端API未运行"
echo -e "${GREEN}✓${NC} 后端API已停止"

# 停止Kafka生产者
echo -e "${YELLOW}[3/5]${NC} 停止Kafka生产者..."
docker stop friendly_shockley 2>/dev/null || echo "  Kafka生产者未运行"
echo -e "${GREEN}✓${NC} Kafka生产者已停止"

# 停止Spark集群
echo -e "${YELLOW}[4/5]${NC} 停止Spark集群..."
ssh student@node2 "$SPARK_HOME/sbin/stop-master.sh" 2>/dev/null || echo "  无法连接到node2"
for node in node1 node2 node3; do
    ssh student@$node "$SPARK_HOME/sbin/stop-worker.sh" 2>/dev/null || echo "  无法连接到$node"
done
echo -e "${GREEN}✓${NC} Spark集群已停止"

# 停止HDFS集群
echo -e "${YELLOW}[5/5]${NC} 停止HDFS集群..."
$HADOOP_HOME/sbin/stop-dfs.sh 2>/dev/null || echo "  HDFS未运行"
echo -e "${GREEN}✓${NC} HDFS集群已停止"

echo ""
echo -e "${GREEN}✓ 所有服务已停止${NC}"
echo ""
