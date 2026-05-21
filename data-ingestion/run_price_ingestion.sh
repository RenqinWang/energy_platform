#!/bin/bash

# 价格数据采集脚本启动器

set -e

# 设置环境变量
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH

# 脚本路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/ingest_price_data.py"

echo "=========================================="
echo "价格数据采集任务"
echo "=========================================="
echo "脚本路径: $PYTHON_SCRIPT"
echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 运行 PySpark 脚本
$SPARK_HOME/bin/spark-submit \
  --master "local[*]" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  "$PYTHON_SCRIPT"

echo ""
echo "结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
