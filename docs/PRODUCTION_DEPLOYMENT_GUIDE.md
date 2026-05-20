# 智慧能源网监测平台 - 生产环境部署指南

## 📋 文档概述

本文档提供生产环境的一键部署方案，包括环境检查、服务启动、数据初始化和验证测试的完整流程。

**目标**: 在全新的3节点集群上快速部署完整的能源监测平台  
**部署时间**: 约30分钟  
**技术栈**: Hadoop 3.3.6, Spark 3.5.7, Kafka, FastAPI, Web前端

---

## 🎯 部署架构

### 节点规划

| 节点 | 角色 | 部署组件 | 最低配置 |
|------|------|----------|----------|
| **Node-1** | 入口+存储+计算 | HDFS NameNode, HDFS DataNode, Spark Worker, Backend API, Frontend | 4核8GB, 50GB磁盘 |
| **Node-2** | 调度+存储+计算+消息 | Spark Master, HDFS DataNode, Spark Worker, Kafka Broker | 4核8GB, 50GB磁盘 |
| **Node-3** | 存储+计算+消息 | HDFS DataNode, Spark Worker, Kafka Broker | 4核8GB, 50GB磁盘 |

### 端口规划

| 服务 | 端口 | 说明 |
|------|------|------|
| HDFS NameNode | 9000 | HDFS客户端连接 |
| HDFS NameNode Web UI | 9870 | HDFS管理界面 |
| Spark Master | 7077 | Spark集群通信 |
| Spark Master Web UI | 8080 | Spark管理界面 |
| Kafka Broker | 9092 | Kafka客户端连接 |
| Backend API | 8000 | FastAPI服务 |
| Frontend | 8080 | Web前端界面 |

---

## 📦 前置条件

### 1. 系统要求

**操作系统**: Ubuntu 22.04 LTS (推荐) 或 CentOS 7+

**软件依赖**:
```bash
# 检查Java版本 (需要Java 17)
java -version

# 检查Python版本 (需要Python 3.8+)
python3 --version

# 检查Docker (用于Kafka生产者)
docker --version
```

### 2. 网络配置

**主机名映射** (`/etc/hosts`):
```bash
192.168.0.94    node1
192.168.1.87    node2
192.168.1.19    node3
```

**SSH免密登录**:
```bash
# 在Node-1上生成密钥并分发到所有节点
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
ssh-copy-id student@node1
ssh-copy-id student@node2
ssh-copy-id student@node3
```

### 3. 软件安装路径

确保以下软件已安装在所有节点：

```bash
# Hadoop
/home/student/hadoop-3.3.6/

# Spark
/home/student/spark-3.5.7-bin-hadoop3/

# Java
/usr/lib/jvm/java-17-openjdk-amd64

# 项目代码
/home/student/energy-platform/
```

---

## 🚀 一键部署脚本

### 创建部署脚本

在Node-1上创建 `/home/student/energy-platform/scripts/deploy_production.sh`:

```bash
#!/bin/bash

#############################################
# 智慧能源网监测平台 - 生产环境一键部署脚本
#############################################

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 环境变量
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/student/hadoop-3.3.6
export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3
export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH

PROJECT_HOME=/home/student/energy-platform
HDFS_NAMENODE="hdfs://node1:9000"
SPARK_MASTER="spark://node2:7077"

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 打印标题
print_header() {
    echo ""
    echo "=========================================="
    echo "  $1"
    echo "=========================================="
}

# 检查命令是否存在
check_command() {
    if ! command -v $1 &> /dev/null; then
        log_error "$1 命令不存在，请先安装"
        exit 1
    fi
}

# 检查端口是否被占用
check_port() {
    if netstat -tuln | grep -q ":$1 "; then
        log_warning "端口 $1 已被占用"
        return 1
    else
        log_info "端口 $1 可用"
        return 0
    fi
}

#############################################
# 步骤1: 环境检查
#############################################
step1_check_environment() {
    print_header "步骤1: 环境检查"

    log_info "检查Java环境..."
    if [ -z "$JAVA_HOME" ]; then
        log_error "JAVA_HOME未设置"
        exit 1
    fi
    java -version 2>&1 | head -n 1
    log_success "Java环境正常"

    log_info "检查Hadoop安装..."
    if [ ! -d "$HADOOP_HOME" ]; then
        log_error "Hadoop未安装在 $HADOOP_HOME"
        exit 1
    fi
    log_success "Hadoop安装正常"

    log_info "检查Spark安装..."
    if [ ! -d "$SPARK_HOME" ]; then
        log_error "Spark未安装在 $SPARK_HOME"
        exit 1
    fi
    log_success "Spark安装正常"

    log_info "检查项目代码..."
    if [ ! -d "$PROJECT_HOME" ]; then
        log_error "项目代码不存在于 $PROJECT_HOME"
        exit 1
    fi
    log_success "项目代码存在"

    log_info "检查网络连通性..."
    for node in node1 node2 node3; do
        if ping -c 1 $node &> /dev/null; then
            log_success "$node 网络连通"
        else
            log_error "$node 网络不通"
            exit 1
        fi
    done

    log_success "环境检查完成"
}

#############################################
# 步骤2: 启动HDFS集群
#############################################
step2_start_hdfs() {
    print_header "步骤2: 启动HDFS集群"

    log_info "检查HDFS进程..."
    if jps | grep -q "NameNode"; then
        log_warning "HDFS已在运行，跳过启动"
        return 0
    fi

    log_info "启动HDFS集群..."
    $HADOOP_HOME/sbin/start-dfs.sh

    sleep 10

    log_info "验证HDFS进程..."
    if jps | grep -q "NameNode"; then
        log_success "NameNode已启动"
    else
        log_error "NameNode启动失败"
        exit 1
    fi

    if jps | grep -q "DataNode"; then
        log_success "DataNode已启动"
    else
        log_error "DataNode启动失败"
        exit 1
    fi

    log_info "检查HDFS健康状态..."
    $HADOOP_HOME/bin/hdfs dfsadmin -report | head -n 20

    log_success "HDFS集群启动完成"
}

#############################################
# 步骤3: 启动Spark集群
#############################################
step3_start_spark() {
    print_header "步骤3: 启动Spark集群"

    log_info "检查Spark Master进程..."
    if ssh student@node2 "jps | grep -q Master"; then
        log_warning "Spark Master已在运行"
    else
        log_info "在Node-2上启动Spark Master..."
        ssh student@node2 "$SPARK_HOME/sbin/start-master.sh"
        sleep 5
    fi

    log_info "检查Spark Worker进程..."
    for node in node1 node2 node3; do
        if ssh student@$node "jps | grep -q Worker"; then
            log_warning "Spark Worker已在$node上运行"
        else
            log_info "在$node上启动Spark Worker..."
            ssh student@$node "$SPARK_HOME/sbin/start-worker.sh $SPARK_MASTER"
            sleep 3
        fi
    done

    sleep 5

    log_info "验证Spark集群状态..."
    if ssh student@node2 "jps | grep -q Master"; then
        log_success "Spark Master已启动"
    else
        log_error "Spark Master启动失败"
        exit 1
    fi

    log_success "Spark集群启动完成"
}

#############################################
# 步骤4: 启动Kafka生产者
#############################################
step4_start_kafka() {
    print_header "步骤4: 启动Kafka生产者"

    log_info "检查Docker容器..."
    if docker ps | grep -q "friendly_shockley"; then
        log_warning "Kafka生产者容器已在运行"
        return 0
    fi

    log_info "启动Kafka生产者容器..."
    docker start friendly_shockley

    sleep 5

    log_info "验证容器状态..."
    if docker ps | grep -q "friendly_shockley"; then
        log_success "Kafka生产者已启动"
        docker logs --tail 10 friendly_shockley
    else
        log_error "Kafka生产者启动失败"
        exit 1
    fi

    log_success "Kafka生产者启动完成"
}

#############################################
# 步骤5: 初始化数据湖
#############################################
step5_init_datalake() {
    print_header "步骤5: 初始化数据湖"

    log_info "检查HDFS数据湖目录..."
    if $HADOOP_HOME/bin/hdfs dfs -test -d $HDFS_NAMENODE/lake; then
        log_warning "数据湖目录已存在"
        
        log_info "检查Bronze层数据..."
        if $HADOOP_HOME/bin/hdfs dfs -test -d $HDFS_NAMENODE/lake/bronze/bronze_sensor_raw; then
            log_success "Bronze层数据已存在，跳过初始化"
            return 0
        fi
    else
        log_info "创建数据湖目录结构..."
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_NAMENODE/lake/bronze
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_NAMENODE/lake/silver
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_NAMENODE/lake/gold
        $HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_NAMENODE/checkpoints
        log_success "数据湖目录创建完成"
    fi

    log_info "同步价格数据..."
    cd $PROJECT_HOME
    $SPARK_HOME/bin/spark-submit \
        --master 'local[*]' \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        data-ingestion/price-sync/sync_price_data.py
    log_success "价格数据同步完成"

    log_info "加载点位字典..."
    $SPARK_HOME/bin/spark-submit \
        --master 'local[*]' \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        data-ingestion/dict-loader/parse_pos_ttl.py
    log_success "点位字典加载完成"

    log_info "等待Kafka数据流入 (30秒)..."
    sleep 30

    log_info "检查Bronze层数据..."
    $HADOOP_HOME/bin/hdfs dfs -ls $HDFS_NAMENODE/lake/bronze/

    log_success "数据湖初始化完成"
}

#############################################
# 步骤6: 生成Silver层数据
#############################################
step6_generate_silver() {
    print_header "步骤6: 生成Silver层数据"

    cd $PROJECT_HOME

    log_info "生成点位事实表..."
    $SPARK_HOME/bin/spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --executor-memory 2g \
        --executor-cores 2 \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        data-processing/silver-layer/generate_point_fact.py
    log_success "点位事实表生成完成"

    log_info "生成冷机状态宽表..."
    $SPARK_HOME/bin/spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --executor-memory 2g \
        --executor-cores 2 \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        data-processing/silver-layer/generate_chiller_status.py
    log_success "冷机状态宽表生成完成"

    log_info "生成价格维表..."
    $SPARK_HOME/bin/spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --executor-memory 2g \
        --executor-cores 2 \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        data-processing/silver-layer/generate_price_dim.py
    log_success "价格维表生成完成"

    log_success "Silver层数据生成完成"
}

#############################################
# 步骤7: 生成Gold层数据
#############################################
step7_generate_gold() {
    print_header "步骤7: 生成Gold层数据"

    cd $PROJECT_HOME

    log_info "生成小时供能曲线..."
    $SPARK_HOME/bin/spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --executor-memory 2g \
        --executor-cores 2 \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        data-processing/gold-layer/generate_supply_curve.py
    log_success "小时供能曲线生成完成"

    log_info "生成日报表..."
    $SPARK_HOME/bin/spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --executor-memory 2g \
        --executor-cores 2 \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        data-processing/gold-layer/generate_daily_report.py
    log_success "日报表生成完成"

    log_success "Gold层数据生成完成"
}

#############################################
# 步骤8: 启动后端API服务
#############################################
step8_start_backend() {
    print_header "步骤8: 启动后端API服务"

    log_info "检查后端API进程..."
    if pgrep -f "uvicorn main:app" > /dev/null; then
        log_warning "后端API已在运行"
        log_info "停止现有进程..."
        pkill -f "uvicorn main:app"
        sleep 3
    fi

    cd $PROJECT_HOME/backend

    log_info "检查Python虚拟环境..."
    if [ ! -d "venv" ]; then
        log_info "创建虚拟环境..."
        python3 -m venv venv
    fi

    log_info "激活虚拟环境并安装依赖..."
    source venv/bin/activate
    pip install -q -r requirements.txt

    log_info "后台启动API服务..."
    nohup uvicorn main:app --host 0.0.0.0 --port 8000 > /tmp/backend_api.log 2>&1 &
    
    sleep 5

    log_info "验证API服务..."
    if curl -s http://localhost:8000/health | grep -q "healthy"; then
        log_success "后端API服务启动成功"
        log_info "API文档: http://localhost:8000/docs"
    else
        log_error "后端API服务启动失败"
        log_error "查看日志: tail -f /tmp/backend_api.log"
        exit 1
    fi

    log_success "后端API服务启动完成"
}

#############################################
# 步骤9: 启动前端服务
#############################################
step9_start_frontend() {
    print_header "步骤9: 启动前端服务"

    log_info "检查前端服务进程..."
    if pgrep -f "python3 -m http.server 8080" > /dev/null; then
        log_warning "前端服务已在运行"
        log_info "停止现有进程..."
        pkill -f "python3 -m http.server 8080"
        sleep 3
    fi

    cd $PROJECT_HOME/frontend

    log_info "后台启动前端服务..."
    nohup python3 -m http.server 8080 > /tmp/frontend.log 2>&1 &
    
    sleep 3

    log_info "验证前端服务..."
    if curl -s http://localhost:8080 | grep -q "能源平台"; then
        log_success "前端服务启动成功"
        log_info "访问地址: http://localhost:8080"
    else
        log_error "前端服务启动失败"
        log_error "查看日志: tail -f /tmp/frontend.log"
        exit 1
    fi

    log_success "前端服务启动完成"
}

#############################################
# 步骤10: 运行验证测试
#############################################
step10_run_tests() {
    print_header "步骤10: 运行验证测试"

    cd $PROJECT_HOME

    log_info "运行分布式环境测试..."
    $SPARK_HOME/bin/spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --executor-memory 2g \
        --executor-cores 2 \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        scripts/test_distributed.py

    if [ $? -eq 0 ]; then
        log_success "所有测试通过"
    else
        log_error "测试失败，请检查日志"
        exit 1
    fi

    log_success "验证测试完成"
}

#############################################
# 步骤11: 打印部署信息
#############################################
step11_print_summary() {
    print_header "部署完成"

    echo ""
    echo "🎉 智慧能源网监测平台部署成功！"
    echo ""
    echo "📊 服务访问地址:"
    echo "  - 前端界面: http://localhost:8080"
    echo "  - API文档:  http://localhost:8000/docs"
    echo "  - API健康检查: http://localhost:8000/health"
    echo ""
    echo "🔧 管理界面:"
    echo "  - HDFS Web UI: http://node1:9870"
    echo "  - Spark Master UI: http://node2:8080"
    echo ""
    echo "📁 数据湖路径:"
    echo "  - HDFS: hdfs://node1:9000/lake"
    echo "  - Bronze层: hdfs://node1:9000/lake/bronze"
    echo "  - Silver层: hdfs://node1:9000/lake/silver"
    echo "  - Gold层: hdfs://node1:9000/lake/gold"
    echo ""
    echo "📝 日志文件:"
    echo "  - 后端API: /tmp/backend_api.log"
    echo "  - 前端服务: /tmp/frontend.log"
    echo ""
    echo "🛠️ 常用命令:"
    echo "  - 查看HDFS数据: hdfs dfs -ls hdfs://node1:9000/lake/"
    echo "  - 查看Spark任务: http://node2:8080"
    echo "  - 停止所有服务: $PROJECT_HOME/scripts/stop_all.sh"
    echo ""
    log_success "部署完成！"
}

#############################################
# 主函数
#############################################
main() {
    echo ""
    echo "=========================================="
    echo "  智慧能源网监测平台"
    echo "  生产环境一键部署脚本"
    echo "=========================================="
    echo ""
    echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""

    # 执行所有步骤
    step1_check_environment
    step2_start_hdfs
    step3_start_spark
    step4_start_kafka
    step5_init_datalake
    step6_generate_silver
    step7_generate_gold
    step8_start_backend
    step9_start_frontend
    step10_run_tests
    step11_print_summary

    echo ""
    echo "结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
}

# 运行主函数
main "$@"
```

---

## 🛑 停止所有服务

创建停止脚本 `/home/student/energy-platform/scripts/stop_all.sh`:

```bash
#!/bin/bash

#############################################
# 停止所有服务
#############################################

export HADOOP_HOME=/home/student/hadoop-3.3.6
export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3

echo "=========================================="
echo "  停止所有服务"
echo "=========================================="

# 停止前端服务
echo "[1/5] 停止前端服务..."
pkill -f "python3 -m http.server 8080"
echo "✓ 前端服务已停止"

# 停止后端API
echo "[2/5] 停止后端API..."
pkill -f "uvicorn main:app"
echo "✓ 后端API已停止"

# 停止Kafka生产者
echo "[3/5] 停止Kafka生产者..."
docker stop friendly_shockley
echo "✓ Kafka生产者已停止"

# 停止Spark集群
echo "[4/5] 停止Spark集群..."
ssh student@node2 "$SPARK_HOME/sbin/stop-master.sh"
for node in node1 node2 node3; do
    ssh student@$node "$SPARK_HOME/sbin/stop-worker.sh"
done
echo "✓ Spark集群已停止"

# 停止HDFS集群
echo "[5/5] 停止HDFS集群..."
$HADOOP_HOME/sbin/stop-dfs.sh
echo "✓ HDFS集群已停止"

echo ""
echo "✓ 所有服务已停止"
echo ""
```

---

## 🔍 故障排查

### 常见问题

#### 1. HDFS NameNode启动失败

**症状**: `jps`命令看不到NameNode进程

**可能原因**:
- NameNode格式化问题
- 端口9000被占用
- 磁盘空间不足

**解决方案**:
```bash
# 检查端口占用
netstat -tuln | grep 9000

# 检查磁盘空间
df -h

# 查看NameNode日志
tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log

# 如需重新格式化 (⚠️ 会删除所有数据)
$HADOOP_HOME/bin/hdfs namenode -format
```

#### 2. Spark Worker无法连接Master

**症状**: Worker启动后立即退出

**可能原因**:
- Master地址配置错误
- 网络不通
- 防火墙阻止

**解决方案**:
```bash
# 测试网络连通性
telnet node2 7077

# 检查Master是否运行
ssh student@node2 "jps | grep Master"

# 查看Worker日志
tail -f $SPARK_HOME/logs/spark-*-worker-*.log

# 检查防火墙
sudo ufw status
```

#### 3. 后端API无法连接HDFS

**症状**: API返回500错误，日志显示HDFS连接失败

**可能原因**:
- HDFS未启动
- JAVA_HOME未设置
- 网络配置错误

**解决方案**:
```bash
# 检查HDFS状态
hdfs dfsadmin -report

# 设置JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 测试HDFS连接
hdfs dfs -ls hdfs://node1:9000/

# 查看API日志
tail -f /tmp/backend_api.log
```

#### 4. 前端无法访问后端API

**症状**: 前端显示"无法连接到服务器"

**可能原因**:
- 后端API未启动
- CORS配置错误
- 端口被防火墙阻止

**解决方案**:
```bash
# 检查后端API状态
curl http://localhost:8000/health

# 检查端口监听
netstat -tuln | grep 8000

# 查看API日志
tail -f /tmp/backend_api.log

# 测试CORS
curl -H "Origin: http://localhost:8080" \
     -H "Access-Control-Request-Method: GET" \
     -X OPTIONS http://localhost:8000/api/stations
```

#### 5. Kafka数据未流入

**症状**: Bronze层没有数据

**可能原因**:
- Kafka生产者容器未启动
- Kafka Broker未运行
- 网络连接问题

**解决方案**:
```bash
# 检查容器状态
docker ps | grep friendly_shockley

# 查看容器日志
docker logs friendly_shockley

# 启动容器
docker start friendly_shockley

# 检查Kafka Broker
ssh student@node2 "jps | grep Kafka"
```

---

## 📊 监控和维护

### 日常监控

**1. 检查集群健康状态**:
```bash
# HDFS健康检查
hdfs dfsadmin -report

# Spark集群状态
# 访问 http://node2:8080

# 检查所有进程
for node in node1 node2 node3; do
    echo "=== $node ==="
    ssh student@$node "jps"
done
```

**2. 检查数据湖大小**:
```bash
hdfs dfs -du -h hdfs://node1:9000/lake/
```

**3. 检查API服务状态**:
```bash
curl http://localhost:8000/health
```

### 定期维护

**1. Delta Lake优化 (每周)**:
```bash
# 优化Bronze层
spark-submit --master spark://node2:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --py-files optimize_tables.py

# 清理历史版本 (保留7天)
spark-submit --master spark://node2:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --py-files vacuum_tables.py
```

**2. 日志清理 (每月)**:
```bash
# 清理Spark日志
find $SPARK_HOME/logs -name "*.log" -mtime +30 -delete

# 清理Hadoop日志
find $HADOOP_HOME/logs -name "*.log" -mtime +30 -delete
```

**3. 数据备份 (每天)**:
```bash
# 备份Gold层数据
hdfs dfs -cp hdfs://node1:9000/lake/gold \
          hdfs://node1:9000/backup/gold_$(date +%Y%m%d)
```

---

## 🔐 安全加固

### 生产环境建议

**1. 启用身份认证**:
```python
# backend/main.py 添加JWT认证
from fastapi.security import HTTPBearer

security = HTTPBearer()

@app.get("/api/protected")
async def protected_route(credentials: HTTPAuthorizationCredentials = Depends(security)):
    # 验证token
    pass
```

**2. 配置防火墙**:
```bash
# 只允许内网访问
sudo ufw allow from 192.168.0.0/16 to any port 9000
sudo ufw allow from 192.168.0.0/16 to any port 7077
sudo ufw allow from 192.168.0.0/16 to any port 8080

# 允许外网访问前端和API
sudo ufw allow 8000
sudo ufw allow 8080
```

**3. 启用HTTPS**:
```bash
# 使用Nginx反向代理
sudo apt install nginx
sudo certbot --nginx -d your-domain.com
```

**4. 数据加密**:
```bash
# HDFS透明加密
hdfs crypto -createZone -keyName myKey -path /lake/sensitive
```

---

## 📚 附录

### A. 完整的环境变量配置

在 `~/.bashrc` 中添加:

```bash
# Java
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Hadoop
export HADOOP_HOME=/home/student/hadoop-3.3.6
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Spark
export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Python
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
```

### B. 快速命令参考

```bash
# 查看所有Java进程
jps

# 查看HDFS文件
hdfs dfs -ls hdfs://node1:9000/lake/

# 查看Spark任务
spark-submit --status <driver-id>

# 查看API日志
tail -f /tmp/backend_api.log

# 重启后端API
pkill -f "uvicorn main:app" && cd /home/student/energy-platform/backend && ./start_api.sh

# 重启前端
pkill -f "python3 -m http.server 8080" && cd /home/student/energy-platform/frontend && python3 -m http.server 8080 &
```

### C. 性能调优参数

**Spark配置** (`$SPARK_HOME/conf/spark-defaults.conf`):
```properties
spark.executor.memory=4g
spark.executor.cores=4
spark.driver.memory=4g
spark.sql.shuffle.partitions=200
spark.default.parallelism=100
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

**HDFS配置** (`$HADOOP_HOME/etc/hadoop/hdfs-site.xml`):
```xml
<property>
    <name>dfs.replication</name>
    <value>3</value>
</property>
<property>
    <name>dfs.blocksize</name>
    <value>134217728</value> <!-- 128MB -->
</property>
```

---

## 📞 技术支持

**文档位置**: `/home/student/energy-platform/docs/`

**关键文档**:
- `PROJECT_COMPLETION_SUMMARY.md` - 项目完成总结
- `PROJECT_MEMO.md` - 项目备忘录
- `STAGE5_WORK_SUMMARY.md` - 后端API文档
- `STAGE6_WORK_SUMMARY.md` - 前端界面文档

**在线资源**:
- Apache Spark文档: https://spark.apache.org/docs/latest/
- Delta Lake文档: https://docs.delta.io/
- FastAPI文档: https://fastapi.tiangolo.com/

---

**文档版本**: 1.0  
**最后更新**: 2026年5月21日  
**维护者**: Renqin Wang
