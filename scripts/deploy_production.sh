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
SPARK_MASTER="spark://192.168.1.87:7077"
SPARK_MASTER_HOST="192.168.1.87"
NODE1_IP="192.168.0.94"
KAFKA_BOOTSTRAP="192.168.1.87:9092,192.168.1.19:9092"

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

    log_success "HDFS集群启动完成"
}

#############################################
# 步骤3: 启动Spark集群
#############################################
step3_start_spark() {
    print_header "步骤3: 启动Spark集群"

    log_info "检查Spark Master进程..."
    if ssh student@node2 "jps | grep -q Master" 2>/dev/null; then
        log_warning "Spark Master已在运行"
    else
        log_info "在Node-2上启动Spark Master..."
        ssh student@node2 "export SPARK_LOCAL_IP=192.168.1.87 && export SPARK_MASTER_HOST=192.168.1.87 && export SPARK_HOME=$SPARK_HOME && \$SPARK_HOME/sbin/stop-master.sh >/dev/null 2>&1 || true; \$SPARK_HOME/sbin/start-master.sh --host 192.168.1.87 --port 7077 --webui-port 8080" 2>/dev/null || log_warning "无法SSH到node2，跳过"
        sleep 5
    fi

    log_success "Spark集群启动完成"
}

#############################################
# 步骤4: 启动Kafka生产者
#############################################
step4_start_kafka() {
    print_header "步骤4: 启动Kafka生产者"

    log_info "启动Node2/Node3 Kafka realtime path..."
    bash "$PROJECT_HOME/scripts/start-kafka-realtime.sh"

    log_success "Kafka生产者启动完成"
}

#############################################
# 步骤5: 启动后端API服务
#############################################
step5_start_backend() {
    print_header "步骤5: 启动后端API服务"

    log_info "检查后端API进程..."
    if pgrep -f "uvicorn main:app" > /dev/null; then
        log_warning "后端API已在运行"
        log_info "停止现有进程..."
        pkill -f "uvicorn main:app"
        sleep 3
    fi

    cd $PROJECT_HOME/backend

    log_info "激活Conda base环境..."
    eval "$(conda shell.bash hook)"
    conda activate base

    log_info "检查依赖是否已安装..."
    if python -c "import fastapi, uvicorn, pyspark" 2>/dev/null; then
        log_success "依赖已安装，跳过安装步骤"
    else
        log_warning "首次部署需要下载Python依赖包（约420MB）"
        log_info "这可能需要5-10分钟，取决于网络速度..."
        log_info "提示: 使用清华镜像加速下载"

        log_info "开始安装依赖..."
        # 尝试使用清华镜像加速
        pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt || \
        pip install -r requirements.txt

        log_success "依赖安装完成"
    fi

    log_info "后台启动API服务..."
    nohup uvicorn main:app --host 0.0.0.0 --port 8001 > /tmp/backend_api.log 2>&1 &

    sleep 5

    log_info "验证API服务..."
    if curl -s http://localhost:8001/health | grep -q "healthy"; then
        log_success "后端API服务启动成功"
        log_info "API文档: http://localhost:8001/docs"
    else
        log_error "后端API服务启动失败"
        log_error "查看日志: tail -f /tmp/backend_api.log"
        exit 1
    fi

    log_success "后端API服务启动完成"
}

#############################################
# 步骤6: 启动前端服务
#############################################
step6_start_frontend() {
    print_header "步骤6: 启动前端服务"

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
# 步骤7: 打印部署信息
#############################################
step7_print_summary() {
    print_header "部署完成"

    echo ""
    echo "🎉 智慧能源网监测平台部署成功！"
    echo ""
    echo "📊 服务访问地址:"
    echo "  - 前端界面: http://${NODE1_IP}:8080"
    echo "  - API文档:  http://${NODE1_IP}:8001/docs"
    echo "  - API健康检查: http://${NODE1_IP}:8001/health"
    echo ""
    echo "🔧 管理界面:"
    echo "  - HDFS Web UI: http://node1:9870"
    echo "  - Spark Master UI: http://192.168.1.87:8080"
    echo "  - Kafka UI: http://node2:8083"
    echo ""
    echo "📝 日志文件:"
    echo "  - 后端API: /tmp/backend_api.log"
    echo "  - 前端服务: /tmp/frontend.log"
    echo ""
    echo "🛠️ 常用命令:"
    echo "  - 停止所有服务: $PROJECT_HOME/scripts/stop_all.sh"
    echo "  - 查看API日志: tail -f /tmp/backend_api.log"
    echo "  - 查看前端日志: tail -f /tmp/frontend.log"
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
    step5_start_backend
    step6_start_frontend
    step7_print_summary

    echo ""
    echo "结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
}

# 运行主函数
main "$@"
