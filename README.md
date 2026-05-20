# 智慧能源网监测平台

基于分布式数据分析的能源站监测与分析平台

## 项目概述

本项目是一个完整的分布式能源数据分析平台，实现从数据采集到可视化展示的完整链路。

**技术栈**：Kafka + Spark + Delta Lake + HDFS + FastAPI + React + ECharts

**业务范围**：单站点（北站）、跨系统协同监测，重点实现冷机系统

## 系统架构

### 三节点部署架构

- **Node-1 (192.168.0.94) - 入口+存储+计算**
  - HDFS NameNode
  - HDFS DataNode
  - Spark Worker
  - Backend API
  - React 前端

- **Node-2 (192.168.1.87) - 调度+存储+计算+消息**
  - Spark Master
  - HDFS DataNode
  - Spark Worker
  - Kafka Broker
  - 任务调度

- **Node-3 (192.168.1.19) - 存储+计算+消息**
  - HDFS DataNode
  - Spark Worker
  - Kafka Broker

### 数据湖分层

- **Bronze 层**：原始数据（`bronze_sensor_raw`, `bronze_price_raw`）
- **Silver 层**：标准化数据（`silver_point_fact`, `silver_chiller_status`, `silver_price_dim`）
- **Gold 层**：应用层结果（报表、预测、建议）

## 目录结构

```
energy-platform/
├── data-ingestion/          # 数据采集模块
│   ├── kafka-streaming/     # Kafka 流式接入
│   ├── price-sync/          # 价格数据同步
│   └── dict-loader/         # 点位字典加载
├── data-processing/         # 数据处理模块
│   ├── bronze-layer/        # Bronze 层入湖
│   ├── silver-layer/        # Silver 层标准化
│   └── gold-layer/          # Gold 层报表与预测
├── backend-api/             # 后端 API 服务
│   └── routers/             # API 路由
├── frontend/                # 前端展示
│   └── src/pages/           # React 页面
├── config/                  # 配置文件
├── scripts/                 # 运维脚本
│   ├── benchmark/           # 性能测试
│   ├── maintenance/         # 维护脚本
│   └── deployment/          # 部署脚本
└── docs/                    # 文档
    └── vibe-context/        # Vibe coding 上下文
```

## 快速开始

### 1. 启动集群

```bash
# 启动 HDFS（在 Node1）
$HADOOP_HOME/sbin/start-dfs.sh

# 启动 Spark（在 Node2）
ssh student@node2 "$SPARK_HOME/sbin/start-all.sh"

# 启动 Kafka（在 Node1）
docker-compose start
```

### 2. 验证集群状态

```bash
# 检查 HDFS
hdfs dfsadmin -report

# 检查 Spark
# 访问 http://node2:8080

# 检查 Kafka
# 访问 http://node1:8083
```

### 3. 停止集群

```bash
# 停止 Spark（在 Node2）
ssh student@node2 "$SPARK_HOME/sbin/stop-all.sh"

# 停止 HDFS（在 Node1）
$HADOOP_HOME/sbin/stop-dfs.sh

# 停止 Kafka（在 Node1）
docker-compose stop
```

## 开发指南

详见 `docs/vibe-context/` 目录下的文档。

## 实现计划

详见 `/home/student/.claude/plans/purrfect-spinning-candy.md`

## 团队成员

（待补充）

## 许可证

本项目仅用于课程作业，不得用于商业用途。
