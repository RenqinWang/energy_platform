# 智慧能源网监测平台

基于 Kafka、Spark、Delta Lake、HDFS、FastAPI 和 React 的分布式能源数据分析平台。

最终提交和答辩口径请先阅读：

- [最终统一说明文档](docs/FINAL_SYSTEM_DESCRIPTION.md)
- [作业要求对照与待完善清单](docs/ASSIGNMENT2_GAP_ANALYSIS.md)
- [数据完整性分析](docs/DATA_COMPLETENESS_ANALYSIS.md)
- [分布式有效性验证](docs/distributed_effectiveness_validation.md)
- [前端数据展示说明](docs/frontend-data-display-guide.md)

## 当前范围

平台覆盖冷机、热机、冷热电三联供三类系统，支持：

- Kafka 模拟流式采集和 Bronze 入湖
- full/stream 两套 Delta Lake 结果命名空间
- Silver 点位标准化、价格维表和冷机状态宽表
- Gold 系统小时表、日/周/月综合报表、预测、收益预测和运行建议
- FastAPI 查询接口
- React 前端页面：系统级展示、设备级查询、主题级查询、综合报表、供能预测、收益预测

## 三节点部署

| 节点 | 内网 IP | 角色 | 组件 |
|---|---|---|---|
| Node-1 | `192.168.0.94` | 入口、存储、计算 | HDFS NameNode/DataNode、Spark Worker、FastAPI、React 前端 |
| Node-2 | `192.168.1.87` | 调度、存储、计算、消息 | Spark Master、HDFS DataNode、Spark Worker、Kafka Broker |
| Node-3 | `192.168.1.19` | 存储、计算、消息 | HDFS DataNode、Spark Worker、Kafka Broker |

核心地址：

- HDFS：`hdfs://node1:9000`
- Spark：`spark://node2:7077`
- Kafka：`192.168.1.87:9092,192.168.1.19:9092`
- 前端：`http://115.120.208.241:3001`
- full 后端：`http://115.120.208.241:8001`
- stream 后端：`http://115.120.208.241:8002`

## 当前目录结构

```text
energy-platform/
├── backend/                    # FastAPI 后端
├── frontend-new/               # React + Vite 前端
├── data-ingestion/             # Kafka、价格、点位字典接入
├── data-processing/
│   ├── silver-layer/           # Silver 层治理脚本
│   ├── gold-layer/             # Gold 报表、预测、建议脚本
│   ├── full/                   # 全量模式说明
│   └── stream/                 # 5 分钟微批增量脚本
├── scripts/                    # 当前运维、启动、验证脚本
│   └── archive/                # 早期旧脚本归档
└── docs/
    ├── FINAL_SYSTEM_DESCRIPTION.md
    ├── ASSIGNMENT2_GAP_ANALYSIS.md
    └── archive/                # 阶段性旧文档归档
```

## 常用命令

```bash
cd /home/student/energy-platform

# full 后端
./scripts/start-backend-mode.sh full

# stream 后端
./scripts/start-backend-mode.sh stream

# Kafka -> stream Bronze
./scripts/start-kafka-to-bronze.sh

# 5 分钟微批
./scripts/run-stream-microbatch.sh loop

# 切到只处理 Kafka 最新数据
./scripts/reset-stream-to-latest.sh
```

前端开发服务：

```bash
cd /home/student/energy-platform/frontend-new
npm run dev -- --host 0.0.0.0 --port 3001
```

## 文档清理说明

阶段性报告、早期实施计划和旧脚本已移入：

- `docs/archive/`
- `scripts/archive/`

这些文件仅用于追溯，不作为最终提交或答辩口径。
