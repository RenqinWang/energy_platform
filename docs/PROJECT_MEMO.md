# 智慧能源网监测平台项目备忘录

> 本文档记录项目实施过程中的关键配置、路径、命令和注意事项

## 📋 目录
- [集群配置](#集群配置)
- [软件路径](#软件路径)
- [Docker容器](#docker容器)
- [HDFS配置](#hdfs配置)
- [Kafka配置](#kafka配置)
- [Spark配置](#spark配置)
- [数据路径](#数据路径)
- [常用命令](#常用命令)
- [已知问题与解决方案](#已知问题与解决方案)
- [端口映射](#端口映射)

---

## 集群配置

### 节点信息

| 节点 | 公网IP | 内网IP | 角色 | 部署组件 |
|------|--------|--------|------|----------|
| **Node-1** (当前机器) | 115.120.208.241 | 192.168.0.94 | 入口+存储+计算 | HDFS NameNode, HDFS DataNode, Spark Worker, Backend API, React前端 |
| **Node-2** | 1.94.113.240 | 192.168.1.87 | 调度+存储+计算+消息 | Spark Master, HDFS DataNode, Spark Worker, Kafka Broker, 任务调度 |
| **Node-3** | 123.60.106.233 | 192.168.1.19 | 存储+计算+消息 | HDFS DataNode, Spark Worker, Kafka Broker |

### 主机名映射
```bash
# /etc/hosts 配置
192.168.0.94    node1
192.168.1.87    node2
192.168.1.19    node3
```

### 当前机器信息
- **主机名**: `ecs-new`
- **操作系统**: Ubuntu 22.04 LTS
- **内核版本**: Linux 5.15.0-179-generic
- **Java版本**: OpenJDK 17.0.18

---

## 软件路径

### Hadoop
- **安装路径**: `/home/student/hadoop-3.3.6/`
- **版本**: 3.3.6
- **hdfs命令**: `/home/student/hadoop-3.3.6/bin/hdfs`
- **配置目录**: `/home/student/hadoop-3.3.6/etc/hadoop/`

### Spark
- **安装路径**: `/home/student/spark-3.5.7-bin-hadoop3/`
- **版本**: 3.5.7
- **spark-submit**: `/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit`
- **配置目录**: `/home/student/spark-3.5.7-bin-hadoop3/conf/`
- **日志目录**: `/home/student/spark-3.5.7-bin-hadoop3/logs/`
- **工作目录**: `/home/student/spark-3.5.7-bin-hadoop3/work/`

### Java
- **JAVA_HOME**: `/usr/lib/jvm/java-17-openjdk-amd64`
- **java命令**: `/usr/bin/java`

### Python
- **python3**: `/usr/bin/python3`
- **注意**: 系统Python没有安装pyspark，必须使用spark-submit运行

### 项目路径
- **项目根目录**: `/home/student/energy-platform/`
- **数据目录**: `/home/student/作业2数据/`
- **点位字典文件**: `/home/student/作业2数据/pos.ttl`

---

## Docker容器

### Kafka生产者容器
- **容器名称**: `friendly_shockley`
- **镜像**: `sensor-streaming-kafka`
- **功能**: 推送传感器数据到Kafka
- **状态检查**: `docker ps | grep friendly_shockley`
- **启动命令**: `docker start friendly_shockley`
- **停止命令**: `docker stop friendly_shockley`
- **日志查看**: `docker logs friendly_shockley`

### 容器详细信息
```bash
# 查看容器详情
docker inspect friendly_shockley

# 查看容器统计信息
docker stats friendly_shockley

# 进入容器
docker exec -it friendly_shockley /bin/bash
```

### Kafka相关容器
- **Zookeeper**: 端口 2181
- **Kafka Broker 1**: 端口 9092 (Node-2)
- **Kafka Broker 2**: 端口 9092 (Node-3)

---

## HDFS配置

### NameNode配置
- **地址**: `hdfs://node1:9000`
- **Web UI**: `http://node1:9870` (需要内网访问)
- **RPC端口**: 9000

### DataNode配置
- **端口**: 9866 (数据传输), 9864 (HTTP)
- **节点**: Node-1, Node-2, Node-3

### 进程检查
```bash
# 检查HDFS进程
jps | grep -E "NameNode|DataNode|SecondaryNameNode"

# 输出示例:
# 6842 NameNode
# 7011 DataNode
# 7281 SecondaryNameNode
```

### HDFS命令使用
```bash
# 必须设置JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 使用完整URI访问HDFS
/home/student/hadoop-3.3.6/bin/hdfs dfs -ls hdfs://node1:9000/

# 查看集群状态
/home/student/hadoop-3.3.6/bin/hdfs dfsadmin -report
```

### 默认文件系统问题
⚠️ **注意**: 系统默认文件系统是 `file:///` (本地文件系统)，不是HDFS。必须使用完整的HDFS URI (`hdfs://node1:9000/`) 才能访问HDFS。

---

## Kafka配置

### Broker地址
- **Node-2**: `192.168.1.87:9092` 或 `node2:9092`
- **Node-3**: `192.168.1.19:9092` 或 `node3:9092`
- **本地访问**: `localhost:29092` (Docker端口映射)

### Topic命名规则
- **传感器数据**: `sensor_<sensor_id>` (例如: `sensor_10LDSCS_T`)
- **总数**: 158个传感器topics

### 连接问题解决
⚠️ **重要**: 在Node-1上连接Kafka时，使用 `localhost:29092` 而不是 `node2:9092`，避免主机名解析问题。

```python
# Spark Streaming 连接配置
.option("kafka.bootstrap.servers", "localhost:29092")
.option("subscribePattern", "sensor_.*")  # 订阅所有传感器topic
```

### Kafka消息格式
```json
{
  "sensor_id": "10LDSCS_T",
  "label": "2#站1#冷机冷冻水出水温度",
  "timestamp": "2018-02-01T00:04:47Z",
  "value": 8.8,
  "is_simulated": false,
  "push_time": "2026-04-21T09:26:34"
}
```

---

## Spark配置

### Spark Master
- **地址**: `spark://node2:7077`
- **Web UI**: `http://node2:8080` (需要内网访问)

### Spark Worker
- **节点**: Node-1, Node-2, Node-3
- **Web UI端口**: 8081

### 提交任务命令模板
```bash
# 本地模式 (单机测试)
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --master 'local[*]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  your_script.py

# 集群模式
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --master spark://node2:7077 \
  --deploy-mode cluster \
  --executor-memory 2g \
  --executor-cores 2 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  your_script.py
```

### Delta Lake配置
```python
spark = (
    SparkSession.builder
    .appName("Your_App_Name")
    .master("local[*]")  # 或 "spark://node2:7077"
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
```

### Kafka + Delta Lake 依赖
```bash
--packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7
```

---

## 数据路径

### HDFS数据湖结构
```
hdfs://node1:9000/
├── lake/
│   ├── bronze/
│   │   ├── bronze_sensor_raw/        # 传感器原始数据 (约35MB)
│   │   │   ├── dt=2018-01-01/
│   │   │   ├── dt=2018-01-02/
│   │   │   └── ...
│   │   └── bronze_price_raw/         # 价格原始数据 (约10KB)
│   │       └── price_year_month=202605/
│   ├── silver/
│   │   └── silver_point_meta_dim/    # 点位字典 (约50KB)
│   └── gold/
│       └── (待创建)
├── checkpoints/
│   └── kafka_to_bronze/              # Kafka Streaming checkpoint
├── spark-logs/                       # Spark事件日志
├── test/
└── tmp/
```

### 本地项目结构
```
/home/student/energy-platform/
├── data-ingestion/
│   ├── kafka-streaming/
│   │   ├── streaming_to_bronze.py
│   │   ├── streaming_to_bronze_test.py
│   │   └── quick_test.py
│   ├── price-sync/
│   │   └── sync_price_data.py
│   └── dict-loader/
│       └── parse_pos_ttl.py
├── data-processing/
│   ├── bronze-layer/
│   ├── silver-layer/
│   │   ├── generate_point_fact.py
│   │   ├── generate_chiller_status.py
│   │   └── generate_price_dim.py
│   └── gold-layer/
│       ├── generate_supply_curve.py
│       └── generate_daily_report.py
├── backend/                          # 阶段5: FastAPI后端
│   ├── main.py
│   ├── data_access.py
│   ├── config.py
│   ├── requirements.txt
│   ├── start_api.sh
│   ├── test_api.py
│   └── README.md
├── frontend/                         # 阶段6: Web前端
│   ├── index.html
│   ├── css/
│   │   └── style.css
│   ├── js/
│   │   ├── config.js
│   │   ├── api.js
│   │   ├── charts.js
│   │   ├── table.js
│   │   └── main.js
│   └── README.md
├── config/
├── scripts/
│   ├── verify_stage2.py
│   ├── verify_stage3.py
│   └── verify_stage4.py
└── docs/
    ├── implementation-plan.md
    ├── STAGE2_WORK_SUMMARY.md
    ├── STAGE3_WORK_SUMMARY.md
    ├── STAGE4_WORK_SUMMARY.md
    ├── STAGE5_WORK_SUMMARY.md
    ├── STAGE6_WORK_SUMMARY.md
    └── PROJECT_MEMO.md
```

---

## 常用命令

### 环境变量设置
```bash
# 每次使用HDFS命令前需要设置
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 或者添加到 ~/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

### HDFS常用命令
```bash
# 列出目录
/home/student/hadoop-3.3.6/bin/hdfs dfs -ls hdfs://node1:9000/lake/

# 查看文件大小
/home/student/hadoop-3.3.6/bin/hdfs dfs -du -h hdfs://node1:9000/lake/bronze/

# 查看文件内容
/home/student/hadoop-3.3.6/bin/hdfs dfs -cat hdfs://node1:9000/lake/bronze/bronze_price_raw/price_year_month=202605/*.parquet | head

# 删除目录
/home/student/hadoop-3.3.6/bin/hdfs dfs -rm -r hdfs://node1:9000/lake/test/

# 创建目录
/home/student/hadoop-3.3.6/bin/hdfs dfs -mkdir -p hdfs://node1:9000/lake/gold/
```

### Spark任务提交
```bash
# 进入项目目录
cd /home/student/energy-platform/

# 运行价格同步
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --master 'local[*]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  data-ingestion/price-sync/sync_price_data.py

# 运行点位字典解析
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --master 'local[*]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  data-ingestion/dict-loader/parse_pos_ttl.py

# 运行验证脚本
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --master 'local[*]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  scripts/verify_stage2.py
```

### Docker命令
```bash
# 查看所有容器
docker ps -a

# 启动Kafka生产者
docker start friendly_shockley

# 查看容器日志
docker logs -f friendly_shockley

# 查看容器统计
docker stats friendly_shockley
```

### 进程检查
```bash
# 检查Java进程
jps

# 检查HDFS进程
jps | grep -E "NameNode|DataNode"

# 检查Spark进程
jps | grep -E "Master|Worker"

# 检查端口占用
netstat -tuln | grep -E "9000|9870|7077|8080|9092"
```

---

## 已知问题与解决方案

### 问题1: spark-submit命令找不到
**现象**: `command not found: spark-submit`

**原因**: spark-submit不在系统PATH中

**解决方案**: 使用完整路径
```bash
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit
```

---

### 问题2: HDFS命令报错 "JAVA_HOME is not set"
**现象**: `ERROR: JAVA_HOME is not set and could not be found.`

**原因**: HDFS脚本需要JAVA_HOME环境变量

**解决方案**: 
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

---

### 问题3: HDFS访问本地文件系统
**现象**: `hdfs dfs -ls /lake/` 显示的是本地文件系统，不是HDFS

**原因**: 默认文件系统配置为 `file:///`

**解决方案**: 使用完整的HDFS URI
```bash
/home/student/hadoop-3.3.6/bin/hdfs dfs -ls hdfs://node1:9000/lake/
```

---

### 问题4: Kafka连接超时
**现象**: Spark Streaming连接Kafka时超时

**原因**: 主机名解析问题或网络不通

**解决方案**: 
- 在Node-1上使用 `localhost:29092`
- 在其他节点使用内网IP: `192.168.1.87:9092`

---

### 问题5: pyspark模块找不到
**现象**: `ModuleNotFoundError: No module named 'pyspark'`

**原因**: 系统Python没有安装pyspark包

**解决方案**: 必须使用spark-submit运行，不能直接用python3
```bash
# 错误方式
python3 your_script.py

# 正确方式
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit your_script.py
```

---

### 问题6: Delta Lake包找不到
**现象**: `java.lang.ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension`

**原因**: 没有加载Delta Lake依赖包

**解决方案**: 使用 `--packages` 参数
```bash
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 your_script.py
```

---

### 问题7: Spark任务中星号需要转义
**现象**: `no matches found: local[*]`

**原因**: Shell会展开星号

**解决方案**: 使用引号
```bash
--master 'local[*]'  # 正确
--master local[*]    # 错误
```

---

### 问题8: friendly_shockley容器未启动
**现象**: Kafka没有数据流入

**原因**: 容器停止运行

**解决方案**: 
```bash
# 检查容器状态
docker ps -a | grep friendly_shockley

# 启动容器
docker start friendly_shockley

# 查看日志确认
docker logs friendly_shockley
```

---

## 端口映射

### HDFS端口
| 服务 | 端口 | 说明 |
|------|------|------|
| NameNode RPC | 9000 | HDFS客户端连接端口 |
| NameNode Web UI | 9870 | HDFS管理界面 |
| DataNode 数据传输 | 9866 | DataNode间数据传输 |
| DataNode HTTP | 9864 | DataNode Web界面 |
| SecondaryNameNode HTTP | 9868 | SecondaryNameNode Web界面 |

### Spark端口
| 服务 | 端口 | 说明 |
|------|------|------|
| Spark Master | 7077 | Spark集群通信端口 |
| Spark Master Web UI | 8080 | Spark Master管理界面 |
| Spark Worker Web UI | 8081 | Spark Worker管理界面 |
| Spark Application UI | 4040 | 应用程序监控界面 |

### Kafka端口
| 服务 | 端口 | 说明 |
|------|------|------|
| Kafka Broker | 9092 | Kafka客户端连接端口 |
| Kafka (Docker映射) | 29092 | 本地访问Kafka端口 |
| Zookeeper | 2181 | Zookeeper客户端端口 |

### 应用端口
| 服务 | 端口 | 说明 |
|------|------|------|
| Backend API | 8000 | FastAPI后端服务 (已实现) |
| Frontend | 8080 | Web前端界面 (已实现) |

---

## 数据源API

### 价格数据API
- **全量接口**: `http://122.9.74.124:8000/full`
- **增量接口**: `http://122.9.74.124:8000/` (返回404，暂不可用)
- **数据格式**: JSON数组
- **更新频率**: 每日更新
- **字段**: id, station_code, price_type, price, created_at, updated_at

### 测试命令
```bash
# 获取全量价格数据
curl -s http://122.9.74.124:8000/full | jq .

# 查看前5条
curl -s http://122.9.74.124:8000/full | jq '.[:5]'
```

---

## 时区配置

### 统一时区策略
- **存储时区**: UTC (所有时间字段统一使用UTC)
- **Spark配置**: `spark.sql.session.timeZone = "UTC"`
- **前端展示**: Asia/Shanghai (由API层转换)

### 时间字段说明
- `event_time`: 业务发生时间 (UTC)
- `ingest_time`: 数据入湖时间 (UTC)
- `created_at`: 记录创建时间 (UTC)
- `updated_at`: 记录更新时间 (UTC)

---

## 性能优化建议

### Delta Lake优化
```bash
# 每日凌晨2:00执行OPTIMIZE
OPTIMIZE bronze_sensor_raw ZORDER BY (station_id, event_time)

# 每周执行VACUUM清理历史版本
VACUUM bronze_sensor_raw RETAIN 168 HOURS
```

### Spark配置优化
```python
.config("spark.executor.memory", "2g")
.config("spark.executor.cores", "2")
.config("spark.driver.memory", "2g")
.config("spark.sql.shuffle.partitions", "200")
```

---

## 安全注意事项

⚠️ **重要提醒**:
1. 数据来源为企业私有，**不要上传到公网**
2. Git仓库不提交任何原始数据样本
3. `.gitignore` 已排除 `data/`、`*.csv`、`*.json`
4. 安全组只对组内成员IP开放端口
5. Web UI端口 (9870, 8080) 不对外开放

---

## 快速参考

### 一键启动Kafka生产者
```bash
docker start friendly_shockley && docker logs -f friendly_shockley
```

### 一键验证阶段3数据
```bash
cd /home/student/energy-platform && \
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --master 'local[*]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  scripts/verify_stage3.py
```

### 一键验证阶段4数据
```bash
cd /home/student/energy-platform && \
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --master 'local[*]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  scripts/verify_stage4.py
```

### 一键启动后端API服务
```bash
cd /home/student/energy-platform/backend && ./start_api.sh
```

### 一键启动前端服务
```bash
cd /home/student/energy-platform/frontend && python3 -m http.server 8080
```

### 一键查看HDFS数据湖
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && \
/home/student/hadoop-3.3.6/bin/hdfs dfs -ls -R hdfs://node1:9000/lake/
```

---

## 更新日志

| 日期 | 更新内容 | 更新人 |
|------|----------|--------|
| 2026-05-20 | 创建备忘录，记录阶段2完成时的配置 | Claude |
| 2026-05-20 | 添加阶段3 Silver层数据治理配置 | Claude |
| 2026-05-20 | 添加阶段4 Gold层数据分析配置 | Claude |
| 2026-05-20 | 添加阶段5 FastAPI后端服务配置 | Claude |
| 2026-05-20 | 添加阶段6 Web前端界面配置 | Claude |

---

## 联系方式

如有问题，请参考以下文档：
- 系统设计方案: `/home/student/files_form_group/第一阶段设计方案_v2.md`
- 实现计划: `/home/student/energy-platform/docs/implementation-plan.md`
- 阶段2总结: `/home/student/energy-platform/docs/stage2_completion_summary.md`

---

**最后更新**: 2026年5月20日
