# 价格数据采集模块

## 概述

本模块负责从外部 HTTP API 获取能源站价格数据，并保存到 Delta Lake Bronze 层。

## 目录结构

```
data-ingestion/
├── ingest_price_data.py      # 价格数据采集脚本（Python）
└── run_price_ingestion.sh    # 启动脚本（Bash）
```

## 数据流向

```
HTTP API (122.9.74.124:8000/full)
    ↓
Python 脚本获取数据
    ↓
PySpark 处理和转换
    ↓
Delta Lake Bronze 层 (hdfs://node1:9000/lake/bronze/bronze_price_raw)
```

## 使用方法

### 方式 1: 使用启动脚本（推荐）

```bash
cd /home/student/energy-platform/data-ingestion
bash run_price_ingestion.sh
```

### 方式 2: 直接运行 Python 脚本

```bash
cd /home/student/energy-platform/data-ingestion

/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --master "local[*]" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  ingest_price_data.py
```

## 数据结构

### 输入数据（API 响应）

```json
{
  "id": 1,
  "station_code": "ST001",
  "price_type": "electricity",
  "price": 0.8545,
  "created_at": "2026-04-12T13:37:58",
  "updated_at": "2026-05-21T00:00:01"
}
```

### 输出数据（Bronze 层）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | Long | 记录ID |
| `station_code` | String | 站点代码 |
| `price_type` | String | 价格类型（electricity/cooling/heating） |
| `price` | Double | 价格值（元/kWh） |
| `created_at` | String | 创建时间 |
| `updated_at` | String | 更新时间 |
| `ingest_time` | Timestamp | 数据入湖时间 |
| `source` | String | 数据源标识（http_api） |
| `api_endpoint` | String | API 端点地址 |
| `dt` | Date | 分区字段（日期） |

## 执行结果

### 成功示例

```
============================================================
价格数据采集任务开始
============================================================
开始时间: 2026-05-21 11:47:50

正在从 http://122.9.74.124:8000/full 获取价格数据...
成功获取 15 条价格记录
正在保存数据到 Bronze 层: hdfs://node1:9000/lake/bronze/bronze_price_raw

数据样例:
+---+------------+-----------+------+-------------------+
|id |station_code|price_type |price |updated_at         |
+---+------------+-----------+------+-------------------+
|1  |ST001       |electricity|0.8545|2026-05-21T00:00:01|
|2  |ST001       |cooling    |0.6189|2026-05-21T00:00:01|
...

✅ 成功保存 15 条记录到 Bronze 层

正在验证 Bronze 层数据...
总记录数: 15

按站点和价格类型统计:
+------------+-----------+-----+
|station_code| price_type|count|
+------------+-----------+-----+
|       ST001|    cooling|    1|
|       ST001|electricity|    1|
|       ST001|    heating|    1|
...

============================================================
✅ 价格数据采集任务完成
============================================================
结束时间: 2026-05-21 11:48:12
```

## 数据验证

### 检查 HDFS 中的数据

```bash
# 查看目录结构
hdfs dfs -ls hdfs://node1:9000/lake/bronze/bronze_price_raw/

# 查看分区
hdfs dfs -ls hdfs://node1:9000/lake/bronze/bronze_price_raw/dt=2026-05-21/
```

### 使用 PySpark 查询数据

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QueryPriceData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 读取 Bronze 层数据
df = spark.read.format("delta").load("hdfs://node1:9000/lake/bronze/bronze_price_raw")

# 显示数据
df.show()

# 统计
df.groupBy("station_code", "price_type").count().show()
```

## 定时任务配置

### 使用 Cron 定时执行

```bash
# 编辑 crontab
crontab -e

# 添加定时任务（每天凌晨 1 点执行）
0 1 * * * /home/student/energy-platform/data-ingestion/run_price_ingestion.sh >> /tmp/price_ingestion.log 2>&1
```

### 使用 Airflow DAG

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'price_data_ingestion',
    default_args=default_args,
    description='采集价格数据到 Bronze 层',
    schedule_interval='0 1 * * *',  # 每天凌晨 1 点
    catchup=False,
)

ingest_task = BashOperator(
    task_id='ingest_price_data',
    bash_command='/home/student/energy-platform/data-ingestion/run_price_ingestion.sh',
    dag=dag,
)
```

## 故障排查

### 问题 1: 无法连接到 API

**错误信息**:
```
获取数据失败: HTTPConnectionPool(host='122.9.74.124', port=8000): Max retries exceeded
```

**解决方法**:
1. 检查网络连接
2. 确认 API 服务是否正常运行
3. 检查防火墙设置

### 问题 2: HDFS 连接失败

**错误信息**:
```
java.net.ConnectException: Call From node1/192.168.0.94 to node1:9000 failed
```

**解决方法**:
1. 检查 HDFS 服务是否启动: `jps | grep NameNode`
2. 启动 HDFS: `$HADOOP_HOME/sbin/start-dfs.sh`

### 问题 3: Spark 依赖包下载失败

**错误信息**:
```
:: problems summary ::
:::: WARNINGS
        module not found: io.delta#delta-spark_2.12;3.2.0
```

**解决方法**:
1. 检查网络连接
2. 使用国内镜像加速
3. 手动下载 JAR 包到 `~/.ivy2/jars/`

## 性能优化

### 1. 批量处理

如果数据量大，可以使用分页获取：

```python
def fetch_price_data_batch(limit=100, offset=0):
    url = f"{API_BASE_URL}/full?limit={limit}&offset={offset}"
    response = requests.get(url)
    return response.json()
```

### 2. 并行处理

使用 Spark 的并行能力处理大量数据：

```python
df.repartition(10).write.format("delta").save(BRONZE_PATH)
```

### 3. 增量更新

使用 Delta Lake 的 MERGE 功能实现增量更新：

```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, BRONZE_PATH)

deltaTable.alias("target").merge(
    new_data.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

## 监控和告警

### 日志记录

脚本会输出详细的执行日志，包括：
- 数据获取状态
- 保存进度
- 验证结果

### 告警配置

可以在脚本中添加告警逻辑：

```python
def send_alert(message):
    # 发送邮件、钉钉、企业微信等
    pass

try:
    main()
except Exception as e:
    send_alert(f"价格数据采集失败: {e}")
    raise
```

## 当前状态

数据接入已经接入当前 full/stream 数据湖流程。最终口径以 `docs/FINAL_SYSTEM_DESCRIPTION.md` 为准。

## 相关文档

- [最终统一说明文档](../docs/FINAL_SYSTEM_DESCRIPTION.md)
- [数据完整性分析](../docs/DATA_COMPLETENESS_ANALYSIS.md)
- [项目备忘录](../docs/PROJECT_MEMO.md)

---

**最后更新**: 2026-05-21  
**维护人员**: Claude AI Assistant
