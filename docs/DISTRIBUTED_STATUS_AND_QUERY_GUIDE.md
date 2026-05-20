# 分布式实现现状与消息查询指南

## 📊 当前分布式实现情况

### 1. 完全分布式的组件 ✅

#### HDFS存储层
- **状态**: 完全分布式运行
- **配置**: 
  - NameNode: Node-1 (192.168.0.94)
  - DataNode: Node-1, Node-2, Node-3
  - 副本数: 3（默认）
- **验证命令**:
  ```bash
  jps | grep -E "NameNode|DataNode"
  # 输出: NameNode, DataNode, SecondaryNameNode
  ```

#### Kafka消息队列
- **状态**: 分布式运行
- **配置**:
  - Broker 1: Node-2 (192.168.1.87:9092)
  - Broker 2: Node-3 (192.168.1.19:9092)
  - Zookeeper: Node-2
- **生产者**: friendly_shockley容器
- **Topic数量**: 158个传感器topics

### 2. 部分分布式的组件 ⚠️

#### Spark集群
- **集群状态**: 已搭建
  - Spark Master: Node-2 (spark://node2:7077)
  - Spark Worker: Node-1, Node-2, Node-3
- **实际使用情况**:
  - ✅ Kafka Streaming: 使用集群模式 `spark://node2:7077`
  - ❌ 价格同步: 使用本地模式 `local[*]`
  - ❌ 点位字典解析: 使用本地模式 `local[*]`
  - ❌ 验证脚本: 使用本地模式 `local[*]`

### 3. 分布式程度总结

| 层级 | 组件 | 分布式状态 | 说明 |
|------|------|-----------|------|
| **存储层** | HDFS | ✅ 完全分布式 | 数据分布在3个节点 |
| **消息层** | Kafka | ✅ 完全分布式 | 2个Broker节点 |
| **计算层** | Spark | ⚠️ 部分分布式 | 集群已搭建，但大部分任务用local模式 |
| **应用层** | Backend/Frontend | ❌ 未部署 | 待实现 |

---

## 🔄 如何升级为完全分布式

### 方案1: 修改现有脚本使用集群模式

将所有脚本的 `.master("local[*]")` 改为 `.master("spark://node2:7077")`

**优点**: 充分利用集群资源
**缺点**: 小任务可能反而变慢（网络开销）

### 方案2: 按任务大小选择模式

- **小任务** (点位字典解析、验证脚本): 保持 `local[*]`
- **大任务** (数据治理、报表生成、预测): 使用 `spark://node2:7077`

**推荐**: 方案2，根据数据量灵活选择

---

## 🔍 如何查询具体的Kafka消息

### 方法1: 使用Spark SQL查询（推荐）

我已经为你创建了查询脚本: `/home/student/energy-platform/scripts/query_messages.py`

**运行方式**:
```bash
cd /home/student/energy-platform/scripts
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit \
  --master 'local[*]' \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  query_messages.py
```

**查询示例**:

#### 1. 查看特定传感器的消息
```python
df.filter(col("sensor_id") == "10LDSCS_T") \
  .orderBy(col("event_time").desc()) \
  .show(10)
```

#### 2. 查看特定日期的消息
```python
df.filter(col("dt") == "2018-01-01") \
  .orderBy("event_time") \
  .show(10)
```

#### 3. 查看特定时间范围
```python
df.filter((col("event_time") >= "2018-01-01 00:00:00") &
          (col("event_time") <= "2018-01-01 01:00:00")) \
  .show(20)
```

#### 4. 查看完整消息结构
```python
df.limit(1).show(1, truncate=False, vertical=True)
```

**输出示例**:
```
-RECORD 0--------------------------------
 sensor_id    | 3LDSCS_T                 
 label        | 2#站8#冷机冷冻水出水温度 
 event_time   | 2018-01-04 00:03:23      
 value        | 14.6                     
 is_simulated | false                    
 push_time    | 2026-05-20 12:16:30      
 source_topic | sensor_3LDSCS_T          
 partition_id | 0                        
 offset_id    | 768                      
 ingest_time  | 2026-05-20 20:31:32.047  
 dt           | 2018-01-04               
```

---

### 方法2: 使用pyspark交互式Shell

```bash
# 启动pyspark shell
/home/student/spark-3.5.7-bin-hadoop3/bin/pyspark \
  --master 'local[*]' \
  --packages io.delta:delta-spark_2.12:3.2.0

# 在shell中执行
df = spark.read.format("delta").load("hdfs://node1:9000/lake/bronze/bronze_sensor_raw")
df.show(10)
df.printSchema()
df.count()
```

---

### 方法3: 使用spark-sql命令行

```bash
# 启动spark-sql
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-sql \
  --master 'local[*]' \
  --packages io.delta:delta-spark_2.12:3.2.0

# 在SQL中查询
CREATE OR REPLACE TEMPORARY VIEW sensor_data
USING delta
LOCATION 'hdfs://node1:9000/lake/bronze/bronze_sensor_raw';

SELECT * FROM sensor_data LIMIT 10;
SELECT sensor_id, COUNT(*) FROM sensor_data GROUP BY sensor_id;
```

---

### 方法4: 直接查询Kafka（实时消息）

如果想查看Kafka中的实时消息（而非已入湖的历史消息）:

```bash
# 需要先进入Kafka容器或使用kafka-console-consumer

# 查看所有topics
docker exec -it <kafka-container-id> kafka-topics --list --bootstrap-server localhost:9092

# 消费特定topic的消息
docker exec -it <kafka-container-id> kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor_10LDSCS_T \
  --from-beginning \
  --max-messages 10
```

---

## 📊 消息字段说明

### Bronze层传感器数据字段

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `sensor_id` | String | 传感器ID | "3LDSCS_T" |
| `label` | String | 传感器标签（中文名称） | "2#站8#冷机冷冻水出水温度" |
| `event_time` | Timestamp | 业务发生时间（UTC） | "2018-01-04 00:03:23" |
| `value` | Double | 传感器读数 | 14.6 |
| `is_simulated` | Boolean | 是否为模拟数据 | false |
| `push_time` | Timestamp | 推送到Kafka的时间 | "2026-05-20 12:16:30" |
| `source_topic` | String | 来源Kafka topic | "sensor_3LDSCS_T" |
| `partition_id` | Integer | Kafka分区ID | 0 |
| `offset_id` | Long | Kafka偏移量 | 768 |
| `ingest_time` | Timestamp | 入湖时间（UTC） | "2026-05-20 20:31:32.047" |
| `dt` | Date | 分区字段（日期） | "2018-01-04" |

---

## 🎯 常见查询场景

### 场景1: 查找某个传感器的异常值

```python
# 查找温度超过20度的记录
df.filter((col("sensor_id") == "3LDSCS_T") & (col("value") > 20)) \
  .orderBy("event_time") \
  .show()
```

### 场景2: 统计每天的消息数量

```python
df.groupBy("dt").count().orderBy("dt").show()
```

### 场景3: 查看数据延迟情况

```python
from pyspark.sql.functions import unix_timestamp

df.withColumn("delay_seconds", 
              unix_timestamp("ingest_time") - unix_timestamp("push_time")) \
  .select("sensor_id", "event_time", "delay_seconds") \
  .orderBy(col("delay_seconds").desc()) \
  .show(10)
```

### 场景4: 查看Kafka元数据

```python
# 查看每个topic的消息数量
df.groupBy("source_topic").count().orderBy("count", ascending=False).show()

# 查看每个分区的消息数量
df.groupBy("source_topic", "partition_id").count().show()

# 查看offset范围
df.groupBy("source_topic") \
  .agg({"offset_id": "min", "offset_id": "max"}) \
  .show()
```

---

## 💡 性能优化建议

### 1. 使用分区过滤
```python
# 好的做法：利用分区字段
df.filter(col("dt") == "2018-01-01").show()

# 不好的做法：不使用分区字段
df.filter(col("event_time").like("2018-01-01%")).show()
```

### 2. 选择必要的列
```python
# 好的做法：只选择需要的列
df.select("sensor_id", "event_time", "value").show()

# 不好的做法：选择所有列
df.show()
```

### 3. 使用缓存
```python
# 如果要多次查询同一个数据集
df_filtered = df.filter(col("dt") == "2018-01-01")
df_filtered.cache()

# 后续查询会更快
df_filtered.count()
df_filtered.show()
```

---

## 🔧 自定义查询脚本模板

```python
#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 创建Spark Session
spark = (
    SparkSession.builder
    .appName("Custom_Query")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .getOrCreate()
)

# 读取数据
df = spark.read.format("delta").load("hdfs://node1:9000/lake/bronze/bronze_sensor_raw")

# 你的查询逻辑
# ...

spark.stop()
```

---

## 📝 总结

### 当前数据查询方式
1. ✅ **Bronze层**: 通过Delta Lake查询（推荐）
2. ✅ **实时Kafka**: 通过kafka-console-consumer
3. ✅ **交互式**: 通过pyspark shell
4. ✅ **SQL**: 通过spark-sql命令行

### 分布式升级建议
1. **短期**: 保持现状，重点完成功能实现
2. **中期**: 将数据治理和报表任务改为集群模式
3. **长期**: 所有批处理任务使用集群模式，实现完全分布式

### 下一步行动
- [ ] 继续实现阶段3（数据治理）
- [ ] 在阶段5（分布式验证）时对比 local vs cluster 性能
- [ ] 根据性能对比结果决定是否全面升级为集群模式

---

**最后更新**: 2026年5月20日
