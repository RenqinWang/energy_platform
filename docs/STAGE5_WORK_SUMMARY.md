# 阶段5工作总结 - 后端API服务实现

## 概述

本阶段实现了基于FastAPI的RESTful API服务，用于查询Delta Lake中的能源平台数据。API服务使用PySpark作为数据访问层，支持多维度数据查询和过滤。

**实施时间**: 2024年（阶段5）  
**技术栈**: FastAPI 0.104.1, PySpark 3.5.0, Delta Lake 3.0.0, Uvicorn  
**数据源**: HDFS Delta Lake (hdfs://node1:9000/lake)

---

## 一、架构设计

### 1.1 整体架构

```
┌─────────────────┐
│   Frontend      │
│  (HTML/JS)      │
└────────┬────────┘
         │ HTTP/REST
         ▼
┌─────────────────┐
│   FastAPI       │
│   Backend       │
└────────┬────────┘
         │ PySpark
         ▼
┌─────────────────┐
│  Data Access    │
│     Layer       │
└────────┬────────┘
         │ Delta Lake API
         ▼
┌─────────────────┐
│   HDFS          │
│  Delta Lake     │
│  (Silver/Gold)  │
└─────────────────┘
```

### 1.2 模块划分

- **main.py**: FastAPI应用主文件，定义API端点和路由
- **data_access.py**: 数据访问层，使用PySpark查询Delta Lake表
- **config.py**: 配置文件，包含Spark、HDFS、API配置
- **start_api.sh**: 服务启动脚本
- **test_api.py**: API测试套件

---

## 二、API端点设计

### 2.1 健康检查端点

#### `GET /` 和 `GET /health`

返回API服务健康状态。

**响应示例**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "version": "1.0.0"
}
```

### 2.2 元数据端点

#### `GET /api/stations`

获取所有站点ID列表。

**响应示例**:
```json
{
  "stations": ["STATION_001", "STATION_002", "STATION_003"]
}
```

#### `GET /api/equipment?station_id={id}`

获取设备ID列表，可按站点过滤。

**查询参数**:
- `station_id` (可选): 站点ID

**响应示例**:
```json
{
  "equipment": ["LJ001", "LJ002", "LJ003"]
}
```

### 2.3 数据查询端点

#### `GET /api/supply-curve`

查询小时级供能曲线数据（gold_supply_curve_hourly表）。

**查询参数**:
- `station_id` (可选): 站点ID
- `equipment_id` (可选): 设备ID
- `start_date` (可选): 开始日期 (YYYY-MM-DD)
- `end_date` (可选): 结束日期 (YYYY-MM-DD)
- `limit` (可选): 最大记录数 (默认1000, 最大10000)

**响应字段**:
- `station_id`: 站点标识
- `equipment_id`: 设备标识
- `stat_hour`: 统计小时 (YYYY-MM-DD HH:00:00)
- `avg_supply_temp`: 平均供水温度 (°C)
- `max_supply_temp`: 最高供水温度 (°C)
- `min_supply_temp`: 最低供水温度 (°C)
- `avg_power`: 平均功率 (kW)
- `run_minutes`: 运行分钟数
- `energy_consumption_kwh`: 能耗 (kWh)
- `cooling_capacity_kw`: 制冷能力 (kW)
- `cooling_supply_kwh`: 供冷量 (kWh)
- `operation_rate`: 运行率 (%)
- `record_count`: 聚合的分钟级记录数

**响应示例**:
```json
[
  {
    "station_id": "STATION_001",
    "equipment_id": "LJ001",
    "stat_hour": "2024-01-15 10:00:00",
    "avg_supply_temp": 7.2,
    "max_supply_temp": 7.5,
    "min_supply_temp": 6.9,
    "avg_power": 450.5,
    "run_minutes": 60,
    "energy_consumption_kwh": 450.5,
    "cooling_capacity_kw": 1351.5,
    "cooling_supply_kwh": 1351.5,
    "operation_rate": 100.0,
    "record_count": 60,
    "dt": "2024-01-15"
  }
]
```

#### `GET /api/daily-report`

查询日报表数据（gold_report_daily表）。

**查询参数**:
- `station_id` (可选): 站点ID
- `equipment_id` (可选): 设备ID
- `start_date` (可选): 开始日期 (YYYY-MM-DD)
- `end_date` (可选): 结束日期 (YYYY-MM-DD)
- `limit` (可选): 最大记录数 (默认1000, 最大10000)

**响应字段**:
- `station_id`: 站点标识
- `equipment_id`: 设备标识
- `stat_date`: 统计日期 (YYYY-MM-DD)
- `avg_supply_temp`: 平均供水温度 (°C)
- `total_energy_consumption_kwh`: 总能耗 (kWh)
- `total_cooling_supply_kwh`: 总供冷量 (kWh)
- `total_run_minutes`: 总运行分钟数
- `daily_operation_rate`: 日运行率 (%)
- `avg_cop`: 平均COP
- `energy_cost`: 能源成本 (元)
- `cooling_revenue`: 供冷收入 (元)
- `net_profit`: 净利润 (元)
- `hour_count`: 聚合的小时级记录数

**响应示例**:
```json
[
  {
    "station_id": "STATION_001",
    "equipment_id": "LJ001",
    "stat_date": "2024-01-15",
    "avg_supply_temp": 7.1,
    "total_energy_consumption_kwh": 10812.0,
    "total_cooling_supply_kwh": 32436.0,
    "total_run_minutes": 1440,
    "daily_operation_rate": 100.0,
    "avg_cop": 3.0,
    "energy_cost": 9160.15,
    "cooling_revenue": 20602.24,
    "net_profit": 11442.09,
    "hour_count": 24,
    "dt": "2024-01-15"
  }
]
```

#### `GET /api/equipment-status`

查询设备状态数据（silver_chiller_status表，分钟级）。

**查询参数**:
- `station_id` (可选): 站点ID
- `equipment_id` (可选): 设备ID
- `start_time` (可选): 开始时间 (YYYY-MM-DD HH:mm:ss)
- `end_time` (可选): 结束时间 (YYYY-MM-DD HH:mm:ss)
- `limit` (可选): 最大记录数 (默认1000, 最大10000)

**响应字段**:
- `station_id`: 站点标识
- `equipment_id`: 设备标识
- `stat_time`: 统计时间 (YYYY-MM-DD HH:mm:ss)
- `supply_temp`: 供水温度 (°C)
- `pressure`: 压力 (kPa)
- `flow`: 流量 (m³/h)
- `power`: 功率 (kW)
- `runtime_hours`: 累计运行小时数
- `start_count`: 累计启动次数
- `run_flag`: 运行状态 (1=运行, 0=停止)
- `record_count`: 聚合的原始记录数

---

## 三、技术实现

### 3.1 数据访问层设计

使用单例模式管理Spark会话，避免重复创建连接：

```python
class DataAccessLayer:
    _instance = None
    _spark = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataAccessLayer, cls).__new__(cls)
        return cls._instance

    def _initialize_spark(self):
        self._spark = SparkSession.builder \
            .appName("EnergyPlatformAPI") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE) \
            .getOrCreate()
```

### 3.2 查询优化

**过滤下推**: 在Spark层面应用过滤条件，减少数据传输：

```python
def query_daily_report(self, station_id=None, equipment_id=None, 
                       start_date=None, end_date=None, limit=1000):
    df = self._spark.read.format("delta").load(config.GOLD_DAILY_REPORT_PATH)
    
    # Apply filters at Spark level
    if station_id:
        df = df.filter(col("station_id") == station_id)
    if equipment_id:
        df = df.filter(col("equipment_id") == equipment_id)
    if start_date:
        df = df.filter(col("stat_date") >= start_date)
    if end_date:
        df = df.filter(col("stat_date") <= end_date)
    
    # Order and limit
    df = df.orderBy(col("stat_date").desc()).limit(limit)
    
    return [row.asDict() for row in df.collect()]
```

### 3.3 CORS配置

支持跨域请求，允许前端访问：

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:8080",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

### 3.4 错误处理

使用HTTPException返回标准HTTP错误响应：

```python
@app.get("/api/daily-report")
async def get_daily_report(...):
    try:
        data = dal.query_daily_report(...)
        return data
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to query daily report: {str(e)}"
        )
```

---

## 四、部署和运行

### 4.1 环境要求

- Python 3.8+
- Java 17 (JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64)
- Apache Spark 3.5.7
- HDFS集群访问权限

### 4.2 依赖安装

```bash
cd /home/student/energy-platform/backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**requirements.txt内容**:
```
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.0
pyspark==3.5.0
delta-spark==3.0.0
python-multipart==0.0.6
```

### 4.3 启动服务

使用启动脚本：

```bash
./start_api.sh
```

或手动启动：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/home/student/spark-3.5.7-bin-hadoop3
source venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

服务启动后访问：
- API服务: http://localhost:8000
- Swagger文档: http://localhost:8000/docs
- ReDoc文档: http://localhost:8000/redoc

### 4.4 测试验证

运行测试套件：

```bash
./test_api.py
```

测试内容包括：
1. 健康检查
2. 获取站点列表
3. 获取设备列表
4. 查询供能曲线
5. 查询日报表
6. 查询设备状态

---

## 五、性能考虑

### 5.1 连接池管理

- 使用单例Spark会话，避免重复创建连接
- Spark会话在应用启动时初始化，关闭时释放

### 5.2 查询限制

- 默认查询限制1000条记录
- 最大查询限制10000条记录
- 建议使用日期过滤减少数据量

### 5.3 日志级别

- Spark日志级别设置为WARN，减少控制台输出
- 保留ERROR和WARN级别日志用于故障排查

---

## 六、API文档

### 6.1 Swagger UI

访问 http://localhost:8000/docs 查看交互式API文档。

Swagger UI提供：
- 所有端点的详细说明
- 请求参数和响应格式
- 在线测试功能
- 示例请求和响应

### 6.2 ReDoc

访问 http://localhost:8000/redoc 查看美化的API文档。

ReDoc提供：
- 清晰的文档结构
- 详细的数据模型说明
- 更好的阅读体验

---

## 七、已知限制

### 7.1 数据完整性

- 当前Bronze层只有温度数据
- pressure、flow、power等字段在设备状态表中为NULL
- 待完整数据接入后自动填充

### 7.2 并发性能

- 当前使用单个Spark会话处理所有请求
- 高并发场景下可能需要连接池优化
- 建议生产环境使用负载均衡

### 7.3 安全性

- 当前版本无身份认证
- 无访问控制和权限管理
- 生产环境需添加认证机制（JWT、OAuth等）

---

## 八、后续优化建议

### 8.1 性能优化

1. **缓存机制**: 对频繁查询的数据添加Redis缓存
2. **异步处理**: 使用异步Spark查询提高并发性能
3. **分页优化**: 实现游标分页，支持大数据集查询

### 8.2 功能增强

1. **聚合API**: 提供预聚合的统计数据API
2. **实时数据**: 支持流式数据查询
3. **数据导出**: 支持CSV、Excel等格式导出

### 8.3 安全加固

1. **身份认证**: 添加JWT或OAuth2认证
2. **访问控制**: 实现基于角色的访问控制（RBAC）
3. **审计日志**: 记录所有API访问日志

### 8.4 监控告警

1. **性能监控**: 集成Prometheus监控API性能
2. **健康检查**: 增强健康检查，包含依赖服务状态
3. **告警机制**: 异常情况自动告警

---

## 九、文件清单

```
backend/
├── main.py              # FastAPI应用主文件 (250行)
├── data_access.py       # 数据访问层 (180行)
├── config.py            # 配置文件 (40行)
├── requirements.txt     # Python依赖 (7行)
├── start_api.sh         # 启动脚本 (25行)
├── test_api.py          # 测试套件 (250行)
└── README.md            # 文档 (400行)
```

**总代码量**: 约1150行

---

## 十、总结

阶段5成功实现了完整的RESTful API服务，具备以下特点：

✅ **功能完整**: 支持所有核心数据查询需求  
✅ **架构清晰**: 分层设计，职责明确  
✅ **性能良好**: 使用Spark分布式查询，支持大数据量  
✅ **文档完善**: Swagger自动生成API文档  
✅ **易于部署**: 提供启动脚本和测试工具  
✅ **可扩展性**: 模块化设计，易于添加新功能  

API服务为前端提供了稳定的数据接口，完成了数据仓库到应用层的最后一环。

---

**文档版本**: 1.0  
**最后更新**: 2024年  
**作者**: Renqin Wang
