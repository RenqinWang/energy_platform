# 能源站价格数据 API 完整文档

## API 基本信息

**基础 URL**: `http://122.9.74.124:8000`  
**API 类型**: FastAPI (RESTful)  
**文档地址**: http://122.9.74.124:8000/docs  
**数据源**: MySQL 数据库 (`energy_prices.prices` 表)

---

## API 端点列表

### 1. 获取全量数据 - `/full`

**方法**: `GET`  
**描述**: 获取价格表的全量数据，支持分页

**请求参数**:
| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `limit` | integer | 否 | 500 | 返回记录数量 |
| `offset` | integer | 否 | 0 | 偏移量（分页） |

**请求示例**:
```bash
# 获取全部数据
curl http://122.9.74.124:8000/full

# 分页获取（每页10条，第2页）
curl "http://122.9.74.124:8000/full?limit=10&offset=10"
```

**响应示例**:
```json
[
  {
    "id": 1,
    "station_code": "ST001",
    "price_type": "electricity",
    "price": 0.8545,
    "created_at": "2026-04-12T13:37:58",
    "updated_at": "2026-05-21T00:00:01"
  }
]
```

---

### 2. 获取记录总数 - `/count`

**方法**: `GET`  
**描述**: 返回价格表的总记录数

**请求示例**:
```bash
curl http://122.9.74.124:8000/count
```

**响应示例**:
```json
{
  "cnt": 15
}
```

---

### 3. 获取 Binlog 状态 - `/list_binlog_status`

**方法**: `GET`  
**描述**: 返回当前 MySQL master binlog 状态（文件名和位置）

**请求示例**:
```bash
curl http://122.9.74.124:8000/list_binlog_status
```

**响应示例**:
```json
{
  "File": "mysql-bin.000004",
  "Position": 58150,
  "Binlog_Do_DB": "",
  "Binlog_Ignore_DB": "",
  "Executed_Gtid_Set": ""
}
```

**字段说明**:
- `File`: 当前 binlog 文件名
- `Position`: 当前 binlog 位置
- `Binlog_Do_DB`: 需要记录 binlog 的数据库
- `Binlog_Ignore_DB`: 忽略 binlog 的数据库
- `Executed_Gtid_Set`: 已执行的 GTID 集合

---

### 4. 列出 Binlog 文件 - `/list_binlog_files`

**方法**: `GET`  
**描述**: 列出服务器上所有 binary log 文件

**请求示例**:
```bash
curl http://122.9.74.124:8000/list_binlog_files
```

**响应示例**:
```json
[
  {
    "Log_name": "mysql-bin.000001",
    "File_size": 180,
    "Encrypted": "No"
  },
  {
    "Log_name": "mysql-bin.000002",
    "File_size": 2997942,
    "Encrypted": "No"
  },
  {
    "Log_name": "mysql-bin.000003",
    "File_size": 4360,
    "Encrypted": "No"
  },
  {
    "Log_name": "mysql-bin.000004",
    "File_size": 58150,
    "Encrypted": "No"
  }
]
```

---

### 5. 获取全部 Binlog 事件 - `/binlog_all`

**方法**: `GET`  
**描述**: 流式返回目标数据库的全部 binlog 事件（JSONL 格式，每行一个 JSON 对象）

**请求示例**:
```bash
curl http://122.9.74.124:8000/binlog_all
```

**响应格式**: JSONL (每行一个 JSON 对象)

**事件类型**:

#### 1. **WriteRowsEvent** - 插入数据事件
```json
{
  "type": "WriteRowsEvent",
  "schema": "energy_prices",
  "table": "prices",
  "log_file": "mysql-bin.000003",
  "log_pos": 2819,
  "timestamp": 1776001078,
  "rows": [
    {
      "values": {
        "UNKNOWN_COL0": 1,
        "UNKNOWN_COL1": "ST001",
        "UNKNOWN_COL2": "electricity",
        "UNKNOWN_COL3": "0.8545",
        "UNKNOWN_COL4": "2026-04-12 13:37:58",
        "UNKNOWN_COL5": "2026-04-12 13:37:58"
      }
    }
  ]
}
```

**字段映射**:
- `UNKNOWN_COL0` → `id`
- `UNKNOWN_COL1` → `station_code`
- `UNKNOWN_COL2` → `price_type`
- `UNKNOWN_COL3` → `price`
- `UNKNOWN_COL4` → `created_at`
- `UNKNOWN_COL5` → `updated_at`

#### 2. **UpdateRowsEvent** - 更新数据事件
```json
{
  "type": "UpdateRowsEvent",
  "schema": "energy_prices",
  "table": "prices",
  "log_file": "mysql-bin.000004",
  "log_pos": 1613,
  "timestamp": 1776044294,
  "rows": [
    {
      "before_values": {
        "UNKNOWN_COL0": 1,
        "UNKNOWN_COL1": "ST001",
        "UNKNOWN_COL2": "electricity",
        "UNKNOWN_COL3": "0.6900",
        "UNKNOWN_COL4": "2026-04-12 13:37:58",
        "UNKNOWN_COL5": "2026-04-12 13:39:10"
      },
      "after_values": {
        "UNKNOWN_COL0": 1,
        "UNKNOWN_COL1": "ST001",
        "UNKNOWN_COL2": "electricity",
        "UNKNOWN_COL3": "0.7957",
        "UNKNOWN_COL4": "2026-04-12 13:37:58",
        "UNKNOWN_COL5": "2026-04-13 01:38:14"
      }
    }
  ]
}
```

#### 3. **其他事件类型**
- `RotateEvent`: Binlog 文件轮转
- `FormatDescriptionEvent`: Binlog 格式描述
- `QueryEvent`: DDL 语句（CREATE, ALTER 等）
- `XidEvent`: 事务提交

---

## 数据库表结构

### `energy_prices.prices` 表

```sql
CREATE TABLE IF NOT EXISTS `prices` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `station_code` VARCHAR(64) NOT NULL,    -- 站点代码
  `price_type` VARCHAR(32) NOT NULL,      -- 价格类型
  `price` DECIMAL(12,4) NOT NULL,         -- 价格值
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  UNIQUE KEY `uniq_station_type_from` (`station_code`, `price_type`),
  INDEX `idx_station_type_from` (`station_code`, `price_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
```

**字段说明**:
| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `id` | BIGINT | 主键ID | 1 |
| `station_code` | VARCHAR(64) | 站点代码 | ST001 |
| `price_type` | VARCHAR(32) | 价格类型 | electricity, cooling, heating |
| `price` | DECIMAL(12,4) | 价格（元/kWh） | 0.8545 |
| `created_at` | TIMESTAMP | 创建时间 | 2026-04-12 13:37:58 |
| `updated_at` | TIMESTAMP | 更新时间 | 2026-05-21 00:00:01 |

---

## 数据内容

### 当前数据统计

- **总记录数**: 15 条
- **站点数量**: 5 个 (ST001 ~ ST005)
- **价格类型**: 3 种 (electricity, cooling, heating)
- **更新频率**: 每日更新（凌晨 00:00）

### 价格数据表

| 站点 | 电价 (元/kWh) | 冷价 (元/kWh) | 热价 (元/kWh) |
|------|--------------|--------------|--------------|
| ST001 | 0.8545 | 0.6189 | 0.5065 |
| ST002 | 0.8390 | 0.6196 | 0.4788 |
| ST003 | 0.8748 | 0.6544 | 0.5069 |
| ST004 | 0.8097 | 0.6068 | 0.4721 |
| ST005 | 0.8187 | 0.6205 | 0.4626 |

---

## 使用场景

### 场景 1: 全量同步（初始化）

```python
import requests

# 获取全量数据
response = requests.get('http://122.9.74.124:8000/full')
prices = response.json()

# 导入到数据湖 Bronze 层
for record in prices:
    save_to_bronze(record)
```

### 场景 2: 增量同步（CDC）

```python
import requests
import json

# 获取当前 binlog 位置
status = requests.get('http://122.9.74.124:8000/list_binlog_status').json()
print(f"当前位置: {status['File']}:{status['Position']}")

# 流式读取 binlog 事件
response = requests.get('http://122.9.74.124:8000/binlog_all', stream=True)

for line in response.iter_lines():
    if line:
        event = json.loads(line)
        
        # 处理插入事件
        if event['type'] == 'WriteRowsEvent':
            for row in event['rows']:
                insert_to_bronze(row['values'])
        
        # 处理更新事件
        elif event['type'] == 'UpdateRowsEvent':
            for row in event['rows']:
                update_in_bronze(row['after_values'])
```

### 场景 3: 定时轮询

```python
import requests
import schedule
import time

def sync_prices():
    """每日凌晨同步价格数据"""
    response = requests.get('http://122.9.74.124:8000/full')
    prices = response.json()
    
    # 更新 Silver 层价格维表
    update_price_dim(prices)
    print(f"同步完成: {len(prices)} 条记录")

# 每天凌晨 1 点执行
schedule.every().day.at("01:00").do(sync_prices)

while True:
    schedule.run_pending()
    time.sleep(60)
```

---

## 与项目集成

### 当前集成状态

✅ **已集成**: 价格数据已导入到 Silver 层  
📍 **表名**: `silver_price_dim`  
📂 **路径**: `hdfs://node1:9000/lake/silver/silver_price_dim`  
📊 **记录数**: 15 条

### 数据流向

```
MySQL (energy_prices.prices)
    ↓ HTTP API
Bronze Layer (bronze_price_raw)
    ↓ PySpark ETL
Silver Layer (silver_price_dim)
    ↓ Join with Gold Layer
Gold Layer (gold_report_daily)
    ↓ 计算经济指标
    - energy_cost = total_energy_consumption_kwh × electricity_price
    - cooling_revenue = total_cooling_supply_kwh × cooling_price
    - net_profit = cooling_revenue - energy_cost
```

### 建议的同步策略

**方案 1: 定时全量同步**（当前使用）
- 频率: 每日凌晨 1 点
- 方式: 调用 `/full` 端点
- 优点: 简单可靠
- 缺点: 无法实时更新

**方案 2: Binlog 增量同步**（推荐）
- 频率: 实时
- 方式: 监听 `/binlog_all` 端点
- 优点: 实时性高，资源占用少
- 缺点: 实现复杂度较高

**方案 3: 混合模式**
- 初始化: 使用 `/full` 全量同步
- 日常: 使用 `/binlog_all` 增量同步
- 容灾: 定期全量校验

---

## 注意事项

### 1. Binlog 字段映射

⚠️ Binlog 事件中的字段名为 `UNKNOWN_COL0` ~ `UNKNOWN_COL5`，需要手动映射：

```python
COLUMN_MAPPING = {
    'UNKNOWN_COL0': 'id',
    'UNKNOWN_COL1': 'station_code',
    'UNKNOWN_COL2': 'price_type',
    'UNKNOWN_COL3': 'price',
    'UNKNOWN_COL4': 'created_at',
    'UNKNOWN_COL5': 'updated_at'
}

def map_columns(row):
    return {COLUMN_MAPPING[k]: v for k, v in row.items()}
```

### 2. 数据类型转换

⚠️ Binlog 中的 `price` 字段为字符串，需要转换为浮点数：

```python
price_value = float(row['UNKNOWN_COL3'])
```

### 3. 时间戳处理

⚠️ Binlog 的 `timestamp` 字段为 Unix 时间戳（秒），需要转换：

```python
from datetime import datetime
event_time = datetime.fromtimestamp(event['timestamp'])
```

### 4. 流式读取

⚠️ `/binlog_all` 返回的是流式数据（JSONL），需要逐行解析：

```python
for line in response.iter_lines():
    if line:
        event = json.loads(line)
        process_event(event)
```

---

## 错误处理

### 常见错误

**1. 422 Validation Error**
```json
{
  "detail": [
    {
      "loc": ["query", "limit"],
      "msg": "value is not a valid integer",
      "type": "type_error.integer"
    }
  ]
}
```
**解决**: 检查参数类型是否正确

**2. 404 Not Found**
```json
{
  "detail": "Not Found"
}
```
**解决**: 检查 URL 路径是否正确

---

## 总结

### API 特点

✅ **完整性**: 提供全量和增量两种同步方式  
✅ **实时性**: Binlog 支持实时数据变更捕获  
✅ **可靠性**: 基于 MySQL binlog，数据不丢失  
✅ **易用性**: RESTful API，支持 Swagger 文档

### 适用场景

- ✅ 数据湖初始化（全量同步）
- ✅ 实时数据同步（CDC）
- ✅ 数据变更审计
- ✅ 价格历史追踪

---

**文档版本**: v1.0  
**创建时间**: 2026-05-21  
**最后更新**: 2026-05-21  
**API 状态**: 正常运行
