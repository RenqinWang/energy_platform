# 能源站价格数据源分析

## 数据源信息

**API 地址**: `http://122.9.74.124:8000`

### 可用端点

1. **`/full`** - 获取全量价格数据
   - 返回格式: JSON 数组
   - 数据类型: 能源站供应价格数据

2. **`/`** - 获取 binlog（增量数据）
   - 状态: 端点不可用或路径不正确
   - 可能的替代路径: `/binlog`, `/incremental`, `/changes`

## 数据结构

### 完整数据样例

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

### 字段说明

| 字段 | 类型 | 说明 | 示例值 |
|------|------|------|--------|
| `id` | Integer | 记录唯一标识 | `1` |
| `station_code` | String | 能源站代码 | `ST001` |
| `price_type` | String | 价格类型 | `electricity`, `cooling`, `heating` |
| `price` | Float | 价格（元/kWh） | `0.8545` |
| `created_at` | DateTime | 创建时间 | `2026-04-12T13:37:58` |
| `updated_at` | DateTime | 更新时间 | `2026-05-21T00:00:01` |

## 数据内容

### 能源站列表

系统包含 **5 个能源站**：

| 站点代码 | 电价 (元/kWh) | 冷价 (元/kWh) | 热价 (元/kWh) |
|---------|--------------|--------------|--------------|
| ST001 | 0.8545 | 0.6189 | 0.5065 |
| ST002 | 0.8390 | 0.6196 | 0.4788 |
| ST003 | 0.8748 | 0.6544 | 0.5069 |
| ST004 | 0.8097 | 0.6068 | 0.4721 |
| ST005 | 0.8187 | 0.6205 | 0.4626 |

### 价格类型

1. **`electricity`** - 电价
   - 用途: 计算设备用电成本
   - 范围: 0.8097 ~ 0.8748 元/kWh
   - 平均: 0.8393 元/kWh

2. **`cooling`** - 冷价
   - 用途: 计算供冷收入
   - 范围: 0.6068 ~ 0.6544 元/kWh
   - 平均: 0.6240 元/kWh

3. **`heating`** - 热价
   - 用途: 计算供热收入
   - 范围: 0.4626 ~ 0.5069 元/kWh
   - 平均: 0.4854 元/kWh

## 数据特点

### 1. 全量数据 (`/full`)

- **总记录数**: 15 条 (5 站点 × 3 价格类型)
- **数据完整性**: 100%
- **更新频率**: 每日更新（根据 `updated_at` 字段）
- **最后更新**: 2026-05-21 00:00:01

### 2. 增量数据 (binlog)

- **状态**: 端点未找到
- **说明**: 可能需要特定的路径或认证
- **用途**: 用于实时同步价格变更

## 与项目的关系

### 当前使用情况

在项目的 **Silver 层** 中，我们已经创建了价格维表：

**表名**: `silver_price_dim`  
**路径**: `hdfs://node1:9000/lake/silver/silver_price_dim`  
**记录数**: 15 条

### 数据映射

| API 字段 | Silver 表字段 | 说明 |
|---------|--------------|------|
| `station_code` | `station_code` | 站点代码 |
| `price_type` | `price_type` | 价格类型 |
| `price` | `price` | 价格值 |
| `updated_at` | `effective_date` | 生效日期 |
| `created_at` | `created_at` | 创建时间 |
| `updated_at` | `updated_at` | 更新时间 |

### 在 Gold 层的应用

价格数据用于计算经济指标：

```python
# 能源成本 = 总能耗 × 电价
energy_cost = total_energy_consumption_kwh * electricity_price

# 供冷收入 = 总供冷量 × 冷价
cooling_revenue = total_cooling_supply_kwh * cooling_price

# 净利润 = 供冷收入 - 能源成本
net_profit = cooling_revenue - energy_cost
```

## 数据获取方式

### 方式 1: 全量同步

```bash
# 获取全量数据
curl http://122.9.74.124:8000/full > price_data.json

# 导入到 Bronze 层
# 通过 Flink/Spark 批处理任务
```

### 方式 2: 增量同步（如果 binlog 可用）

```bash
# 获取增量数据
curl http://122.9.74.124:8000/binlog > price_binlog.json

# 实时同步到 Bronze 层
# 通过 Flink CDC 或 Kafka Connect
```

### 方式 3: 定时轮询

```python
import requests
import schedule

def sync_price_data():
    response = requests.get('http://122.9.74.124:8000/full')
    data = response.json()
    # 写入 Bronze 层
    save_to_bronze(data)

# 每天凌晨同步一次
schedule.every().day.at("00:00").do(sync_price_data)
```

## 数据质量

### 优点

✅ **数据完整**: 所有站点都有三种价格类型  
✅ **结构规范**: JSON 格式，字段清晰  
✅ **时间戳完整**: 包含创建和更新时间  
✅ **数据稳定**: 价格变动不频繁

### 注意事项

⚠️ **历史数据**: API 只提供当前价格，不包含历史价格  
⚠️ **增量接口**: binlog 端点不可用，需要确认正确路径  
⚠️ **认证机制**: 未知是否需要 API Key 或 Token  
⚠️ **限流策略**: 未知请求频率限制

## 与 Kafka 传感器数据的区别

| 维度 | 价格数据 | 传感器数据 |
|------|---------|-----------|
| **数据源** | HTTP API | Kafka 消息队列 |
| **更新频率** | 每日 | 每 5 分钟 |
| **数据量** | 15 条 | 32,292 条 |
| **数据类型** | 维度数据 | 事实数据 |
| **变化频率** | 低 | 高 |
| **存储层级** | Silver 层 | Bronze → Silver → Gold |

## 建议

### 短期

1. **确认 binlog 端点**: 联系数据提供方确认增量数据接口
2. **实现定时同步**: 每日凌晨同步一次全量数据
3. **添加数据校验**: 检查价格合理性（不为负，不超过阈值）

### 长期

1. **实现增量同步**: 使用 binlog 实现实时价格更新
2. **历史价格管理**: 保留价格变更历史，支持时间旅行查询
3. **价格预测**: 基于历史数据预测未来价格趋势
4. **多版本管理**: 支持不同时间段的价格版本

---

**文档创建时间**: 2026-05-21  
**数据检查时间**: 2026-05-21 10:59  
**API 状态**: 正常（/full 端点可用）
