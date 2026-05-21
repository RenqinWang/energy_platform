# 数据完整性分析报告

## 问题描述

前端展示的日报表数据中，除了**平均温度**字段外，其他字段（总能耗、总供冷量、COP、净利润等）均显示为 `--`（缺省值）。

## 数据层级分析

### 1. 前端显示的是哪一层数据？

**答案：Gold 层 - 日报表数据 (`gold_report_daily`)**

- 表路径：`hdfs://node1:9000/lake/gold/gold_report_daily`
- 数据粒度：按日期、站点、设备聚合
- 总记录数：225 条
- 时间范围：2018-01-01 至 2018-03-15（约 75 天 × 3 台设备）

### 2. 数据完整性统计

#### Gold 层 - 日报表 (gold_report_daily)
```
总记录数: 225
✅ avg_supply_temp (平均温度): 225 条 (100%)
❌ total_energy_consumption_kwh (总能耗): 0 条 (0%)
❌ total_cooling_supply_kwh (总供冷量): 0 条 (0%)
❌ avg_cop (平均COP): 0 条 (0%)
❌ net_profit (净利润): 0 条 (0%)
```

#### Silver 层 - 冷机状态表 (silver_chiller_status)
```
总记录数: 32,292
✅ supply_temp (供水温度): 32,292 条 (100%)
❌ pressure (压力): 0 条 (0%)
❌ flow (流量): 0 条 (0%)
❌ power (功率): 0 条 (0%)
❌ run_flag (运行状态): 0 条 (0%)
```

## 根本原因

### Bronze 层数据源问题

**原始数据只包含温度传感器数据，缺少其他类型的传感器数据：**

1. ✅ **温度传感器** (theme=temperature) - 数据完整
2. ❌ **压力传感器** (theme=pressure) - 数据缺失
3. ❌ **流量传感器** (theme=flow) - 数据缺失
4. ❌ **功率传感器** (theme=power) - 数据缺失
5. ❌ **运行时长传感器** (theme=runtime) - 数据缺失
6. ❌ **启动次数传感器** (theme=count) - 数据缺失
7. ❌ **运行状态传感器** (theme=status) - 数据缺失

### 数据流向分析

```
Bronze Layer (bronze_sensor_raw)
    ↓ 只有 temperature 主题的数据
Silver Layer (silver_chiller_status)
    ↓ pressure, flow, power, run_flag 字段为 NULL
Gold Layer (gold_supply_curve_hourly)
    ↓ 无法计算能耗、供冷量
Gold Layer (gold_report_daily)
    ↓ 经济指标全部为 NULL
Frontend Display
    ↓ 只能显示温度数据
```

## 如何补充缺省数据

### 方案一：补充真实传感器数据（推荐）

**步骤：**

1. **配置 Kafka 生产者**，生成完整的传感器数据：
   ```python
   # 在 Kafka 生产者中添加其他传感器类型
   sensor_themes = [
       'temperature',  # 已有
       'pressure',     # 需要添加
       'flow',         # 需要添加
       'power',        # 需要添加
       'runtime',      # 需要添加
       'count',        # 需要添加
       'status'        # 需要添加
   ]
   ```

2. **更新点位元数据字典** (`silver_point_dim`)：
   - 添加压力、流量、功率等传感器的点位信息
   - 确保每个设备都有完整的传感器配置

3. **重新运行数据处理流程**：
   ```bash
   # 等待新数据流入 Bronze 层
   # 重新生成 Silver 层
   cd /home/student/energy-platform/data-processing/silver-layer
   bash generate_chiller_status.sh
   
   # 重新生成 Gold 层
   cd /home/student/energy-platform/data-processing/gold-layer
   bash generate_supply_curve.sh
   bash generate_daily_report.sh
   ```

### 方案二：使用模拟数据（测试用）

**创建模拟数据生成脚本：**

```python
# 基于现有温度数据，生成合理的模拟值
# - pressure: 基于温度推算（温度越低，压力越高）
# - flow: 随机生成在合理范围内（50-150 m³/h）
# - power: 基于设备型号设定（500-1500 kW）
# - runtime: 累计运行时长
# - run_flag: 基于功率判断（power > 0 则为 1）
```

**优点：**
- 快速验证系统功能
- 展示完整的数据流程

**缺点：**
- 不是真实数据
- 不能用于生产环境

### 方案三：从历史数据导入（如果有）

如果企业有历史数据文件（CSV、Excel、数据库等）：

1. 将历史数据转换为标准格式
2. 导入到 Bronze 层
3. 重新运行数据处理流程

## 当前系统状态

### ✅ 已正常工作的部分

1. **数据采集层**：Kafka 生产者正常运行
2. **数据存储层**：HDFS + Delta Lake 正常
3. **数据处理层**：PySpark 脚本逻辑正确
4. **API 服务层**：FastAPI 正常响应
5. **前端展示层**：页面正常加载

### ⚠️ 数据质量问题

- **不是代码问题**：所有脚本逻辑都是正确的
- **是数据源问题**：Bronze 层只有温度数据
- **系统设计正确**：当完整数据到来时，会自动计算所有指标

## 验证方法

### 检查 Bronze 层原始数据

```bash
hdfs dfs -cat hdfs://node1:9000/lake/bronze/bronze_sensor_raw/*.json | head -20
```

查看是否包含 pressure、flow、power 等主题的数据。

### 检查点位元数据

```bash
# 查看点位字典中是否定义了其他传感器
hdfs dfs -cat hdfs://node1:9000/lake/silver/silver_point_dim/*.parquet
```

## 结论

1. **前端显示的是 Gold 层日报表数据**
2. **数据缺失的根本原因是 Bronze 层只有温度传感器数据**
3. **补充方法：在 Kafka 生产者中添加其他传感器类型的数据**
4. **系统架构和代码逻辑都是正确的**，只需要补充数据源即可

## 建议

### 短期（演示用）
- 使用方案二：生成模拟数据，展示完整功能

### 长期（生产用）
- 使用方案一：接入真实传感器数据
- 确保所有设备的所有传感器都正常上报数据

---

**文档创建时间**: 2026-05-21  
**数据检查时间**: 2026-05-21 10:51  
**检查人员**: Claude (AI Assistant)
