# 数据指标来源与计算方法说明

**文档版本**: 1.0  
**更新时间**: 2026-05-21

---

## 📊 原始数据来源

### 1. 点位事实表 (silver_point_fact)

冷机系统的原始数据来自158个传感器点位，按主题（theme）分类：

| 主题 | 记录数 | 说明 | 单位 | 示例点位 |
|------|--------|------|------|----------|
| **temperature** | 1,604,214 | 温度数据 | ℃ | 2#站7#冷机冷冻水出水温度 |
| **status** | 812,456 | 运行状态 | bool | 2#站10#冷机运行 (0/1) |
| **flow** | 444,178 | 流量数据 | m³/h | 2#站10#冷机流量 |
| **count** | 368,438 | 启动次数（累计） | times | 2#站1#冷机启动次数 |
| **pressure** | 182,171 | 压力数据 | MPa | 2#站3#发电机油压 |
| **other** | 364,350 | 其他数据 | - | 电池电压等 |

**注意**: 
- ❌ **没有功率（power）数据** - 原始数据中不包含功率传感器
- ❌ **没有回水温度（return_temp）数据** - 只有出水温度
- ❌ **没有运行时长（runtime）数据** - 只有启动次数

---

## 🔧 Silver层指标（冷机状态表）

### silver_chiller_status 表结构

从点位事实表按**1分钟**聚合生成，每条记录代表某台冷机在某一分钟的状态。

| 字段 | 数据来源 | 计算方法 | 说明 |
|------|----------|----------|------|
| **supply_temp** | theme='temperature' | `MAX(value)` | 供水温度（℃） |
| **return_temp** | ❌ 无数据 | `NULL` | 回水温度（当前数据中缺失） |
| **pressure** | theme='pressure' | `MAX(value)` | 压力（MPa） |
| **flow** | theme='flow' | `MAX(value)` | 流量（m³/h） |
| **power** | ❌ 无数据 | `NULL` | 功率（kW，当前数据中缺失） |
| **runtime_hours** | theme='count' | `MAX(value)` | 累计运行时长（小时，实际是启动次数） |
| **start_count** | theme='count' | `MAX(value)` | 累计启动次数 |
| **run_flag** | theme='status' | `MAX(value)` | 运行状态（0=停机，1=运行） |
| **record_count** | - | `COUNT(*)` | 该分钟内的记录数（数据质量指标） |

**数据聚合逻辑**:
```python
# 按 station_id, equipment_id, stat_time(分钟) 分组
df_status = df_chiller.groupBy("station_id", "equipment_id", "stat_time", "dt").agg(
    spark_max(when(col("theme") == "temperature", col("value"))).alias("supply_temp"),
    spark_max(when(col("theme") == "pressure", col("value"))).alias("pressure"),
    spark_max(when(col("theme") == "flow", col("value"))).alias("flow"),
    spark_max(when(col("theme") == "power", col("value"))).alias("power"),  # 实际为NULL
    spark_max(when(col("theme") == "runtime", col("value"))).alias("runtime_hours"),  # 实际为NULL
    spark_max(when(col("theme") == "count", col("value"))).alias("start_count"),
    spark_max(when(col("theme") == "status", col("value"))).alias("run_flag"),
    count("*").alias("record_count")
)
```

---

## 🏆 Gold层指标（供冷曲线表）

### gold_supply_curve_hourly 表结构

从Silver层按**小时**聚合生成，每条记录代表某台冷机在某一小时的运行情况。

### 1. 平均功率 (avg_power)

**数据来源**: ❌ **当前无数据**

**计算方法**:
```python
avg(col("power")).alias("avg_power")  # 结果为 NULL
```

**说明**: 
- 原始数据中没有功率传感器
- 需要从设备铭牌或能耗表获取功率数据
- 或者通过电表数据计算：功率 = 电流 × 电压 × 功率因数

**如何获取**:
1. **硬件方案**: 安装功率表或电能表
2. **软件方案**: 根据设备型号查询额定功率
3. **估算方案**: 根据制冷量和COP反推（不准确）

---

### 2. 能耗 (energy_consumption_kwh)

**数据来源**: 依赖 `avg_power` 和 `run_minutes`

**计算方法**:
```python
energy_consumption_kwh = avg_power × run_minutes ÷ 60
```

**说明**:
- 单位: kWh（千瓦时）
- 公式: 能耗 = 平均功率(kW) × 运行时间(小时)
- **当前状态**: 因为 `avg_power` 为 NULL，所以 `energy_consumption_kwh` 也为 NULL

**示例计算**:
```
假设: avg_power = 500 kW, run_minutes = 45分钟
能耗 = 500 × (45 ÷ 60) = 500 × 0.75 = 375 kWh
```

---

### 3. 供冷量 (cooling_supply_kwh)

**数据来源**: 依赖 `supply_temp`, `return_temp`, `flow`

**理论计算公式**:
```python
# 制冷量（kW）= 1.163 × 流量(m³/h) × 温差(℃)
cooling_capacity_kw = 1.163 × flow × (return_temp - supply_temp)

# 供冷量（kWh）= 制冷量(kW) × 运行时间(小时)
cooling_supply_kwh = cooling_capacity_kw × run_minutes ÷ 60
```

**物理原理**:
- Q = c × m × ΔT
  - c: 水的比热容 = 4.2 kJ/(kg·℃)
  - m: 质量流量 = 体积流量 × 密度 ÷ 3600
  - ΔT: 温差 = return_temp - supply_temp
- 简化系数: 1.163 = 4.2 × 1000 ÷ 3600

**当前实现**（估算方案）:
```python
# 因为缺少 return_temp，使用功率和COP估算
cooling_capacity_kw = avg_power × 3.0  # 假设COP=3
cooling_supply_kwh = cooling_capacity_kw × run_minutes ÷ 60
```

**当前状态**: 因为 `avg_power` 为 NULL，所以 `cooling_supply_kwh` 也为 NULL

**如何获取准确数据**:
1. **最佳方案**: 安装回水温度传感器
2. **次优方案**: 使用设备铭牌的额定制冷量
3. **估算方案**: 根据功率和COP估算（当前方案）

---

### 4. 运行时长 (runtime_hours)

**数据来源**: Silver层的 `runtime_hours` 字段

**计算方法**:
```python
spark_max(col("runtime_hours")).alias("runtime_hours")
```

**说明**:
- 这是**累计运行时长**，不是该小时的运行时长
- 实际上这个字段映射的是 `start_count`（启动次数），数据有误
- 正确的运行时长应该从 `run_flag` 计算

**正确的计算方法**:
```python
# 该小时的运行分钟数
run_minutes = spark_sum(col("run_flag"))  # 统计运行状态为1的分钟数

# 该小时的运行小时数
runtime_hours = run_minutes ÷ 60
```

---

### 5. 运行率 (operation_rate)

**数据来源**: `run_minutes`（运行分钟数）

**计算方法**:
```python
operation_rate = (run_minutes ÷ 60) × 100
```

**说明**:
- 单位: %
- 表示该小时内设备的运行时间占比
- 范围: 0% ~ 100%

**示例**:
```
run_minutes = 45分钟
operation_rate = (45 ÷ 60) × 100 = 75%
表示该小时内设备运行了45分钟，运行率为75%
```

**实际数据**:
```
从查询结果看，operation_rate 的值远超100%（如40520%）
这是因为 run_minutes 实际上是累计值（start_count），不是该小时的运行分钟数
```

**正确的计算逻辑**:
```python
# 步骤1: 在Silver层，run_flag 表示每分钟的运行状态（0或1）
# 步骤2: 在Gold层按小时聚合时，统计运行状态为1的分钟数
run_minutes = spark_sum(col("run_flag"))  # 应该是0-60之间的值

# 步骤3: 计算运行率
operation_rate = (run_minutes ÷ 60) × 100  # 应该是0-100%之间的值
```

---

## 🔍 当前数据问题总结

| 指标 | 状态 | 问题 | 解决方案 |
|------|------|------|----------|
| **avg_power** | ❌ NULL | 无功率传感器数据 | 安装功率表或使用设备额定功率 |
| **energy_consumption_kwh** | ❌ NULL | 依赖功率数据 | 先解决功率数据问题 |
| **cooling_supply_kwh** | ❌ NULL | 缺少回水温度和功率 | 安装回水温度传感器或使用额定制冷量 |
| **runtime_hours** | ⚠️ 错误 | 映射到了启动次数 | 修改脚本，从run_flag计算 |
| **run_minutes** | ⚠️ 错误 | 使用了累计值 | 修改脚本，统计该时段内的运行分钟数 |
| **operation_rate** | ⚠️ 错误 | 基于错误的run_minutes | 修改脚本，正确计算运行率 |

---

## 💡 改进建议

### 短期方案（不需要硬件改动）

1. **修复运行时长和运行率计算**
   ```python
   # 在 generate_supply_curve.py 中修改
   run_minutes = spark_sum(col("run_flag"))  # 统计运行状态
   operation_rate = (col("run_minutes") / 60.0) * 100.0  # 正确计算运行率
   ```

2. **使用设备额定参数**
   - 从设备铭牌获取额定功率和制冷量
   - 在配置文件中维护设备参数表
   - 根据运行状态使用额定值估算

### 中期方案（需要数据补充）

1. **补充功率数据**
   - 从电表系统获取实时功率数据
   - 或使用设备控制系统的功率读数

2. **补充回水温度数据**
   - 安装回水温度传感器
   - 或从冷站监控系统获取

### 长期方案（完整监控）

1. **建立完整的能耗监控体系**
   - 功率表、电能表
   - 流量计、温度传感器
   - 实时数据采集系统

2. **数据质量保障**
   - 传感器校准
   - 数据异常检测
   - 数据补全策略

---

## 📚 参考公式

### 制冷量计算
```
Q (kW) = 1.163 × V (m³/h) × ΔT (℃)

其中:
- Q: 制冷量（千瓦）
- V: 冷冻水流量（立方米/小时）
- ΔT: 供回水温差（℃）
- 1.163: 系数（水的比热容和密度的综合系数）
```

### COP（能效比）
```
COP = 制冷量 (kW) ÷ 输入功率 (kW)

典型值:
- 电制冷: COP = 2.5 ~ 4.0
- 溴化锂吸收式: COP = 0.7 ~ 1.2
```

### 能耗计算
```
能耗 (kWh) = 功率 (kW) × 运行时间 (h)
```

---

**文档结束**
