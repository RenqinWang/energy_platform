# 计算逻辑修复总结

**修复时间**: 2026-05-21 19:51  
**修复类型**: 短期方案 - 修复运行时长和运行率计算逻辑

---

## 🐛 发现的问题

### 1. 数据映射错误

**问题**: Silver层生成时，`run_flag`字段混合了两种不同的数据：
- **"X#冷机运行"** - 运行状态（0/1）
- **"X#冷机运行时间"** - 累计运行时间（如2026小时）

**影响**: 
- `run_flag`的值变成了累计运行时间（如1756.0, 2026.0）
- 导致`run_minutes`计算错误（每小时24312分钟，而不是0-60分钟）
- 导致`operation_rate`超过100%（如40520%）

### 2. 运行时长计算错误

**问题**: Gold层的`runtime_hours`字段直接使用了Silver层的累计值，而不是该时段的实际运行时长。

**影响**: 无法正确反映设备在特定时段的运行情况。

---

## ✅ 修复方案

### 修复1: 区分运行状态和运行时间（Silver层）

**文件**: `data-processing/silver-layer/generate_chiller_status.py`

**修改前**:
```python
# 运行状态
spark_max(when(col("theme") == "status", col("value"))).alias("run_flag"),
```

**修改后**:
```python
# 运行时长（累计）- 从"运行时间"点位获取
spark_max(when((col("theme") == "status") & col("point_name").contains("运行时间"), col("value"))).alias("runtime_hours"),

# 运行状态（0/1）- 从"运行"点位获取，排除"运行时间"
spark_max(when((col("theme") == "status") & ~col("point_name").contains("运行时间"), col("value"))).alias("run_flag"),
```

**效果**: 
- `run_flag`现在只包含0/1值
- `runtime_hours`包含累计运行时间

---

### 修复2: 正确计算运行时长和运行率（Gold层）

**文件**: `data-processing/gold-layer/generate_supply_curve.py`

#### 2.1 聚合时区分累计值和实际值

**修改前**:
```python
# 运行时长（小时）
spark_max(col("runtime_hours")).alias("runtime_hours"),

# 运行状态（运行的分钟数）
spark_sum(col("run_flag")).alias("run_minutes"),
```

**修改后**:
```python
# 累计运行时长（小时）- 从Silver层获取的累计值
spark_max(col("runtime_hours")).alias("cumulative_runtime_hours"),

# 运行分钟数（该小时内运行状态为1的分钟数，0-60之间）
spark_sum(col("run_flag")).alias("run_minutes"),
```

#### 2.2 计算该时段的实际运行时长

**新增**:
```python
# 计算该小时的实际运行时长（小时）= 运行分钟数 / 60
df_hourly = df_hourly.withColumn(
    "runtime_hours",
    col("run_minutes") / 60.0
)
```

#### 2.3 修正能耗和供冷量计算

**修改前**:
```python
# 计算能耗（kWh）= 平均功率 * 运行分钟数 / 60
df_hourly = df_hourly.withColumn(
    "energy_consumption_kwh",
    col("avg_power") * col("run_minutes") / 60.0
)

# 计算供冷量（kWh）= 制冷量 * 运行分钟数 / 60
df_hourly = df_hourly.withColumn(
    "cooling_supply_kwh",
    col("cooling_capacity_kw") * col("run_minutes") / 60.0
)
```

**修改后**:
```python
# 计算能耗（kWh）= 平均功率 * 运行时长
df_hourly = df_hourly.withColumn(
    "energy_consumption_kwh",
    col("avg_power") * col("runtime_hours")
)

# 计算供冷量（kWh）= 制冷量 * 运行时长
df_hourly = df_hourly.withColumn(
    "cooling_supply_kwh",
    col("cooling_capacity_kw") * col("runtime_hours")
)
```

#### 2.4 修正运行率计算

**修改前**:
```python
# 计算运行率（%）= 运行分钟数 / 60
df_hourly = df_hourly.withColumn(
    "operation_rate",
    col("run_minutes") / 60.0 * 100.0
)
```

**修改后**:
```python
# 计算运行率（%）= (运行分钟数 / 60) * 100
# 正常范围: 0% ~ 100%
df_hourly = df_hourly.withColumn(
    "operation_rate",
    (col("run_minutes") / 60.0) * 100.0
)
```

**说明**: 虽然公式看起来相同，但由于`run_minutes`的值已经修正，结果现在是正确的。

---

### 修复3: 日报表同步修改

**文件**: `data-processing/gold-layer/generate_daily_report.py`

**修改**:
```python
# 运行统计
spark_sum(col("runtime_hours")).alias("total_runtime_hours"),  # 累加每小时的运行时长
spark_max(col("cumulative_runtime_hours")).alias("cumulative_runtime_hours"),  # 累计运行时长
spark_max(col("start_count")).alias("total_start_count"),
spark_sum(col("run_minutes")).alias("total_run_minutes"),
```

---

## 📊 修复效果对比

### 修复前

| 指标 | 修复前的值 | 问题 |
|------|-----------|------|
| `run_flag` | 1756.0, 2026.0 | 应该是0/1 |
| `run_minutes` | 24312 | 应该是0-60 |
| `operation_rate` | 40520% | 应该是0-100% |
| `runtime_hours` | 累计值 | 应该是该时段的实际值 |

### 修复后

| 指标 | 修复后的值 | 说明 |
|------|-----------|------|
| `run_flag` | 0.0, 1.0 | ✅ 正确的运行状态 |
| `run_minutes` | 12 | ✅ 该小时运行了12分钟 |
| `operation_rate` | 20% | ✅ 12/60 = 20% |
| `runtime_hours` | 0.2 | ✅ 12分钟 = 0.2小时 |
| `cumulative_runtime_hours` | 2026.0 | ✅ 累计运行时长 |

---

## 🧪 验证结果

### API测试

**请求**:
```bash
curl "http://localhost:8001/api/supply-curve?start_date=2018-01-01&equipment_id=chiller_10&limit=1"
```

**响应**:
```json
{
    "equipment_id": "chiller_10",
    "stat_hour": "2018-01-01 23:00:00",
    "run_minutes": 12,
    "operation_rate": 20.0,
    "runtime_hours": 0.2
}
```

**结论**: ✅ 运行率在合理范围内（0-100%）

---

## 📝 字段说明（修复后）

### Silver层 (silver_chiller_status)

| 字段 | 类型 | 说明 | 数据来源 |
|------|------|------|----------|
| `run_flag` | double | 运行状态（0=停机，1=运行） | "X#冷机运行"点位 |
| `runtime_hours` | double | 累计运行时长（小时） | "X#冷机运行时间"点位 |

### Gold层 (gold_supply_curve_hourly)

| 字段 | 类型 | 说明 | 计算方法 |
|------|------|------|----------|
| `run_minutes` | double | 该小时的运行分钟数（0-60） | `SUM(run_flag)` |
| `runtime_hours` | double | 该小时的运行时长（0-1小时） | `run_minutes / 60` |
| `cumulative_runtime_hours` | double | 累计运行时长（小时） | `MAX(runtime_hours)` from Silver |
| `operation_rate` | double | 运行率（0-100%） | `(run_minutes / 60) * 100` |

---

## ⚠️ 仍然存在的限制

虽然修复了计算逻辑，但以下指标仍然为NULL（因为缺少原始数据）：

1. **avg_power** - 无功率传感器数据
2. **energy_consumption_kwh** - 依赖功率数据
3. **cooling_supply_kwh** - 缺少回水温度数据

**解决方案**: 参考 `DATA_METRICS_EXPLANATION.md` 中的中长期方案。

---

## ✨ 总结

通过本次修复：

1. ✅ 正确区分了运行状态和累计运行时间
2. ✅ 修正了运行分钟数的计算（0-60范围）
3. ✅ 修正了运行率的计算（0-100%范围）
4. ✅ 区分了该时段运行时长和累计运行时长
5. ✅ 所有计算逻辑符合物理意义

**数据质量**: 从错误的40520%运行率修正为合理的0-100%范围。

---

**修复完成！**
