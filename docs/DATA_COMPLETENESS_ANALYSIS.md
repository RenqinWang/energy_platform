# 数据完整性分析报告

## 当前结论

这份报告的旧版本是在 2026-05-21 生成的，当时结论是“Bronze 层只有温度数据，导致 Silver/Gold 大量字段为空”。该结论现在已经不准确。

截至 2026-05-23，Silver 层治理逻辑已重新修复并重跑：

- `silver_point_meta_dim` 已按 `point_code` 去重，`PT2005_P` 只保留 1 条。
- `SLG_JZ*`、`S_SLG_JZ*` 已正确归类为 `generator`，不再混入冷机系统。
- 点位字典新增 `measure_role`，冷机出水温度与回水温度已能分别聚合到 `supply_temp` 和 `return_temp`。
- 冷机没有实测功率点位，`power` 已在 Silver 层按水侧冷量和固定 COP 做保守估算。
- `silver_point_fact` 记录数为 6,117,217，与 `bronze_sensor_raw` 一致，不再存在 Join 放大。

## 当前数据完整性统计

### Silver 点位字典 (`silver_point_meta_dim`)

```
总记录数: 302
point_code 唯一数: 302
重复 point_code 数: 0
PT2005_P: 1 条
SLG_JZ* / S_SLG_JZ*: 64 条，system_type 全部为 generator
```

`measure_role` 分布：

```
NULL:        88
status:      65
supply_temp: 42
return_temp: 42
pressure:    24
flow:        23
count:       18
```

### Silver 点位事实表 (`silver_point_fact`)

```
bronze_sensor_raw: 6,117,217 条
silver_point_fact: 6,117,217 条
未匹配点位: 0 条
```

这说明 `pos.ttl` 中重复 `PT2005_P` 导致的 Join 放大问题已消除。

### Silver 冷机状态表 (`silver_chiller_status`)

```
总记录数: 403,820
supply_temp 非空:     394,892
return_temp 非空:     403,786
pressure 非空:        364,374
flow 非空:            403,802
power 非空:            49,061
runtime_hours 非空:   372,233
start_count 非空:     368,438
run_flag 非空:        403,787
```

因此，旧报告里“Silver 只有供水温度，flow/run_flag 全空”的说法已经不成立。现在冷机状态表已经具备出水温度、回水温度、站级冷水总管压力、流量、运行状态、运行时长、启动次数和估算功率。

## 当前口径与限制

### 1. `power` 是估算值，不是实测值

当前数据没有找到明确的冷机实测功率点位。现在的处理策略是：

```
cooling_capacity_kw = 1.163 * flow * (return_temp - supply_temp)
power = cooling_capacity_kw / 3.0
```

估算只在 `run_flag > 0`、流量大于 0、出回水温差大于 0 且相关字段齐全时回填。因此 `power` 有 49,061 条非空，不是全量非空。能耗、COP、成本和净利润可以展示，但必须标注为估算口径。

### 2. Gold 层已基于新 Silver 重跑

当前已经重跑的是：

```
silver_point_meta_dim
silver_point_fact
silver_chiller_status
gold_supply_curve_hourly
gold_report_daily
```

当前结果：

```
gold_supply_curve_hourly 总行数: 35,900
avg_power 非空:                 4,671
energy_consumption_kwh 非空:    4,671
cooling_capacity_kw 非空:      12,226
cooling_supply_kwh 非空:       12,226

gold_report_daily 总行数:       1,510
total_energy_consumption_kwh:     385
total_cooling_supply_kwh:         837
avg_cop:                          385
energy_cost:                      385
cooling_revenue:                  837
net_profit:                       385
```

`gold_supply_curve_hourly.cooling_capacity_kw` 优先使用水侧公式 `1.163 * flow * (return_temp - supply_temp)` 计算；当水侧条件不满足但功率可用时，按 `avg_power * 3.0` 回退估算。日报的能耗、COP、成本和净利润已经有部分结果。

## 数据流向

```
bronze_sensor_raw
  -> silver_point_meta_dim 映射
  -> silver_point_fact
  -> silver_chiller_status
  -> gold_supply_curve_hourly
  -> gold_report_daily
  -> FastAPI / frontend
```

当前 Silver 层已修复；Gold 层已重跑。功率缺口已经用估算方式缓解，但实测功率仍未补齐。

## 建议

### 短期

- 前端说明应标注“功率为估算值，非实测值”，不要再写“只有温度数据”或“power 全空”。
- 继续保留 `scripts/verify_silver_governance.py` 作为 Silver 治理校验入口。

### 中期

- 继续调研 `pos.ttl` 和原始传感器数据中是否存在冷机实测功率相关点位。
- 如果存在，补充 `measure_role=power` 的映射规则，并让实测值覆盖估算值。
- 如果不存在，保留当前估算口径，同时在报表和答辩材料里明确 COP=3.0 假设。

### 长期

- 为 generator、cchp、boiler 建立各自的状态宽表，避免把不同系统强行塞进 `silver_chiller_status`。

---

文档更新时间: 2026-05-23
