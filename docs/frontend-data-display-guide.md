# 前端页面数据展示说明

本文档说明当前 React 前端各页面展示的数据内容、后端接口、数据湖表来源、关键字段含义和真实数据样例。

## 总览

当前新版前端入口为：

```text
http://115.120.208.241:3001/
```

后端接口服务为：

```text
http://115.120.208.241:8001/
```

前端主要展示 Gold 层面向业务加工后的数据。价格数据本身不直接单独展示，而是先进入 `bronze_price_raw` 和 `silver_price_dim`，再参与 Gold 收益预测计算。

| 页面 | 路由 | 主要接口 | 主要 Delta 表 | 数据层 |
|---|---|---|---|---|
| 系统级展示 | `/system` | `/api/system-supply-curve`, `/api/system-daily-report` | `gold_system_supply_hourly`, `gold_system_report_daily` | Gold |
| 设备级查询 | `/device` | `/api/system-equipment`, `/api/system-supply-curve` | `gold_system_supply_hourly` | Gold |
| 主题级查询 | `/theme` | `/api/system-equipment`, `/api/system-supply-curve` | `gold_system_supply_hourly` | Gold |
| 综合报表 | `/reports` | `/api/system-daily-report`, `/api/system-weekly-report`, `/api/system-monthly-report` | `gold_system_report_daily`, `gold_system_report_weekly`, `gold_system_report_monthly` | Gold |
| 供能趋势预测 | `/forecast` | `/api/system-forecast`, `/api/system-forecast-metrics`, `/api/system-supply-curve`, `/api/advice` | `gold_system_forecast_supply`, `gold_system_forecast_metrics`, `gold_system_supply_hourly`, `gold_operation_advice` | Gold |
| 收益预测 | `/revenue` | `/api/system-revenue-forecast` | `gold_system_revenue_forecast` | Gold |

## 1. 系统级展示

### 展示内容

系统级展示用于按系统查看整体运行情况，当前支持：

```text
冷机系统 chiller
热机系统 heating
冷热电三联供 cchp
```

页面展示内容包括：

- 各设备最新小时供能状态
- 当前供能量、能耗、运行率
- 供水温度、回水温度、压力、流量
- 设备运行/停机判断
- 系统级日报汇总，例如总供能量、总能耗、收益、成本、利润

注意：页面里的“在线/离线”不是网络心跳状态，而是根据最近 Gold 小时记录推断的运行状态。更准确地说是“运行中/停机”。

### 数据来源

| 用途 | 接口 | Delta 表 |
|---|---|---|
| 最新小时状态 | `/api/system-supply-curve?limit=500` | `gold_system_supply_hourly` |
| 日报概览 | `/api/system-daily-report?limit=80` | `gold_system_report_daily` |

### 关键字段

| 字段 | 含义 | 单位 |
|---|---|---|
| `system_type` | 系统类型：`chiller`、`heating`、`cchp` | - |
| `equipment_id` | 设备编号，例如 `chiller_10`、`heating_3`、`cchp_system` | - |
| `stat_hour` | 小时统计时间 | yyyy-MM-dd HH:mm:ss |
| `supply_kwh` | 总供能量 | kWh |
| `cooling_supply_kwh` | 供冷量 | kWh |
| `heating_supply_kwh` | 供热量 | kWh |
| `electric_supply_kwh` | 供电量 | kWh |
| `energy_consumption_kwh` | 能源消耗量 | kWh |
| `avg_supply_temp` | 平均供水温度 | 摄氏度 |
| `avg_return_temp` | 平均回水温度 | 摄氏度 |
| `avg_pressure` | 平均压力 | MPa |
| `avg_flow` | 平均流量 | m3/h |
| `operation_rate` | 小时运行率 | % |
| `metric_quality` | 指标来源或质量标记 | - |

### 真实样例

接口：

```text
GET /api/system-supply-curve?system_type=chiller&equipment_id=chiller_10&limit=1
```

返回样例：

```json
{
  "station_id": "station_2",
  "system_type": "chiller",
  "equipment_id": "chiller_10",
  "stat_hour": "2018-10-31 23:00:00",
  "avg_supply_temp": 5.933333333333334,
  "avg_return_temp": 8.916666666666666,
  "avg_pressure": 0.39398333333333335,
  "avg_flow": 377.77774999999997,
  "avg_power": 436.8675470711112,
  "energy_consumption_kwh": 436.8675470711112,
  "supply_kwh": 1310.7439776958329,
  "cooling_supply_kwh": 1310.7439776958329,
  "heating_supply_kwh": 0.0,
  "electric_supply_kwh": 0.0,
  "run_minutes": 60.0,
  "operation_rate": 100.0,
  "metric_quality": "chiller_status_derived",
  "dt": "2018-10-31"
}
```

## 2. 设备级查询

### 展示内容

设备级查询用于选择一个系统和一个设备，查看该设备历史小时数据。

页面展示：

- 供能曲线
- 能耗曲线
- 温度变化
- 流量、压力、功率
- 运行率
- 小时明细表格

### 数据来源

| 用途 | 接口 | Delta 表 |
|---|---|---|
| 设备列表 | `/api/system-equipment` | `gold_system_report_daily` |
| 历史小时数据 | `/api/system-supply-curve` | `gold_system_supply_hourly` |

### 关键字段

设备级查询使用的字段与系统级小时表一致，重点展示：

```text
stat_hour
supply_kwh
cooling_supply_kwh
heating_supply_kwh
electric_supply_kwh
energy_consumption_kwh
avg_supply_temp
avg_return_temp
avg_flow
avg_pressure
avg_power
operation_rate
metric_quality
```

### 真实样例

```json
{
  "equipment_id": "chiller_10",
  "stat_hour": "2018-10-31 22:00:00",
  "supply_kwh": 1407.2299379733329,
  "energy_consumption_kwh": 469.0250449733333,
  "avg_supply_temp": 5.933333333333334,
  "avg_return_temp": 9.133333333333333,
  "avg_flow": 378.1249833333333,
  "avg_pressure": 0.3935166666666667,
  "operation_rate": 100.0
}
```

## 3. 主题级查询

### 展示内容

主题级查询用于从某一类指标角度对比多台设备。当前主题包括：

| 主题 | 前端字段 | 含义 |
|---|---|---|
| 供能主题 | `supply_kwh` | 总供能量 |
| 能耗主题 | `energy_consumption_kwh` | 总能耗 |
| 温度主题 | `avg_supply_temp` | 平均供水温度 |
| 流量主题 | `avg_flow` | 平均流量 |
| 压力主题 | `avg_pressure` | 平均压力 |
| 运行主题 | `operation_rate` | 小时运行率 |

### 数据来源

| 用途 | 接口 | Delta 表 |
|---|---|---|
| 设备列表 | `/api/system-equipment` | `gold_system_report_daily` |
| 主题曲线 | `/api/system-supply-curve` | `gold_system_supply_hourly` |

### 注意事项

热机系统当前源数据中没有流量点位，因此热机的 `avg_flow` 在 Gold 表中为空。三联供源数据中有累计热量、累计冷量和发电量，流量字段也可能为空。冷机流量字段较完整。

## 4. 综合报表

### 展示内容

综合报表支持：

```text
日度报表
周度报表
月度报表
```

展示指标包括：

- 峰值供能量
- 谷值供能量
- 总供能量
- 总供冷量、总供热量、总供电量
- 总能耗
- 峰值期时长
- 谷值期时长
- 总运行时长
- 设备利用率
- 负荷因子
- 能效指标
- 异常统计
- 成本、收入、利润

### 数据来源

| 报表类型 | 接口 | Delta 表 |
|---|---|---|
| 日度 | `/api/system-daily-report` | `gold_system_report_daily` |
| 周度 | `/api/system-weekly-report` | `gold_system_report_weekly` |
| 月度 | `/api/system-monthly-report` | `gold_system_report_monthly` |

### 关键字段

| 字段 | 含义 |
|---|---|
| `peak_supply_kwh` | 周期内小时供能峰值 |
| `valley_supply_kwh` | 周期内非零小时供能谷值 |
| `total_supply_kwh` | 周期总供能量 |
| `total_cooling_supply_kwh` | 周期总供冷量 |
| `total_heating_supply_kwh` | 周期总供热量 |
| `total_electric_supply_kwh` | 周期总供电量 |
| `total_energy_consumption_kwh` | 周期总能耗 |
| `peak_duration_hours` | 峰值期小时数 |
| `valley_duration_hours` | 谷值期小时数 |
| `equipment_utilization_rate` | 设备利用率 |
| `avg_cop` | 平均供能/能耗比 |
| `anomaly_count` | 异常小时数 |
| `total_supply_revenue` | 供能收入 |
| `total_energy_cost` | 能源成本 |
| `net_profit` | 净利润 |

### 真实样例

接口：

```text
GET /api/system-daily-report?system_type=heating&equipment_id=heating_3&limit=1
```

返回样例：

```json
{
  "station_id": "station_2",
  "system_type": "heating",
  "equipment_id": "heating_3",
  "stat_date": "2018-10-31",
  "peak_supply_kwh": 0.0,
  "valley_supply_kwh": null,
  "total_supply_kwh": 0.0,
  "total_heating_supply_kwh": 0.0,
  "total_energy_consumption_kwh": 0.0,
  "total_runtime_hours": 0.0,
  "daily_operation_rate": 0.0,
  "avg_supply_temp": 21.390972222222228,
  "equipment_utilization_rate": 0.0,
  "data_completeness_rate": 1.0,
  "hour_count": 24,
  "anomaly_count": 0,
  "net_profit": 0.0
}
```

## 5. 供能趋势预测

### 展示内容

供能趋势预测页面展示：

- 未来 24 小时供能预测曲线
- 最近历史供能曲线
- 预测置信区间
- 当前运行状态解释
- 模型训练和评估指标
- 特征设计、窗口设计、模型选择依据
- 运行建议

当前预测算法：

```text
RandomForestRegressor
```

当前模型版本：

```text
random_forest_system_v3
```

### 数据来源

| 用途 | 接口 | Delta 表 |
|---|---|---|
| 预测曲线 | `/api/system-forecast` | `gold_system_forecast_supply` |
| 历史曲线 | `/api/system-supply-curve` | `gold_system_supply_hourly` |
| 模型指标 | `/api/system-forecast-metrics` | `gold_system_forecast_metrics` |
| 运行建议 | `/api/advice` | `gold_operation_advice` |

### 预测表关键字段

| 字段 | 含义 |
|---|---|
| `target_hour` | 预测目标小时 |
| `forecast_hour_offset` | 预测步长，1 到 24 |
| `predicted_supply_kwh` | 预测总供能量 |
| `predicted_cooling_kwh` | 预测供冷量 |
| `predicted_heating_kwh` | 预测供热量 |
| `predicted_electric_kwh` | 预测供电量 |
| `predicted_energy_kwh` | 预测能源消耗 |
| `confidence_lower` | 置信区间下界 |
| `confidence_upper` | 置信区间上界 |
| `recent_24_supply_kwh` | 最近 24 小时实际供能量 |
| `recent_24_operation_rate` | 最近 24 小时平均运行率 |
| `current_operation_status` | 最近运行状态 |
| `forecast_interpretation` | 预测解释 |
| `algorithm` | 算法名称 |
| `feature_set` | 特征集合 |
| `train_test_split` | 训练测试划分方式 |

### 真实样例

接口：

```text
GET /api/system-forecast?system_type=chiller&equipment_id=chiller_10&limit=1
```

返回样例：

```json
{
  "station_id": "station_2",
  "system_type": "chiller",
  "equipment_id": "chiller_10",
  "forecast_time": "2026-05-24 06:20:31",
  "target_hour": "2018-11-01 00:00:00",
  "forecast_hour_offset": 1,
  "predicted_supply_kwh": 159.70800504623105,
  "predicted_cooling_kwh": 159.70800504623105,
  "predicted_energy_kwh": 143.53055034071238,
  "confidence_lower": 0.0,
  "confidence_upper": 367.38941470798085,
  "recent_24_supply_kwh": 45033.701247899175,
  "recent_24_operation_rate": 100.0,
  "current_operation_status": "recent_active",
  "forecast_interpretation": "最近24小时存在运行，预测值可作为连续运行趋势参考。",
  "model_version": "random_forest_system_v3",
  "algorithm": "RandomForestRegressor",
  "feature_set": "season_date_time_lag_rolling_load_v1",
  "train_test_split": "time_ordered_80_20"
}
```

## 6. 收益预测

### 展示内容

收益预测页面根据供能趋势预测和价格数据计算未来收益，展示：

- 未来 24 小时预测供能量
- 预测收入
- 预测能源成本
- 预测利润
- 利润率
- 供冷、供热、供电收益构成

### 数据来源

| 用途 | 接口 | Delta 表 |
|---|---|---|
| 收益预测 | `/api/system-revenue-forecast` | `gold_system_revenue_forecast` |

收益预测的价格来源链路为：

```text
122.9.74.124:8000/full 或 /binlog_all
  -> bronze_price_raw
  -> silver_price_dim
  -> gold_system_revenue_forecast
  -> 前端收益预测页
```

注意：前端不直接展示 `silver_price_dim`。它展示的是价格参与计算后的 Gold 收益预测结果。

### 关键字段

| 字段 | 含义 |
|---|---|
| `forecast_date` | 预测日期 |
| `target_hour` | 预测目标小时 |
| `predicted_supply_kwh` | 预测总供能量 |
| `predicted_cooling_kwh` | 预测供冷量 |
| `predicted_heating_kwh` | 预测供热量 |
| `predicted_electric_kwh` | 预测供电量 |
| `predicted_energy_kwh` | 预测能源消耗 |
| `energy_price` | 电价 |
| `cooling_price` | 冷价 |
| `heating_price` | 热价 |
| `predicted_energy_cost` | 预测能源成本 |
| `predicted_supply_revenue` | 预测供能收入 |
| `predicted_profit` | 预测利润 |
| `profit_margin` | 利润率 |

### 真实样例

接口：

```text
GET /api/system-revenue-forecast?system_type=cchp&equipment_id=cchp_system&limit=1
```

返回样例：

```json
{
  "station_id": "station_2",
  "system_type": "cchp",
  "equipment_id": "cchp_system",
  "forecast_date": "2018-11-01",
  "target_hour": "2018-11-01 00:00:00",
  "forecast_hour": 0,
  "predicted_supply_kwh": 1328.3528369276855,
  "predicted_cooling_kwh": 0.16803864156785145,
  "predicted_heating_kwh": 2.1634092226063086,
  "predicted_electric_kwh": 1326.0213890635118,
  "predicted_energy_kwh": 8573.571698692478,
  "energy_price": 0.83934,
  "cooling_price": 0.6240399999999999,
  "heating_price": 0.48538,
  "predicted_energy_cost": 7196.141669580545,
  "predicted_supply_revenue": 1114.1377310989205,
  "predicted_profit": -6082.003938481625,
  "profit_margin": -5.458933638736649,
  "model_version": "random_forest_system_v3",
  "algorithm": "RandomForestRegressor"
}
```

## 7. 价格数据是否直接展示

当前前端没有单独的“价格明细页”。价格数据在数据湖中的位置是：

| 表 | 层级 | 当前用途 |
|---|---|---|
| `bronze_price_raw` | Bronze | 保存 `/full` 快照和 `/binlog_all` 变更事件解析结果 |
| `silver_price_dim` | Silver | 按站点、价格类型、日期去重后的价格维表 |
| `gold_system_revenue_forecast` | Gold | 收益预测使用价格后的结果 |

当前 `silver_price_dim` 已经刷新为：

```text
记录数：645
日期范围：2026-04-12 到 2026-05-24
站点数：5
价格类型：electricity、cooling、heating
```

2026-05-24 样例：

```text
ST001 electricity 0.7777
ST001 cooling     0.5947
ST001 heating     0.4874
ST002 electricity 0.7293
ST002 cooling     0.5946
ST002 heating     0.4475
```

## 8. 数据层判断

当前新版前端页面主要读 Gold 层：

```text
设备级查询：Gold
主题级查询：Gold
系统级展示：Gold
综合报表：Gold
供能趋势预测：Gold
收益预测：Gold
```

旧接口中仍保留一个 Silver 查询：

```text
/api/equipment-status -> silver_chiller_status
```

但新版页面当前主要使用统一系统 Gold 接口，不再以这个旧 Silver 接口作为主展示来源。
