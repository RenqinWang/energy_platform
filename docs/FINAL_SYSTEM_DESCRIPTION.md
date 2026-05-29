# 智慧能源网监测平台最终统一说明文档

更新时间：2026-05-24  
适用范围：作业 2 第二阶段提交、答辩讲解、演示准备  
最终口径：本文档为当前项目统一说明文档；其他阶段性总结若与本文档冲突，以本文档为准。

## 1. 项目目标

本项目面向园区级智慧能源网协同监测与分析场景，构建一套从数据接入、数据治理、数据湖存储、统计分析、趋势预测到前端展示和运行建议的分布式数据分析平台。

平台解决的问题包括：

- 多源能源数据统一接入：Kafka 采集数据和外部价格 API 数据。
- 采集点标准化治理：将原始点位数据映射到设备、系统、主题和业务指标。
- 分层数据湖存储：基于 Delta Lake 构建 Bronze、Silver、Gold 三层。
- 批流结合处理：保留全量批处理结果，同时支持 Kafka 模拟流式和 5 分钟微批增量处理。
- 业务分析展示：支持设备级、主题级、系统级、综合报表、供能预测、收益预测和运行建议。
- 分布式有效性验证：通过 HDFS、Kafka、Spark 计算对比验证分布式处理价值。

## 2. 作业要求完成情况

| 作业任务 | 要求 | 当前平台实现 | 状态 |
|---|---|---|---|
| 任务一：数据采集 | Kafka 实时流式接入能源站采集数据 | Kafka 生产者 -> Spark Structured Streaming -> stream Bronze Delta 表 | 已实现 |
| 任务一：价格数据接入 | 价格数据批量或增量导入 | 通过 `122.9.74.124:8000/full` 和 binlog 口径同步到 Bronze/Silver 价格表 | 已实现 |
| 任务二：数据治理与存储 | Bronze/Silver/Gold 三层 Delta Lake | 已形成 full 和 stream 两套命名空间，并保留 control 水位表 | 已实现 |
| 任务二：标准化治理 | 点位、设备、主题、时间对齐和缺失处理 | `silver_point_fact`、`silver_point_meta_dim`、`silver_chiller_status`、`silver_price_dim` | 基本实现 |
| 任务三：综合报表 | 冷机、热机、三联供日/周/月报表，峰谷和总供能等指标 | `gold_system_report_daily/weekly/monthly` | 已实现 |
| 任务三：供能预测 | 至少一类趋势预测，说明特征、窗口、模型、评估 | 当前统一口径为 RandomForestRegressor，输出预测表和模型指标表 | 已实现，需答辩时说明方法 |
| 任务四：设备级展示 | 单设备历史数据，体现实时更新 | `/device` 页面，查询 Gold 小时表 | 已实现 |
| 任务四：主题级展示 | 温度、流量、压力、能耗等主题查询 | `/theme` 页面，基于 Gold 小时表主题字段 | 已实现 |
| 任务四：系统级展示 | 某系统全部设备状态和综合报表 | `/system` 页面，覆盖冷机、热机、三联供 | 已实现 |
| 任务四：预测和建议 | 展示预测结果并给出运行建议 | `/forecast` 页面和 `gold_operation_advice` | 已实现 |
| 任务四：收益预测 | 结合供能预测和能源价格计算收益 | `/revenue` 页面和 `gold_system_revenue_forecast` | 已实现 |
| 任务五：分布式验证 | 性能、扩展性或容错性三选一 | 已完成单机流式链路与 Spark/Kafka/HDFS 分布式链路性能对比，见 `docs/TASK5_DISTRIBUTED_VALIDATION.md` | 已实现 |

## 3. 部署架构

### 3.1 节点信息

| 节点 | 公网 IP | 内网 IP | 角色 | 部署组件 |
|---|---|---|---|---|
| Node-1 | `115.120.208.241` | `192.168.0.94` | 入口、存储、计算 | HDFS NameNode/DataNode、Spark Worker、FastAPI、React 前端 |
| Node-2 | `1.94.113.240` | `192.168.1.87` | 调度、存储、计算、消息 | Spark Master、HDFS DataNode、Spark Worker、Kafka Broker |
| Node-3 | `123.60.106.233` | `192.168.1.19` | 存储、计算、消息 | HDFS DataNode、Spark Worker、Kafka Broker |

### 3.2 核心服务

| 层级 | 组件 | 地址或端口 | 说明 |
|---|---|---|---|
| 存储层 | HDFS | `hdfs://node1:9000` | 三 DataNode 分布式存储 |
| 计算层 | Spark | `spark://node2:7077` | Spark Standalone 集群地址 |
| 消息层 | Kafka | `192.168.1.87:9092,192.168.1.19:9092` | 两 Broker |
| 后端 | FastAPI full | `http://115.120.208.241:8001` | 读取 `/lake/full` |
| 后端 | FastAPI stream | `http://115.120.208.241:8002` | 读取 `/lake/stream` |
| 前端 | React/Vite | `http://115.120.208.241:3001` | 支持 full/stream 展示口径 |

## 4. 数据源说明

### 4.1 能源站采集数据

能源采集数据通过课程提供的 Kafka 生产者模拟推送。业务时间来自历史采集数据，主要覆盖 2018 年 1 月、2 月、4 月、7 月、10 月。Kafka 推送时间是模拟流式处理的入湖时间，用于体现实时接入和微批刷新能力。

Kafka 消息典型结构：

```json
{
  "sensor_id": "10LDSCS_T",
  "label": "2#站1#冷机冷冻水出水温度",
  "timestamp": "2018-02-01T00:04:47Z",
  "value": 8.8,
  "is_simulated": false,
  "push_time": "2026-05-24T20:20:00"
}
```

平台保留两类时间字段：

| 时间字段 | 含义 | 用途 |
|---|---|---|
| `event_time` | 采集数据的业务发生时间，通常是 2018 年历史时间 | 报表、预测、历史查询 |
| `ingest_time` / `push_time` | 数据进入 Kafka 或写入数据湖的处理时间 | 模拟实时更新、水位控制、微批增量 |

### 4.2 价格数据

价格数据来自外部 HTTP API：

```text
http://122.9.74.124:8000/full
```

数据类型包括：

- `electricity`：电价，用于计算能源消耗成本。
- `cooling`：冷价，用于计算供冷收入。
- `heating`：热价，用于计算供热收入。

重要口径：价格 API 是当前价格数据，时间范围与 2018 年能源采集数据不完全重合。因此收益预测不解释为“2018 年真实收益”，而解释为：

```text
使用 2018 年历史负荷和供能模式作为运行工况，
使用当前价格 API 的最新价格或同类价格映射作为经济参数，
计算在当前价格政策下该类供能模式的收益、成本和利润。
```

## 5. 数据湖设计

### 5.1 命名空间

当前数据湖分为 full、stream 和 control 三类路径。

```text
hdfs://node1:9000/lake/
├── full/
│   ├── silver/
│   └── gold/
├── stream/
│   ├── bronze/bronze_streaming/
│   ├── silver/
│   └── gold/
└── control/
    ├── etl_watermark/
    └── stream_last_batch_point_fact/
```

| 命名空间 | 用途 | 后端模式 |
|---|---|---|
| `/lake/full` | 全量批处理结果，适合稳定演示和历史分析 | `ENERGY_DATA_MODE=full` |
| `/lake/stream` | Kafka 模拟流式和 5 分钟微批结果 | `ENERGY_DATA_MODE=stream` |
| `/lake/control` | 流式水位和 last-batch 明细 | 后台控制表 |

### 5.2 Bronze 层

Bronze 层保存原始或近原始数据，保证可追溯。

| 表 | 路径 | 说明 |
|---|---|---|
| `bronze_streaming` | `/lake/stream/bronze/bronze_streaming` | Kafka 采集数据入湖表，保留 topic、partition、offset、ingest_time |
| `bronze_price_raw` | Bronze 价格路径 | 外部价格 API 原始快照或 binlog 解析结果 |

Bronze 采集数据关键字段：

| 字段 | 含义 |
|---|---|
| `sensor_id` / `point_code` | 采集点编号 |
| `label` | 采集点中文名称 |
| `event_time` | 业务时间 |
| `value` | 原始读数 |
| `source_topic` | Kafka topic |
| `partition_id` | Kafka 分区 |
| `offset_id` | Kafka offset |
| `ingest_time` | 入湖处理时间 |
| `dt` | 按业务日期分区 |

### 5.3 Silver 层

Silver 层完成标准化、清洗、点位映射、时间对齐和维表构建。

| 表 | 说明 | 主要用途 |
|---|---|---|
| `silver_point_meta_dim` | 点位元数据维表，从 `pos.ttl` 解析 | 点位到系统、设备、主题、角色映射 |
| `silver_point_fact` | 标准化点位事实表 | 统一保存清洗后的点位明细 |
| `silver_chiller_status` | 冷机分钟级状态宽表 | 冷机供冷、温度、流量、压力、运行状态 |
| `silver_price_dim` | 价格维表 | 价格去重、生效日期和价格类型标准化 |

点位治理核心口径：

| 处理项 | 策略 |
|---|---|
| 点位映射 | 按 `point_code` 关联 `silver_point_meta_dim` |
| 重复点位 | 元数据按 `point_code` 去重，避免 Join 放大 |
| 时间对齐 | 以 `event_time` 对齐到分钟或小时 |
| 缺失值 | 按指标类型处理，不统一补 0；保留 `quality_flag`，报表端按字段可用性计算完整率 |
| 累计量 | 小时粒度取累计量差分，过滤负差分和异常大跳变 |
| 冷机功率 | 原始数据缺少明确功率点时，用水侧供冷量和固定 COP 做估算 |

缺失值处理策略：

| 指标类型 | 适用字段或主题 | 处理策略 | 质量标记 |
|---|---|---|---|
| 状态型 | `theme = status`，如运行开关、启停标志 | 短时缺失采用前值保持；若距离上一条记录超过 1 小时，不伪造状态，只标记为状态过期 | `status_stale` |
| 连续型 | `temperature`、`pressure`、`flow`、`power` | 5 分钟以内短缺口用上一条有效值填充；超过 5 分钟保留 `NULL`，下游报表计算时剔除该点 | `normal` 或 `long_missing` |
| 累计型 | `cumulative`、`runtime`，如累计热量、累计电量、运行时长 | 不做线性补值，不强制补 0；保留 `NULL`，避免累计量差分被伪造 | `cumulative_gap` |
| 价格型 | `silver_price_dim.price` | 价格维表按站点、类型、生效日期去重；收益计算侧优先使用最近可用价格口径，不能把价格缺失补成 0 | 计算侧记录口径 |
| 系统不适用字段 | 例如某些热机或三联供没有冷量、压力等点位 | 保留为空或在前端展示“无该类点位/暂无数据”，不构造假数据 | 字段可用性参与完整率 |

实现说明：

- 全量链路 `/lake/full/silver/silver_point_fact` 使用 `data-processing/silver-layer/generate_point_fact.py` 中的 `handle_missing_values`，会执行上述状态型、连续型、累计型缺失治理。
- 流式链路 `/lake/stream/silver/silver_point_fact` 当前以 5 分钟微批增量写入为主，已对 `value IS NULL` 打 `quality_flag = missing`，但没有做跨批次前值填充；答辩时应说明其当前定位是“增量接入与刷新演示”，若要和全量口径完全一致，需要在微批脚本中引入跨批次状态缓存或最近有效值查询。
- `0` 和 `NULL` 的含义不同：`0` 表示采集到的数值为零，常见于停机、无供能或瞬时负荷为零；`NULL` 表示该点位缺失、该字段不适用或质量治理后不参与计算。

### 5.4 Gold 层

Gold 层面向前端展示、报表、预测和决策建议。

| 表 | 说明 |
|---|---|
| `gold_supply_curve_hourly` | 冷机小时供能曲线，保留原冷机专项结果 |
| `gold_system_supply_hourly` | 冷机、热机、三联供统一小时表 |
| `gold_system_report_daily` | 系统日度综合报表 |
| `gold_system_report_weekly` | 系统周度综合报表 |
| `gold_system_report_monthly` | 系统月度综合报表 |
| `gold_system_forecast_supply` | 未来 24 小时供能预测 |
| `gold_system_forecast_metrics` | 模型训练评估指标 |
| `gold_system_revenue_forecast` | 收益预测结果 |
| `gold_operation_advice` | 运行分析与决策建议 |

统一小时表 `gold_system_supply_hourly` 关键字段：

| 字段 | 含义 |
|---|---|
| `station_id` | 站点编号 |
| `system_type` | 系统类型：`chiller`、`heating`、`cchp` |
| `equipment_id` | 设备编号 |
| `stat_hour` | 小时统计时间 |
| `supply_kwh` | 总供能量 |
| `cooling_supply_kwh` | 供冷量 |
| `heating_supply_kwh` | 供热量 |
| `electric_supply_kwh` | 供电量 |
| `energy_consumption_kwh` | 能源消耗 |
| `avg_supply_temp` / `avg_return_temp` | 平均供水/回水温度 |
| `avg_flow` / `avg_pressure` | 平均流量/压力 |
| `operation_rate` | 小时运行率 |
| `metric_quality` | 指标来源或估算口径 |

## 6. 冷机、热机、三联供处理口径

| 系统 | 原始数据类型 | Silver/Gold 处理 | 供能指标 |
|---|---|---|---|
| 冷机 | 供回水温度、流量、压力、运行状态、运行时长等 | 先生成 `silver_chiller_status`，再聚合到小时 Gold | `cooling_supply_kwh`，并作为 `supply_kwh` |
| 热机 | 锅炉/燃烧器状态、负荷、燃气累计量、温度、压力等 | 燃气累计量按小时差分，按负荷或运行权重分摊到热机 | `heating_supply_kwh`，并作为 `supply_kwh` |
| 冷热电三联供 | 累计热量、累计冷量、发电量、燃气量等 | 累计冷/热/电按小时差分，燃气量换算能源输入 | `cooling_supply_kwh`、`heating_supply_kwh`、`electric_supply_kwh` 汇总为 `supply_kwh` |

累计量处理规则：

```text
hour_delta = current_hour_end_value - previous_hour_end_value
```

当差分为负数或超过合理阈值时视为仪表重置、数据跳变或异常，本小时对应差分不参与供能计算。

## 7. 全量批处理与模拟流式微批

### 7.1 全量模式

全量模式读取 `/lake/full` 下的 Silver/Gold 表，适合稳定展示和批量分析。

启动方式：

```bash
cd /home/student/energy-platform
./scripts/start-backend-mode.sh full
```

后端默认读取：

```bash
ENERGY_DATA_MODE=full
HDFS_LAKE_PATH=hdfs://node1:9000/lake/full
```

### 7.2 流式模拟模式

流式模式用于展示实时接入和 5 分钟微批增量刷新能力。

流程：

```text
Kafka 生产者
  -> Spark Structured Streaming
  -> /lake/stream/bronze/bronze_streaming
  -> stream_microbatch_silver.py
  -> /lake/stream/silver/silver_point_fact
  -> Gold 增量合并
  -> 前端 stream 模式展示
```

关键机制：

| 机制 | 说明 |
|---|---|
| Kafka latest offset | 切换后只消费新产生的 Kafka 消息，避免重放历史 backlog |
| `etl_watermark` | 记录 Silver 已处理的最大 `ingest_time` |
| `stream_last_batch_point_fact` | 保存本轮微批明细，供下游只重算受影响分钟、小时和周期 |
| Delta merge | Silver/Gold 增量合并，不全表覆盖 |
| 无新增跳过 | 当本轮 `New Bronze rows = 0` 时跳过下游 Gold 刷新 |

常用命令：

```bash
cd /home/student/energy-platform

# 启动 Kafka -> stream Bronze
./scripts/start-kafka-to-bronze.sh

# 运行一次微批
./scripts/run-stream-microbatch.sh once

# 每 5 分钟循环运行
./scripts/run-stream-microbatch.sh loop

# 切换到“从当前 Kafka latest 起只处理新数据”
./scripts/reset-stream-to-latest.sh

# 启动 stream 后端
./scripts/start-backend-mode.sh stream
```

运行前需要确认只保留一个 `run-stream-microbatch.sh loop` 进程，避免两个 loop 同时写同一批数据。

## 8. 综合报表计算

综合报表覆盖三类系统：

- 冷机系统：`chiller`
- 热机系统：`heating`
- 冷热电三联供：`cchp`

支持三个周期：

- 日度：`gold_system_report_daily`
- 周度：`gold_system_report_weekly`
- 月度：`gold_system_report_monthly`

核心指标：

| 指标 | 计算口径 |
|---|---|
| 峰值供能量 | 周期内小时 `supply_kwh` 最大值 |
| 谷值供能量 | 周期内非零小时 `supply_kwh` 最小值 |
| 总供能量 | 周期内 `supply_kwh` 求和 |
| 总供冷量 | 周期内 `cooling_supply_kwh` 求和 |
| 总供热量 | 周期内 `heating_supply_kwh` 求和 |
| 总供电量 | 周期内 `electric_supply_kwh` 求和 |
| 总能耗 | 周期内 `energy_consumption_kwh` 求和 |
| 峰值期时长 | 供能量接近周期峰值的小时数 |
| 谷值期时长 | 供能量接近周期非零谷值的小时数 |
| 设备利用率 | 运行小时或运行率相对周期总小时的比例 |
| 能效指标 | 总供能量 / 总能耗；冷机可解释为估算 COP 口径 |
| 异常统计 | 按数据质量、缺失、异常负荷或异常能效计数 |

## 9. 供能趋势预测

当前统一预测口径为：

```text
模型：RandomForestRegressor
模型版本：random_forest_system_v3
预测目标：未来 24 小时 supply_kwh 及分项供能量
输出表：gold_system_forecast_supply
指标表：gold_system_forecast_metrics
```

### 9.1 特征设计

| 特征类型 | 示例 |
|---|---|
| 季节特征 | 月份、季节、是否供冷/供热季 |
| 日期类型 | 星期几、是否周末 |
| 时段变化 | 小时、小时周期 sin/cos 编码 |
| 历史负荷 | `lag_1h`、`lag_24h`、`rolling_mean_24h`、`rolling_max_24h` |
| 当前状态 | 最近 24 小时供能量、最近运行率、当前运行状态 |
| 系统类型 | 冷机、热机、三联供分别建模或分组建模 |

### 9.2 时间窗口

采用按时间顺序切分方式，避免未来数据泄漏：

```text
训练集：较早 80% 小时序列
测试集：较晚 20% 小时序列
预测窗口：最后一个历史小时之后的未来 24 小时
```

### 9.3 模型选择依据

选择随机森林回归的原因：

- 能处理非线性关系，适合能源负荷与时间、季节、运行状态之间的复杂关系。
- 对特征尺度不敏感，工程实现成本低。
- 对缺失、异常和小样本相对稳健。
- 训练速度较快，适合作业演示和多设备批量预测。
- 可通过特征重要性和误差指标解释模型效果。

### 9.4 评估方式

模型评估建议统一展示：

- MAE：平均绝对误差。
- RMSE：均方根误差。
- MAPE：相对误差，适合供能量非零样本。
- R2：拟合优度。

当前前端通过 `/api/system-forecast-metrics` 展示模型指标。答辩时应强调：当前至少完成一类趋势预测，且覆盖冷机、热机、三联供统一 Gold 口径。

### 9.5 预测解释

如果某设备最近处于停机状态，预测曲线不强制归零。此时预测表示“在历史季节、日期、时段和负荷模式下的趋势需求”，不代表设备一定会立即运行。前端和建议层应标注：

```text
当前停机，预测仅代表趋势需求。
```

## 10. 收益预测

收益预测基于供能预测和价格维表计算未来收益。

数据链路：

```text
价格 API
  -> bronze_price_raw
  -> silver_price_dim
  -> gold_system_revenue_forecast
  -> /revenue 页面
```

核心公式：

```text
能源成本 = predicted_energy_kwh * electricity_price
供冷收入 = predicted_cooling_kwh * cooling_price
供热收入 = predicted_heating_kwh * heating_price
供电收入 = predicted_electric_kwh * electricity_price
预测利润 = 供能收入 - 能源成本
利润率 = 预测利润 / 供能收入
```

说明：

- 价格来自 2026 年价格 API。
- 能源负荷来自 2018 年历史采集数据训练出的供能预测。
- 收益预测代表“当前价格下该运行模式的经济性”，不是 2018 年历史真实收益。

## 11. 前端展示设计

新版前端入口：

```text
http://115.120.208.241:3001/
```

主要页面：

| 页面 | 路由 | 主要接口 | 主要表 |
|---|---|---|---|
| 系统级展示 | `/system` | `/api/system-supply-curve`, `/api/system-daily-report` | `gold_system_supply_hourly`, `gold_system_report_daily` |
| 设备级查询 | `/device` | `/api/system-equipment`, `/api/system-supply-curve` | `gold_system_supply_hourly` |
| 主题级查询 | `/theme` | `/api/system-equipment`, `/api/system-supply-curve` | `gold_system_supply_hourly` |
| 综合报表 | `/reports` | `/api/system-daily-report`, `/api/system-weekly-report`, `/api/system-monthly-report` | `gold_system_report_*` |
| 供能预测 | `/forecast` | `/api/system-forecast`, `/api/system-forecast-metrics`, `/api/advice` | `gold_system_forecast_supply`, `gold_operation_advice` |
| 收益预测 | `/revenue` | `/api/system-revenue-forecast` | `gold_system_revenue_forecast` |

前端状态口径：

- “运行”：最近统计周期内 `operation_rate > 0` 或 `run_minutes > 0`。
- “停机”：最近统计周期内无运行。
- “无最近数据”：数据湖中没有该设备最近记录。
- 不使用“在线/离线”解释真实网络心跳，因为当前数据来自历史数据的 Kafka 模拟重放。

实时更新口径：

```text
前端实时更新能力 = Kafka 模拟流式产生数据 + Bronze 入湖 + 5 分钟微批刷新 Silver/Gold + 前端定时重新请求接口。
```

## 12. 后端 API

后端基于 FastAPI 和 PySpark 读取 Delta Lake。

关键接口：

| 接口 | 用途 |
|---|---|
| `/health` | 健康检查 |
| `/api/data-mode` | 当前后端读取 full 或 stream |
| `/api/system-equipment` | 系统/设备列表 |
| `/api/system-supply-curve` | 统一系统小时曲线 |
| `/api/system-daily-report` | 日报表 |
| `/api/system-weekly-report` | 周报表 |
| `/api/system-monthly-report` | 月报表 |
| `/api/system-forecast` | 供能趋势预测 |
| `/api/system-forecast-metrics` | 模型指标 |
| `/api/system-revenue-forecast` | 收益预测 |
| `/api/operation-advice` 或 `/api/advice` | 运行建议 |
| `/api/price-history` | 价格历史 |

## 13. 分布式有效性验证

当前已完成性能对比验证，见：

```text
docs/TASK5_DISTRIBUTED_VALIDATION.md
```

现有报告比较了两种流式数据湖运行模式：

- 单机模式：Spark `local[*]`、单 Kafka Broker、HDFS 副本数 1。
- 分布式模式：Spark Standalone `spark://192.168.1.87:7077`、双 Kafka Broker、HDFS 副本数 3。

验证任务包括：

- Kafka 生产者模拟流式推送。
- Spark Structured Streaming 写入 stream Bronze。
- 5 分钟微批增量生成 Silver 明细层。
- Gold 小时表、日/周/月报表、预测表、收益预测表刷新。
- 三节点 CPU、内存、Kafka offset、Spark 微批耗时和数据湖行数统计。

正式结果摘要：

- 单机正式报告目录：`reports/stream_validation_single_20260525_160213`。
- 分布式正式报告目录：`reports/stream_validation_distributed_20260525_194758`。
- 分布式模式处理了约 1.37 倍 Silver 数据和 Gold 小时数据。
- 分布式模式下 Node-2、Node-3 CPU 平均使用率明显上升，说明 Kafka/Spark/HDFS 任务实际分布到集群节点。
- 两组生产间隔分别为 2000 ms 和 1800 ms，不是严格同参；分布式输入压力更高，因此结论按保守口径解释。

## 14. 演示流程建议

建议答辩按以下顺序演示：

1. 展示三节点部署架构和 HDFS DataNode 状态。
2. 展示 Kafka 生产者和 Kafka-to-Bronze 入湖日志。
3. 展示 HDFS Delta Lake 三层路径。
4. 打开前端 `/system`，说明系统级监测。
5. 打开 `/device`，按设备查看历史曲线。
6. 打开 `/theme`，按温度、流量、压力、能耗主题查询。
7. 打开 `/reports`，展示日/周/月综合报表和峰谷指标。
8. 打开 `/forecast`，解释随机森林预测、特征和模型指标。
9. 打开 `/revenue`，解释价格来源和收益计算口径。
10. 展示 `TASK5_DISTRIBUTED_VALIDATION.md` 和 `reports/task5` 中的单机/分布式验证结果。

## 15. 常用启动命令

```bash
cd /home/student/energy-platform

# full 后端
./scripts/start-backend-mode.sh full

# stream 后端
./scripts/start-backend-mode.sh stream

# Kafka 到 stream Bronze
./scripts/start-kafka-to-bronze.sh

# 5 分钟微批循环
./scripts/run-stream-microbatch.sh loop

# 切换到只处理新 Kafka 数据
./scripts/reset-stream-to-latest.sh
```

前端：

```bash
cd /home/student/energy-platform/frontend-new
npm run dev -- --host 0.0.0.0 --port 3001
```

## 16. 已知限制与答辩说明

| 限制 | 说明 | 答辩口径 |
|---|---|---|
| 能源数据是 2018 年历史数据 | Kafka 是模拟重放，不是真实 2026 设备采集 | 平台展示的是流式处理能力，不代表真实设备心跳 |
| 价格数据是 2026 年当前价格 | 与 2018 负荷时间不重合 | 收益预测表示当前价格下的经济性复盘/预测 |
| 冷机功率部分为估算 | 原始点位缺少明确实测功率 | 使用水侧冷量和固定 COP 估算，并保留 `metric_quality` |
| 热机/三联供部分字段可能为空 | 原始点位不一定覆盖流量、压力等主题 | 缺失保留为空，不伪造数据；主题页应提示暂无该类点位 |
| 分布式验证同参性有限 | 单机正式测试生产间隔为 2000 ms，分布式正式测试为 1800 ms | 分布式承载了更高输入压力，按保守口径解释；若时间允许可再补严格同参测试 |
| 微批循环可能重复启动 | 多个 loop 会导致日志交叉和并发写入 | 运行前确认只保留一个 `run-stream-microbatch.sh loop` |

## 17. 组员分工

以下内容提交前需要由小组补全。

| 姓名 | 学号 | 主要分工 | 贡献比例 |
|---|---|---|---:|
| 待填写 | 待填写 | 集群部署、HDFS/Kafka/Spark 配置 | 待填写 |
| 待填写 | 待填写 | 数据湖治理、Silver/Gold 处理、预测与报表 | 待填写 |
| 待填写 | 待填写 | FastAPI、React 前端、展示与文档 | 待填写 |

## 18. 最终提交材料建议

建议提交包包含：

- 源代码：`energy-platform/`
- 最终说明文档：`docs/FINAL_SYSTEM_DESCRIPTION.md`
- 设计文档：`docs/第一阶段设计方案_v2.md`
- 分布式验证报告：`docs/TASK5_DISTRIBUTED_VALIDATION.md`
- 数据完整性报告：`docs/DATA_COMPLETENESS_ANALYSIS.md`
- 前端展示说明：`docs/frontend-data-display-guide.md`
- Vibe coding 上下文：`final_submission/HW2-组长姓名-组长学号/vibe_coding_context/VIBE_CODING_CONTEXT.md`。
- 演示视频：按第 14 节流程录制。

## 19. 文档版本说明

以下文档为阶段性记录，部分内容已经过期，仅用于追溯：

- `docs/archive/PROJECT_COMPLETION_SUMMARY.md`
- `docs/archive/FINAL_COMPLETION_REPORT.md`
- `docs/archive/FORECAST_AND_ADVICE_IMPLEMENTATION.md`
- `frontend-new/DATA_EXPLANATION.md`

最终提交和答辩说明以本文档为准。
