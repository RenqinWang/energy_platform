# 智慧能源网监测平台实现计划

## Context（背景与目标）

本项目是基于分布式数据分析的智慧能源网监测平台作业的第二阶段实现。第一阶段已完成详细的系统设计方案，包括业务架构、功能架构、部署架构和数据架构。

**核心目标**：
- 构建一个完整的分布式能源数据分析平台
- 实现从数据采集到可视化展示的完整链路
- 证明分布式方案相对单机方案的价值

**技术栈**：Kafka + Spark + Delta Lake + HDFS + React + ECharts

**业务范围**：单站点（北站）、跨系统协同监测，第一阶段重点实现冷机系统

**现有基础**：
- ✅ 环境已配置：Hadoop 3.3.6、Spark 3.5.7、Docker Compose（Kafka）
- ✅ 数据已准备：点位字典（pos.ttl）、传感器数据存档
- ✅ 设计已完成：详细的系统设计方案文档
- ❌ 代码未实现：需要从零开始构建

## 实现路径（按优先级排序）

### 阶段 1：环境验证与基础搭建（1-2天）

**目标**：确保集群环境可用，搭建项目代码框架

#### 1.1 集群环境验证
- 验证 HDFS 集群状态（NameNode、DataNode）
- 验证 Spark 集群状态（Master、Worker）
- 启动 Kafka 集群（通过 docker-compose）
- 测试节点间网络连通性
- 验证 HDFS 读写权限

#### 1.2 项目代码结构搭建
```
energy-platform/
├── data-ingestion/          # 数据采集模块
│   ├── kafka-streaming/     # Kafka 流式接入
│   ├── price-sync/          # 价格数据同步
│   └── dict-loader/         # 点位字典加载
├── data-processing/         # 数据处理模块
│   ├── bronze-layer/        # Bronze 层入湖
│   ├── silver-layer/        # Silver 层标准化
│   └── gold-layer/          # Gold 层报表与预测
├── backend-api/             # 后端 API 服务
├── frontend/                # 前端展示
├── config/                  # 配置文件
├── scripts/                 # 运维脚本
└── docs/                    # 文档
```

#### 1.3 Delta Lake 环境配置
- 安装 Delta Lake Python 包（delta-spark）
- 配置 Spark Session 支持 Delta Lake
- 创建 HDFS 数据湖目录结构：
  - `/lake/bronze/`
  - `/lake/silver/`
  - `/lake/gold/`
  - `/checkpoints/`

### 阶段 2：数据采集实现（2-3天）

**目标**：打通数据接入链路，实现 Bronze 层数据落地

#### 2.1 点位字典解析与加载
**关键文件**：`data-ingestion/dict-loader/parse_pos_ttl.py`

**实现要点**：
- 解析 `/home/student/作业2数据/pos.ttl` RDF 格式文件
- 提取点位元数据：`point_code`、`point_name`、`station_id`、`system_type`、`equipment_id`、`theme`、`unit`
- 生成 `silver_point_meta_dim` 维表并写入 Delta Lake
- 支持增量更新（若字典文件变更）

**输出**：`hdfs://node-1:9000/lake/silver/silver_point_meta_dim/`

#### 2.2 Kafka 流式数据接入
**关键文件**：`data-ingestion/kafka-streaming/streaming_to_bronze.py`

**实现要点**：
- 使用 Spark Structured Streaming 订阅 Kafka topic（`subscribePattern = "sensor_.*"`）
- 解析 JSON 消息体：`sensor_id`、`label`、`timestamp`、`value`、`is_simulated`、`push_time`
- 添加元数据字段：`source_topic`、`partition_id`、`offset_id`、`ingest_time`
- 使用 `foreachBatch + MERGE INTO` 实现幂等写入（避免重复消费）
- 写入 `bronze_sensor_raw` 表，按 `dt=yyyy-MM-dd` 分区
- 配置 checkpoint 路径：`hdfs://node-1:9000/checkpoints/kafka_to_bronze/`
- 微批触发间隔：`processingTime='1 minute'`

**输出**：`hdfs://node-1:9000/lake/bronze/bronze_sensor_raw/`

#### 2.3 能源价格数据同步
**关键文件**：`data-ingestion/price-sync/sync_price_data.py`

**实现要点**：
- 启动时通过 `http://122.9.74.124:8000/full` 拉取全量价格快照
- 定时轮询 `http://122.9.74.124:8000/` 获取 binlog 增量（每 5 分钟）
- 解析字段：`id`、`station_code`、`price_type`、`price`、`created_at`、`updated_at`
- 添加 `source_type`（full/binlog）和 `ingest_time`
- 写入 `bronze_price_raw` 表，按 `price_year_month=YYYYMM` 分区
- 使用 crontab 或 Airflow 调度

**输出**：`hdfs://node-1:9000/lake/bronze/bronze_price_raw/`

### 阶段 3：数据治理与标准化（3-4天）

**目标**：实现 Silver 层数据清洗、标准化和设备状态聚合

#### 3.1 点位事实表生成
**关键文件**：`data-processing/silver-layer/generate_point_fact.py`

**实现要点**：
- 从 `bronze_sensor_raw` 读取原始流数据
- 关联 `silver_point_meta_dim` 点位字典，补齐业务字段
- 字段映射：`sensor_id` → `point_code`，补充 `station_id`、`system_type`、`equipment_id`、`theme`
- 时间字段处理：`timestamp`(UTC) → `event_time`
- 缺失值处理（按设计方案 5.5 节策略）：
  - 状态型：前值保持（超过 1 小时置 `unknown`）
  - 连续型：短时缺失（≤5 分钟）线性插值，长时缺失保留 NULL 并标记 `quality_flag`
  - 累计型：保留缺失，标记 `quality_flag = 'cumulative_gap'`
- 写入 `silver_point_fact` 表，按 `dt=yyyy-MM-dd` 分区

**输出**：`hdfs://node-1:9000/lake/silver/silver_point_fact/`

#### 3.2 冷机设备状态宽表生成
**关键文件**：`data-processing/silver-layer/generate_chiller_status.py`

**实现要点**：
- 从 `silver_point_fact` 筛选 `system_type = 'chiller'` 的点位数据
- 按 `station_id + equipment_id + stat_time`（1 分钟粒度）聚合
- 宽表字段：`supply_temp`、`return_temp`、`pressure`、`flow`、`power`、`runtime_hours`、`start_count`、`run_flag`
- 使用 Spark SQL 的 `PIVOT` 或 `CASE WHEN` 实现点位到字段的转换
- 写入 `silver_chiller_status` 表，按 `dt=yyyy-MM-dd` 分区

**输出**：`hdfs://node-1:9000/lake/silver/silver_chiller_status/`

#### 3.3 价格维表生成
**关键文件**：`data-processing/silver-layer/generate_price_dim.py`

**实现要点**：
- 从 `bronze_price_raw` 读取原始价格数据
- 字段标准化：`DATE(updated_at)` → `effective_date`
- 去重逻辑：同一 `station_code + price_type + effective_date` 保留最新 `updated_at`
- 写入 `silver_price_dim` 表，按 `price_year_month=YYYYMM` 分区

**输出**：`hdfs://node-1:9000/lake/silver/silver_price_dim/`

### 阶段 4：数据分析计算（3-4天）

**目标**：实现统计报表和趋势预测，生成 Gold 层数据产物

#### 4.1 小时级供能曲线生成
**关键文件**：`data-processing/gold-layer/generate_hourly_curve.py`

**实现要点**：
- 从 `silver_chiller_status` 按小时聚合供能数据
- 计算指标：`supply_value`（平均供冷量）、`avg_temp`、`avg_flow`
- 按 `station_id + system_type + stat_date + hour` 分组
- 写入 `gold_supply_curve_hourly` 表

**输出**：`hdfs://node-1:9000/lake/gold/gold_supply_curve_hourly/`

#### 4.2 日/周/月报表生成
**关键文件**：`data-processing/gold-layer/generate_reports.py`

**实现要点**：
- 从 `silver_chiller_status` 或 `gold_supply_curve_hourly` 聚合
- 核心指标计算：
  - `peak_value`、`valley_value`：基于分位数（P95、P5）
  - `total_supply`：累计供能量
  - `peak_duration`、`valley_duration`：峰谷期时长（分钟）
  - `avg_value`：平均值
  - `utilization_rate`：设备利用率
- 生成三张表：`gold_report_day`、`gold_report_week`、`gold_report_month`
- 调度频率：每日凌晨 2:00（crontab 或 Airflow）

**输出**：
- `hdfs://node-1:9000/lake/gold/gold_report_day/`
- `hdfs://node-1:9000/lake/gold/gold_report_week/`
- `hdfs://node-1:9000/lake/gold/gold_report_month/`

#### 4.3 供能趋势预测
**关键文件**：`data-processing/gold-layer/forecast_supply.py`

**实现要点**：
- **预测对象**：冷机系统总供冷量
- **预测粒度**：小时级
- **预测窗口**：用过去 30 天历史预测未来 24 小时
- **特征工程**：
  - 时间特征：`hour`、`day_of_week`、`month`、`is_workday`
  - 滞后特征：`lag_1h`、`lag_24h`、`lag_168h`（前 1/24/168 小时负荷）
  - 统计特征：`rolling_mean_24h`、`rolling_max_24h`
  - 季节特征：`month + hour` 组合编码
- **模型选择**：
  - 第一阶段：ARIMA 或 XGBoost（易实现、可解释）
  - 第二阶段扩展：LSTM（若时间充足）
- **评估指标**：MAPE、RMSE、MAE
- **训练集/测试集**：时间切分（后 7 天作测试集）
- **输出字段**：`pred_value`、`lower_bound`、`upper_bound`、`model_name`、`feature_version`
- 调度频率：每日凌晨 3:00 滚动重训练

**输出**：`hdfs://node-1:9000/lake/gold/gold_forecast_supply/`

#### 4.4 收益估算
**关键文件**：`data-processing/gold-layer/estimate_profit.py`

**实现要点**：
- 关联 `gold_forecast_supply` 和 `silver_price_dim`
- 计算公式：`estimated_profit = pred_supply * energy_price`
- 按 `station_code + stat_date` 聚合
- 写入 `gold_profit_estimate` 表

**输出**：`hdfs://node-1:9000/lake/gold/gold_profit_estimate/`

#### 4.5 运行建议生成
**关键文件**：`data-processing/gold-layer/generate_advice.py`

**实现要点**：
- 从 `gold_advice_rule` 读取规则配置
- 遍历启用的规则，执行 `condition_sql`（Spark SQL 片段）
- 扫描 Gold 层数据（报表、预测、收益），匹配规则条件
- 生成建议记录：`advice_text`、`risk_level`、`evidence_metrics`、`rule_id`
- 典型规则示例：
  - 预测负荷下降 30% 且运行机组数过多 → 建议关停部分机组
  - 温差异常 + 流量低 + 压力波动 → 提示传感器或设备异常
  - 价格波动 ±15% + 预测负荷高 → 收益提醒
- 写入 `gold_operation_advice` 表
- 调度频率：每日或每小时

**输出**：`hdfs://node-1:9000/lake/gold/gold_operation_advice/`

### 阶段 5：后端 API 服务（2-3天）

**目标**：提供 RESTful API 供前端查询数据

#### 5.1 技术选型
- **框架**：Flask 或 FastAPI（推荐 FastAPI，支持异步、自动文档）
- **数据访问**：PySpark 或 PyArrow（读取 Delta Lake）
- **部署节点**：Node-1

#### 5.2 核心 API 端点设计

**关键文件**：`backend-api/main.py`

**API 列表**：

1. **设备级查询 API**
   - `GET /api/device/{device_id}/history?start_time&end_time`
   - 返回：点位历史曲线数据（从 `silver_point_fact`）

2. **主题级查询 API**
   - `GET /api/theme/{theme_type}/data?station_id&start_time&end_time`
   - 主题类型：`temperature`、`flow`、`pressure`、`power`
   - 返回：按主题聚合的点位数据

3. **系统级综合查询 API**
   - `GET /api/system/{system_type}/status?station_id`
   - 返回：系统内所有设备状态（从 `silver_chiller_status`）

4. **报表查询 API**
   - `GET /api/reports/{report_type}?station_id&system_type&date`
   - 报表类型：`day`、`week`、`month`
   - 返回：统计报表数据（从 `gold_report_*`）

5. **趋势预测查询 API**
   - `GET /api/forecast?station_id&system_type&model_name`
   - 返回：预测曲线数据（从 `gold_forecast_supply`）

6. **收益分析查询 API**
   - `GET /api/profit?station_code&date`
   - 返回：收益估算数据（从 `gold_profit_estimate`）

7. **运行建议查询 API**
   - `GET /api/advice?station_id&date`
   - 返回：运行建议列表（从 `gold_operation_advice`）

8. **实时数据查询 API**
   - `GET /api/realtime/latest?station_id&system_type`
   - 返回：最新采集数据（从 `silver_point_fact` 最新分区）

#### 5.3 实现要点
- 时区转换：UTC → Asia/Shanghai（响应时完成）
- 数据缓存：使用 Redis 缓存热点查询（可选）
- 鉴权：简单 Token 鉴权
- CORS 配置：允许前端跨域访问
- 错误处理：统一错误响应格式

**部署**：
- 端口：8000
- 启动命令：`uvicorn main:app --host 0.0.0.0 --port 8000`

### 阶段 6：前端展示实现（3-4天）

**目标**：构建可视化展示界面，实现六类页面

#### 6.1 技术选型
- **框架**：React 18 + TypeScript
- **图表库**：ECharts 5.x
- **UI 组件**：Ant Design 或 Material-UI
- **状态管理**：React Query（数据获取）+ Zustand（全局状态）
- **路由**：React Router v6
- **构建工具**：Vite

#### 6.2 页面设计

**关键目录**：`frontend/src/pages/`

**1. 设备级查询页** (`DeviceQuery.tsx`)
- **用户**：运维人员
- **功能**：
  - 下拉选择：站点、系统、设备、点位
  - 时间范围选择器
  - 历史曲线图（ECharts 折线图）
  - 最新值卡片（实时更新，5 秒轮询）
- **API 调用**：`/api/device/{device_id}/history`、`/api/realtime/latest`

**2. 主题级查询页** (`ThemeQuery.tsx`)
- **用户**：调度/分析人员
- **功能**：
  - 主题选择：温度、流量、压力、能耗、功率
  - 多点位对比图（ECharts 多系列折线图）
  - 统计汇总表格
- **API 调用**：`/api/theme/{theme_type}/data`

**3. 系统级综合页** (`SystemOverview.tsx`)
- **用户**：站点管理者
- **功能**：
  - 冷机系统拓扑图（设备状态可视化）
  - 设备状态卡片（运行/停机、关键参数）
  - 系统报表展示（日/周/月切换）
- **API 调用**：`/api/system/chiller/status`、`/api/reports/{report_type}`

**4. 趋势预测页** (`ForecastView.tsx`)
- **用户**：调度/分析人员
- **功能**：
  - 历史曲线 + 预测曲线对比（ECharts 折线图 + 置信区间）
  - 模型选择器（ARIMA、XGBoost、LSTM）
  - 预测准确度指标展示（MAPE、RMSE）
  - 运行建议卡片（基于预测结果）
- **API 调用**：`/api/forecast`、`/api/advice`

**5. 收益分析页** (`ProfitAnalysis.tsx`)
- **用户**：站点管理者
- **功能**：
  - 日期选择器
  - 供能预测 + 价格 → 收益估算
  - 收益趋势图（ECharts 柱状图）
  - 价格波动提醒
- **API 调用**：`/api/profit`

**6. 运行分析看板** (`Dashboard.tsx`)
- **用户**：运维 + 管理者
- **功能**：
  - 当日运行建议列表（按风险等级排序）
  - 关键指标卡片（总供能量、设备利用率、异常点位数）
  - 趋势图表区（供能曲线、负荷分布）
  - 证据指标展示（支撑建议的数据）
- **API 调用**：`/api/advice`、`/api/reports/day`、`/api/realtime/latest`

#### 6.3 实时更新机制
- **第一阶段**：前端轮询（每 5 秒拉取最新值，趋势曲线 1 分钟刷新）
- **第二阶段扩展**：SSE（Server-Sent Events）推送（若时间充足）

#### 6.4 部署
- **端口**：3000（开发）/ 80（生产）
- **构建命令**：`npm run build`
- **部署节点**：Node-1（与 Backend API 同节点）

### 阶段 7：分布式有效性验证（2-3天）

**目标**：证明分布式方案相对单机方案的价值

#### 7.1 性能对比验证（主选）

**关键文件**：`scripts/benchmark/performance_comparison.py`

**实验设计**：

| 实验场景 | 单机基准 | 集群基准 | 输入规模 | 对比指标 |
|---------|---------|---------|---------|---------|
| 报表生成 | `spark-submit --master local[*]` | `spark-submit --master spark://node-2:7077 --deploy-mode cluster` | 2018年1月全量数据 | 总耗时、吞吐量 |
| 历史查询 | 单机 Spark SQL | 集群 Spark SQL | 多设备、多时间范围 | 平均响应时间、P95 响应时间 |
| 多任务并发 | 单任务顺序执行 | 多任务并行执行 | 实时接入 + 报表 + 预测 + 查询 | 任务完成时间、资源利用率 |

**实施步骤**：
1. 准备测试数据集（2018年1月全量数据）
2. 编写基准测试脚本（单机模式 vs 集群模式）
3. 每场景重复 3 次，取中位数
4. 记录指标：执行时间、CPU 利用率、内存使用、网络 I/O
5. 生成对比报告（表格 + 图表）

**预期结果**：
- 报表生成：集群耗时约为单机的 40-60%
- 历史查询：集群响应时间降低 30-50%
- 多任务并发：集群能同时处理多任务，单机需顺序执行

#### 7.2 扩展性验证（补充）

**实验设计**：
- 通过调整 Spark `--num-executors` 参数，观察任务完成时间变化
- 或临时关闭某个 Worker 节点，观察性能下降幅度

**实施步骤**：
1. 固定任务：生成月报表
2. 变量：Executor 数量（1、2、3）
3. 记录每种配置下的执行时间
4. 绘制扩展性曲线（Executor 数量 vs 执行时间）

#### 7.3 容错性验证（补充）

**实验设计**：
- 在 Streaming 任务运行时 `kill -9` 一个 Worker 进程
- 观察 Spark 自动重新调度和 checkpoint 恢复能力

**实施步骤**：
1. 启动 Kafka 到 Bronze 的 Streaming 任务
2. 等待任务稳定运行（处理若干批次）
3. 强制终止 Node-3 的 Spark Worker
4. 观察 Spark Master 日志，确认任务重新调度到其他节点
5. 验证数据完整性（无丢失、无重复）

**预期结果**：
- 任务在 1-2 分钟内自动恢复
- 数据无丢失（通过 checkpoint 机制）

### 阶段 8：数据湖维护与优化（1-2天）

**目标**：实现 Delta Lake 小文件治理和性能优化

#### 8.1 OPTIMIZE 任务
**关键文件**：`scripts/maintenance/optimize_tables.py`

**实现要点**：
- 每日凌晨 2:00 对 Bronze、Silver 表执行 `OPTIMIZE`
- 合并小文件并按查询热点字段排序：`OPTIMIZE ... ZORDER BY (station_id, event_time)`
- 优先处理的表：`bronze_sensor_raw`、`silver_point_fact`、`silver_chiller_status`

#### 8.2 VACUUM 任务
**关键文件**：`scripts/maintenance/vacuum_tables.py`

**实现要点**：
- 每周执行一次 `VACUUM`
- 清理超过 7 天的历史版本文件
- 保留 Delta 默认时间窗口

#### 8.3 Checkpoint 管理
- Streaming checkpoint 路径：`/checkpoints/<job_name>/`
- 任务停止时不删除，支持续传
- 定期备份 checkpoint 元数据

### 阶段 9：文档与答辩准备（2-3天）

**目标**：整理项目文档和答辩材料

#### 9.1 Vibe Coding 上下文文档
**目录**：`docs/vibe-context/`

**文档列表**：
1. **项目说明**（`README.md`）：项目概述、技术栈、目录结构
2. **表结构设计**（`table_schema.md`）：Bronze/Silver/Gold 层表结构详细说明
3. **API 契约**（`api_contract.md`）：所有 API 端点的请求/响应格式
4. **规则配置**（`advice_rules.md`）：运行建议规则的配置说明
5. **部署指南**（`deployment.md`）：集群部署步骤和配置说明
6. **开发指南**（`development.md`）：本地开发环境搭建和调试方法

#### 9.2 系统说明文档
**文件**：`docs/system_documentation.md`

**内容要求**（按作业要求）：
- 具体数据处理方法
- 存储设计方案
- 算法设计思路
- 分析结果
- 展示设计
- 各组员姓名、学号、分工情况和贡献比例

#### 9.3 演示视频
**时长**：5-10 分钟

**内容大纲**：
1. 系统架构介绍（30 秒）
2. 数据采集演示（1 分钟）：Kafka 流入湖、价格同步
3. 数据治理演示（1 分钟）：Bronze → Silver → Gold 数据流转
4. 前端展示演示（3 分钟）：六类页面逐一展示
5. 分布式验证演示（2 分钟）：性能对比实验结果
6. 总结与亮点（1 分钟）

#### 9.4 答辩 PPT
**页数**：15-20 页

**内容大纲**：
1. 项目背景与目标
2. 系统架构设计
3. 关键技术实现
4. 数据处理流程
5. 前端展示效果
6. 分布式验证结果
7. 项目亮点与创新
8. 遇到的问题与解决方案
9. 总结与展望

## 关键文件清单

### 数据采集模块
- `data-ingestion/dict-loader/parse_pos_ttl.py` - 点位字典解析
- `data-ingestion/kafka-streaming/streaming_to_bronze.py` - Kafka 流式入湖
- `data-ingestion/price-sync/sync_price_data.py` - 价格数据同步

### 数据处理模块
- `data-processing/silver-layer/generate_point_fact.py` - 点位事实表生成
- `data-processing/silver-layer/generate_chiller_status.py` - 冷机状态宽表生成
- `data-processing/silver-layer/generate_price_dim.py` - 价格维表生成
- `data-processing/gold-layer/generate_hourly_curve.py` - 小时级供能曲线
- `data-processing/gold-layer/generate_reports.py` - 日/周/月报表生成
- `data-processing/gold-layer/forecast_supply.py` - 供能趋势预测
- `data-processing/gold-layer/estimate_profit.py` - 收益估算
- `data-processing/gold-layer/generate_advice.py` - 运行建议生成

### 后端 API
- `backend-api/main.py` - FastAPI 主应用
- `backend-api/routers/device.py` - 设备级查询 API
- `backend-api/routers/theme.py` - 主题级查询 API
- `backend-api/routers/system.py` - 系统级查询 API
- `backend-api/routers/reports.py` - 报表查询 API
- `backend-api/routers/forecast.py` - 预测查询 API
- `backend-api/routers/profit.py` - 收益查询 API
- `backend-api/routers/advice.py` - 建议查询 API

### 前端页面
- `frontend/src/pages/DeviceQuery.tsx` - 设备级查询页
- `frontend/src/pages/ThemeQuery.tsx` - 主题级查询页
- `frontend/src/pages/SystemOverview.tsx` - 系统级综合页
- `frontend/src/pages/ForecastView.tsx` - 趋势预测页
- `frontend/src/pages/ProfitAnalysis.tsx` - 收益分析页
- `frontend/src/pages/Dashboard.tsx` - 运行分析看板

### 运维脚本
- `scripts/benchmark/performance_comparison.py` - 性能对比实验
- `scripts/maintenance/optimize_tables.py` - Delta Lake OPTIMIZE
- `scripts/maintenance/vacuum_tables.py` - Delta Lake VACUUM
- `scripts/deployment/start_cluster.sh` - 启动集群
- `scripts/deployment/stop_cluster.sh` - 停止集群

## 技术难点与解决方案

### 难点 1：点位字典解析
**问题**：pos.ttl 是 RDF/Turtle 格式，需要解析出点位元数据

**解决方案**：
- 使用 `rdflib` Python 库解析 RDF 图
- 通过 SPARQL 查询提取点位信息
- 映射到 `silver_point_meta_dim` 表结构

### 难点 2：Kafka 多 Topic 订阅
**问题**：每个点位一个 topic，数量多（306 个）

**解决方案**：
- 使用 `subscribePattern = "sensor_.*"` 正则订阅
- 从消息元数据中提取 `source_topic` 字段
- 避免逐 topic 配置

### 难点 3：幂等写入
**问题**：Streaming 任务重启可能导致重复消费

**解决方案**：
- 使用 `foreachBatch + MERGE INTO` 实现 upsert
- 以 `topic + partition + offset` 作为逻辑去重键
- 配置 checkpoint 支持续传

### 难点 4：缺失值处理
**问题**：不同类型指标的缺失值处理策略不同

**解决方案**：
- 状态型：前值保持（超过 1 小时置 `unknown`）
- 连续型：短时缺失线性插值，长时缺失保留 NULL 并标记
- 累计型：保留缺失，标记 `quality_flag`
- 在 Spark SQL 中使用 `LAST_VALUE` 和 `LAG` 函数实现

### 难点 5：设备状态宽表生成
**问题**：需要将点位数据透视为设备维度的宽表

**解决方案**：
- 使用 Spark SQL 的 `PIVOT` 或 `CASE WHEN` 实现
- 按 `station_id + equipment_id + stat_time` 分组
- 通过点位字典的 `theme` 字段映射到宽表列名

### 难点 6：时序预测特征工程
**问题**：需要构造滞后特征和滚动统计特征

**解决方案**：
- 使用 Spark Window 函数：`LAG`、`AVG OVER`、`MAX OVER`
- 构造时间特征：`hour`、`day_of_week`、`is_workday`
- 使用 `pyspark.ml.feature` 进行特征编码

### 难点 7：Delta Lake 小文件问题
**问题**：流式入湖产生大量小文件，影响查询性能

**解决方案**：
- 每日凌晨执行 `OPTIMIZE` 合并小文件
- 使用 `ZORDER BY` 按查询热点字段排序
- 每周执行 `VACUUM` 清理历史版本

## 依赖安装清单

### Python 依赖
```bash
# 核心依赖
pip install pyspark==3.5.7
pip install delta-spark==3.2.0
pip install kafka-python==2.0.2
pip install rdflib==7.0.0

# 机器学习
pip install scikit-learn==1.5.0
pip install xgboost==2.0.3
pip install statsmodels==0.14.0  # ARIMA

# 后端 API
pip install fastapi==0.110.0
pip install uvicorn==0.29.0
pip install pydantic==2.6.0
pip install python-multipart==0.0.9

# 数据处理
pip install pandas==2.2.0
pip install numpy==1.26.0
pip install pyarrow==15.0.0

# 工具
pip install requests==2.31.0
pip install python-dotenv==1.0.0
```

### Node.js 依赖
```bash
# 前端框架
npm install react@18.2.0 react-dom@18.2.0
npm install react-router-dom@6.22.0
npm install typescript@5.3.0

# UI 组件
npm install antd@5.15.0
npm install @ant-design/icons@5.3.0

# 图表库
npm install echarts@5.5.0
npm install echarts-for-react@3.0.2

# 状态管理
npm install @tanstack/react-query@5.24.0
npm install zustand@4.5.0

# 工具
npm install axios@1.6.7
npm install dayjs@1.11.10

# 开发工具
npm install -D vite@5.1.0
npm install -D @vitejs/plugin-react@4.2.0
npm install -D @types/react@18.2.0
npm install -D @types/react-dom@18.2.0
```

## 配置文件模板

### Spark 配置（`config/spark-defaults.conf`）
```properties
spark.master                     spark://node-2:7077
spark.sql.extensions             io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog  org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.sql.session.timeZone       UTC
spark.hadoop.fs.defaultFS        hdfs://node-1:9000
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://node-1:9000/spark-logs
spark.history.fs.logDirectory    hdfs://node-1:9000/spark-logs
```

### HDFS 配置（`config/core-site.xml`）
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://node-1:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/data/hadoop/tmp</value>
  </property>
</configuration>
```

### Kafka 配置（`docker-compose.yml`）
```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      
  kafka-1:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://node-2:9092
      
  kafka-2:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9093:9092"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://node-3:9092
```

## 验证检查清单

### 阶段 1 验证
- [ ] HDFS NameNode 启动成功（`jps` 看到 NameNode）
- [ ] HDFS DataNode 三节点都启动（`hdfs dfsadmin -report`）
- [ ] Spark Master 启动成功（访问 `http://node-2:8080`）
- [ ] Spark Worker 三节点都注册（Master Web UI 显示 3 个 Worker）
- [ ] Kafka 集群启动（`docker ps` 看到 zookeeper、kafka-1、kafka-2）
- [ ] HDFS 目录创建成功（`hdfs dfs -ls /lake/`）

### 阶段 2 验证
- [ ] 点位字典解析成功（`silver_point_meta_dim` 表有 306 条记录）
- [ ] Kafka Streaming 任务运行（`spark-submit` 无报错）
- [ ] Bronze 层有数据写入（`hdfs dfs -ls /lake/bronze/bronze_sensor_raw/`）
- [ ] 价格数据同步成功（`bronze_price_raw` 表有数据）

### 阶段 3 验证
- [ ] `silver_point_fact` 表生成（有数据且字段完整）
- [ ] `silver_chiller_status` 宽表生成（设备维度聚合正确）
- [ ] `silver_price_dim` 维表生成（价格数据标准化）
- [ ] 缺失值处理生效（`quality_flag` 字段有标记）

### 阶段 4 验证
- [ ] 小时级供能曲线生成（`gold_supply_curve_hourly` 表有数据）
- [ ] 日报表生成（`gold_report_day` 表有数据且指标正确）
- [ ] 趋势预测完成（`gold_forecast_supply` 表有预测结果）
- [ ] 收益估算完成（`gold_profit_estimate` 表有数据）
- [ ] 运行建议生成（`gold_operation_advice` 表有建议记录）

### 阶段 5 验证
- [ ] Backend API 启动成功（`curl http://node-1:8000/docs` 返回 Swagger 文档）
- [ ] 设备级查询 API 正常（返回历史数据）
- [ ] 报表查询 API 正常（返回日报表数据）
- [ ] 预测查询 API 正常（返回预测曲线）

### 阶段 6 验证
- [ ] 前端启动成功（访问 `http://node-1:3000`）
- [ ] 设备级查询页正常显示（曲线图渲染）
- [ ] 系统级综合页正常显示（设备状态卡片）
- [ ] 趋势预测页正常显示（预测曲线 + 置信区间）
- [ ] 运行分析看板正常显示（建议列表）
- [ ] 实时更新生效（5 秒轮询刷新）

### 阶段 7 验证
- [ ] 性能对比实验完成（有单机 vs 集群的耗时数据）
- [ ] 对比报告生成（表格 + 图表）
- [ ] 扩展性实验完成（可选）
- [ ] 容错性实验完成（可选）

## 时间规划建议

| 阶段 | 预计时间 | 关键里程碑 |
|-----|---------|-----------|
| 阶段 1：环境验证 | 1-2 天 | 集群联通、目录创建 |
| 阶段 2：数据采集 | 2-3 天 | Bronze 层有数据 |
| 阶段 3：数据治理 | 3-4 天 | Silver 层表生成 |
| 阶段 4：数据分析 | 3-4 天 | Gold 层报表和预测 |
| 阶段 5：后端 API | 2-3 天 | API 可调用 |
| 阶段 6：前端展示 | 3-4 天 | 六类页面完成 |
| 阶段 7：分布式验证 | 2-3 天 | 性能对比报告 |
| 阶段 8：维护优化 | 1-2 天 | OPTIMIZE/VACUUM |
| 阶段 9：文档答辩 | 2-3 天 | 文档和视频 |
| **总计** | **19-28 天** | **完整系统** |

**建议**：
- 前 4 个阶段是核心数据链路，优先级最高
- 阶段 5-6 可以部分并行（后端 API 完成一个，前端就可以开始对接）
- 阶段 7-9 可以根据时间灵活调整

## 风险与应对

### 风险 1：集群环境问题
**风险**：节点间网络不通、HDFS 启动失败

**应对**：
- 提前验证网络连通性（ping、telnet）
- 检查防火墙和安全组配置
- 准备单机降级方案（local[*] 模式）

### 风险 2：数据质量问题
**风险**：点位字典解析失败、Kafka 消息格式不符

**应对**：
- 先用小样本数据测试
- 编写数据质量检查脚本
- 记录异常数据并人工审查

### 风险 3：时间不足
**风险**：某个阶段耗时超预期

**应对**：
- 优先保证核心功能（数据采集 → 治理 → 报表 → 展示）
- 预测和建议功能可以简化（用简单模型）
- 前端页面可以先做 3-4 个核心页面

### 风险 4：性能问题
**风险**：查询响应慢、前端卡顿

**应对**：
- 使用 Delta Lake 的 OPTIMIZE 和 ZORDER
- API 层增加缓存（Redis）
- 前端使用虚拟滚动和懒加载

## 总结

本实现计划基于第一阶段的详细设计方案，将系统实现分为 9 个阶段，每个阶段都有明确的目标、关键文件和验证标准。核心思路是：

1. **先打通数据链路**：从 Kafka 到 Bronze 到 Silver 到 Gold，确保数据能流转
2. **再实现分析功能**：报表、预测、建议，逐步丰富 Gold 层数据产物
3. **最后完善展示**：后端 API + 前端页面，实现可视化和交互
4. **验证分布式价值**：通过性能对比实验证明集群优于单机

整个实现过程预计需要 19-28 天，建议按优先级逐步推进，确保核心功能完成的前提下再扩展高级功能。


