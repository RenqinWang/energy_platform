# 项目对话与开发过程纪要

整理时间：2026-05-25  
项目：智慧能源网分布式数据分析平台  
说明：本文档由 Codex 根据本轮项目协作对话整理，不是逐字聊天记录，重点保留项目推进、技术决策、问题排查和最终交付相关内容。

## 1. 初始目标与项目理解

本轮协作围绕 `energy-platform` 项目展开。最初目标是理解当前项目结构和进度，并逐步完成《作业2-分布式数据分析》第二阶段要求，包括：

- Kafka 数据采集与流式模拟。
- HDFS/Spark/Delta Lake 三层数据湖。
- Silver 明细层和 Gold 报表层加工。
- FastAPI 后端查询 Delta 表。
- React 前端展示设备级、主题级、系统级、报表、预测、收益和运行建议。
- 分布式有效性验证。
- 最终提交材料整理。

项目最终形成了 full 全量处理口径和 stream 模拟流式处理口径两套数据链路。

## 2. 集群与服务确认

对话中多次确认了当前有效集群节点，最终以以下三台机器为准：

| 节点 | 公网 IP | 内网 IP | 角色 |
|---|---|---|---|
| Node-1 | `115.120.208.241` | `192.168.0.94` | 入口、HDFS NameNode/DataNode、Spark Worker、FastAPI、React 前端 |
| Node-2 | `1.94.113.240` | `192.168.1.87` | Spark Master、HDFS DataNode、Spark Worker、Kafka Broker |
| Node-3 | `123.60.106.233` | `192.168.1.19` | HDFS DataNode、Spark Worker、Kafka Broker |

曾经提到的 `192.168.2.165`、`123.60.53.116` 后续确认不再使用。

HDFS `dfsadmin -report` 中 Live DataNodes 应为：

- `192.168.0.94`
- `192.168.1.19`
- `192.168.1.87`

## 3. 数据源与时间口径

项目涉及两类数据源：

1. 能源站采集数据  
   通过 Kafka 生产者读取本地历史数据并模拟实时推送。业务时间来自历史数据，主要为 2018 年。

2. 能源价格数据  
   通过 `122.9.74.124:8000/full` 获取全量价格数据，通过 `122.9.74.124:8000/` 获取 binlog 增量数据。

对话中明确了几个重要口径：

- 前端看到的 2018 年时间不是错误，而是历史业务时间。
- Kafka 的“实时”体现在处理时间实时推进，即生产者持续重放、Bronze 入湖、Silver/Gold 微批刷新、前端定时重新请求。
- 价格数据和能源负荷数据时间不完全重合，因此收益预测解释为“用当前价格对历史或预测负荷进行经济性估算”，不是严格同日真实收益。

## 4. 数据湖分层设计

项目最终采用 Bronze、Silver、Gold 三层数据湖，并进一步区分 full 与 stream 两套路径。

### 4.1 Full 口径

full 口径保留稳定的全量处理结果，用于完整历史数据展示和答辩兜底。

典型链路：

```text
历史原始数据 / 价格全量数据
  -> Bronze full
  -> Silver full
  -> Gold full
  -> FastAPI full
  -> React 前端 full 模式
```

### 4.2 Stream 口径

stream 口径用于模拟实时处理和任务五验证。

典型链路：

```text
Kafka 生产者模拟推送
  -> Spark Structured Streaming 写入 Bronze stream
  -> 5 分钟微批增量处理 Silver stream
  -> Gold stream 报表/预测/收益刷新
  -> FastAPI stream
  -> React 前端 stream 模式
```

对话中讨论并实现了 stream 层重置能力，确保每次 Kafka 从 0 开始运行时，stream 层可清空重新生成。

## 5. Silver 层治理与缺失值处理

对话中重点检查了冷机、热机、冷热电三联供的原始流量、温差、累计热量等字段是否正确映射。

最终口径：

- Silver 层负责点位标准化、设备归属、系统归属、主题归属和时间字段统一。
- 对真实缺失字段不伪造数据。
- 对可计算指标使用明确规则派生，例如冷机水侧供冷量、供能量等。
- 缺失值保留质量标记，方便前端和文档解释。

已在数据完整性文档中记录缺失处理策略：

- `docs/DATA_COMPLETENESS_ANALYSIS.md`
- `docs/FINAL_SYSTEM_DESCRIPTION.md`

## 6. Gold 层报表与业务指标

用户明确要求综合报表围绕冷机、热机、冷热电三联供生成日度、周度、月度综合报表，至少包含：

- 峰值
- 谷值
- 总供能量
- 峰值期时长
- 谷值期时长
- 设备利用率
- 能效指标
- 异常统计
- 站点或系统间对比

对话中补齐了 Gold 层对冷机、热机、三联供的统一加工，形成系统小时表和日/周/月报表。

当前 Gold 重点表包括：

- `gold_system_hourly`
- `gold_system_report_daily`
- `gold_system_report_weekly`
- `gold_system_report_monthly`
- `gold_system_trend_forecast`
- `gold_system_forecast_metrics`
- `gold_system_revenue_forecast`
- `gold_operation_advice`

## 7. 供能趋势预测

对话中多次讨论预测逻辑，重点包括：

- 为什么选择随机森林。
- 特征设计、时间窗口构造、模型选择依据、训练与评估方式。
- 为什么冷机某些设备历史供能量为 0 但仍有预测曲线。
- 是否应强制将当前停机设备预测归零。

最终口径：

- 预测模型主要采用 `RandomForestRegressor`。
- 特征包括季节、日期类型、时段变化、历史负荷窗口等。
- 对当前停机设备不强制把预测归零，而是在前端和建议层标注“当前停机，预测仅代表趋势需求”。
- 冷机、热机、三联供都纳入统一系统级预测展示。

## 8. 收益预测

对话中发现 Dashboard 利润预测和 `/revenue` 页面口径不一致，且出现较大负数。

最终修正方向：

- Dashboard 的收益预测应和 `/revenue` 页面保持一致。
- 收益预测应基于 Gold 预测结果和价格数据统一计算。
- 避免使用不一致的临时估算逻辑导致异常负值。
- 在文档中说明价格时间与能源业务时间不完全重合的业务解释。

## 9. 前端展示调整

前端主要页面包括：

- 设备级查询 `/device`
- 主题级查询 `/theme`
- 系统级展示 `/system`
- 综合报表 `/reports`
- 供能预测 `/forecast`
- 收益预测 `/revenue`
- 监测大屏 `/dashboard`

对话中完成或讨论的前端问题包括：

- 设备选择不能只有 chiller，需要覆盖冷机、热机、三联供。
- 主题查询时间范围变化后，图表时间轴应同步变化。
- 收益预测页面字体和排版需要统一。
- 监测大屏切换为白天模式。
- 顶部导航栏从暗色改为浅色风格。
- 监测大屏增加仪表盘、矩阵、趋势图等更丰富的 ECharts 展示。
- Dashboard 日期切换后失效的问题需要按可用日期筛选。
- 运行分析与决策建议应按当日日期和当前数据计算，而不是固定文本。

前端最终以 `http://115.120.208.241:3001` 作为主要展示地址。

## 10. 在线、离线和停机口径

用户重点追问了“设备运行状态”中在线、告警、离线的来源。

对话中明确：

- 当前没有真实 2026 设备心跳数据。
- Kafka 数据来自 2018 年历史记录重放。
- 因此前端不应把“在线/离线”解释成真实网络心跳。
- 更合理口径是：在当前展示窗口内是否存在有效运行负荷或有效采集数据。

最终答辩口径：

```text
在线/离线表示当前数据窗口内设备是否有有效运行数据，不代表真实 2026 年设备网络心跳。
```

## 11. 流式处理与微批增量

对话中围绕 stream 模式排查了多个问题：

- Kafka 生产者速率是否正常。
- 真实时间被压缩了多少倍。
- 如何降低或提高压缩比。
- stream 层重置后为什么前端没有数据。
- Gold 报表为什么落后于 Bronze/Silver。
- 是否实现了 5 分钟微批真正只处理新数据。

最终实现目标：

- Kafka 生产者可调速。
- stream 层可重置。
- 微批通过水位控制增量处理。
- Bronze、Silver、Gold 逐层推进。
- 前端 stream 模式读取 stream Gold 结果。

对话中也明确了 Gold 报表滞后是正常现象，因为 Gold 由周期性微批触发，不是每条 Kafka 消息即时更新。

## 12. 分布式有效性验证

任务五最终选择“性能对比验证”。

测试设计：

- 第一次：单机模式，Spark local、单 Kafka broker、HDFS 副本数 1。
- 第二次：分布式模式，Spark Standalone、双 Kafka broker、HDFS 副本数 3。
- 测试持续约半小时到一小时。
- 记录 Kafka 吞吐、Spark 微批耗时、Silver/Gold 行数、CPU/内存、节点参与度。

正式报告路径：

- `reports/stream_validation_single_20260525_160213`
- `reports/stream_validation_distributed_20260525_194758`
- `docs/TASK5_DISTRIBUTED_VALIDATION.md`

关键结果：

- 分布式模式承载更高输入压力。
- 分布式模式处理了约 1.37 倍 Silver 数据和 Gold 小时数据。
- Node-2、Node-3 CPU 使用率明显提升，证明任务实际分布到集群节点。
- 单机和分布式 Kafka 生产间隔分别为 2000 ms 和 1800 ms，因此不是严格同参；文档中已按保守口径说明。

## 13. 最终提交材料整理

最后阶段按照作业 PDF 要求整理了最终提交目录：

```text
final_submission/HW2-组长姓名-组长学号/
```

已生成材料包括：

- `README_提交说明.md`
- `VIDEO_PLACEHOLDER.md`
- `CODE_PACKAGE_MANIFEST.md`
- `source_code_energy_platform.tar.gz`
- `code_scripts_configs_energy_platform.tar.gz`
- `docs/系统说明文档.md`
- `docs/任务五_分布式有效性验证.md`
- `docs/数据完整性与缺失处理.md`
- `docs/部署运行指南.md`
- `docs/前端展示说明.md`
- `docs/提交检查清单.md`
- `vibe_coding_context/VIBE_CODING_CONTEXT.md`
- `reports/task5/`

整体提交包：

```text
final_submission/HW2-组长姓名-组长学号.tar.gz
```

代码、脚本和配置专用包：

```text
final_submission/HW2-组长姓名-组长学号/code_scripts_configs_energy_platform.tar.gz
```

## 14. 当前仍需人工补充

提交前仍需用户手动补全：

1. 将目录名和压缩包名中的 `组长姓名-组长学号` 改为真实信息。
2. 在 `docs/系统说明文档.md` 第 17 节补全组员姓名、学号、分工和贡献比例。
3. 录制演示视频并放入最终提交目录，建议命名为 `demo_video.mp4`。
4. 最终确认不上传原始企业私有数据、HDFS 数据湖 dump、`node_modules`、`venv`。

## 15. 关键文件索引

| 文件 | 用途 |
|---|---|
| `docs/FINAL_SYSTEM_DESCRIPTION.md` | 最终统一系统说明文档 |
| `docs/TASK5_DISTRIBUTED_VALIDATION.md` | 任务五分布式有效性验证报告 |
| `docs/DATA_COMPLETENESS_ANALYSIS.md` | 数据完整性和缺失处理说明 |
| `docs/PRODUCTION_DEPLOYMENT_GUIDE.md` | 部署和启动说明 |
| `docs/frontend-data-display-guide.md` | 前端展示和数据口径说明 |
| `docs/PROJECT_MEMO.md` | 项目部署备忘录 |
| `vibe_coding_context/VIBE_CODING_CONTEXT.md` | Vibe coding 上下文 |
| `CODE_PACKAGE_MANIFEST.md` | 代码包范围说明 |

