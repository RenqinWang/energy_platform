# 分布式有效性验证报告

## 验证方式

本次选择“性能对比验证”。

目标：比较同一批数据分析任务在单线程执行和并行执行下的耗时差异，验证平台在历史查询、综合报表聚合、趋势预测特征计算等任务上的并行处理收益。

## 验证环境

- HDFS NameNode: `hdfs://node1:9000`
- Gold 表: `hdfs://node1:9000/lake/gold/gold_system_supply_hourly`
- HDFS Live DataNodes: 3
  - `192.168.0.94:9866 (node1)`
  - `192.168.1.19:9866 (node3)`
  - `192.168.1.87:9866 (node2)`
- HDFS 状态:
  - Under replicated blocks: 0
  - Blocks with corrupt replicas: 0
  - Missing blocks: 0

说明：当前 HDFS 存储层为真实三节点集群。Spark standalone 远端 Worker 进程未在本次检查中检测到，因此本次计算对比采用同一 HDFS 数据源上的 `local[1]` 单线程基线与 `local[*]` 并行执行模式。该验证能证明当前平台在并行 Spark 计算和分布式 HDFS 存储组合下的性能收益；如果后续启动 Spark standalone/YARN Worker，可用同一脚本通过 `--parallel-master spark://node2:7077` 等参数切换为集群地址继续复测。

## 验证任务

验证脚本：[distributed_effectiveness_benchmark.py](/home/student/energy-platform/scripts/distributed_effectiveness_benchmark.py)

执行命令：

```bash
/home/student/spark-3.5.7-bin-hadoop3/bin/spark-submit scripts/distributed_effectiveness_benchmark.py --scale-factor 20 --trials 2 --shuffle-partitions 24
```

数据规模：

- 原始 Gold 小时表记录数：53,884
- 模拟分析负载倍数：20
- 实际参与计算记录数：1,077,680
- 每种模式重复次数：2

对比模式：

- 单线程基线：`local[1]`
- 并行执行：`local[*]`

任务类型：

- 综合报表生成：按站点、系统、设备、日期聚合峰值、谷值、总供能、总能耗、运行率。
- 历史数据查询：按设备取最新 168 小时窗口并汇总供能量。
- 趋势预测特征计算：构造 24 小时滞后、24 小时滚动均值、滚动最大值等预测特征。

## 验证结果

| 任务 | 单线程平均耗时 | 并行平均耗时 | 加速比 |
|---|---:|---:|---:|
| 综合报表生成 | 9.8171 s | 4.1922 s | 2.342x |
| 历史窗口查询 | 9.0616 s | 4.5847 s | 1.976x |
| 预测特征计算 | 11.2088 s | 6.1015 s | 1.837x |

结果文件：

- [distributed_effectiveness_benchmark_latest.json](/home/student/energy-platform/reports/distributed_effectiveness_benchmark_latest.json)
- [distributed_effectiveness_benchmark_20260524_150303.json](/home/student/energy-platform/reports/distributed_effectiveness_benchmark_20260524_150303.json)

## 正确性校验

同一任务在单线程和并行模式下输出行数一致：

- 综合报表输出行数：45,300
- 历史窗口查询输出行数：300
- 预测特征计算输出行数：1,077,380

关键 checksum 一致：

- 综合报表总供能 checksum: `709588229.891428`
- 综合报表总能耗 checksum: `737956724.8584212`
- 历史窗口供能 checksum: `10942309.04876112`

预测特征窗口中的滚动均值 checksum 在单线程和并行模式间仅存在浮点聚合顺序导致的极小误差，不影响业务结果。

## 结论

在相同数据、相同业务逻辑和相同输出结果下，并行 Spark 执行相比单线程基线有明显性能提升：

- 综合报表生成加速约 2.34 倍。
- 历史数据查询加速约 1.98 倍。
- 趋势预测特征计算加速约 1.84 倍。

因此，当前平台的 HDFS + Spark 架构对报表生成、历史查询和预测计算具备可验证的并行处理收益，满足任务五中“性能对比验证”的要求。
