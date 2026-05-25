# 系统说明文档 Mermaid 图源

本文档是 `system_report.tex` 中 4 张图的 Mermaid/Markdown 版本。可在 Typora、Obsidian、GitHub、VS Code Mermaid 插件中直接预览，也可以用 `mermaid-cli` 导出为 PDF/SVG/PNG。

## 导出方式

当前机器未安装 `mmdc`。安装后可在项目根目录运行：

```bash
npm install -g @mermaid-js/mermaid-cli

mmdc -i docs/assets/fig_deployment.mmd -o docs/assets/fig_deployment.pdf -b transparent
mmdc -i docs/assets/fig_lake_layers.mmd -o docs/assets/fig_lake_layers.pdf -b transparent
mmdc -i docs/assets/fig_stream_microbatch.mmd -o docs/assets/fig_stream_microbatch.pdf -b transparent
mmdc -i docs/assets/fig_frontend_flow.mmd -o docs/assets/fig_frontend_flow.pdf -b transparent
```

如果使用 Typora，可以直接打开本 Markdown 文件，确认 Mermaid 图预览正常后选择“导出 PDF”。

## 图 1：三节点分布式部署架构

```mermaid
flowchart LR
  N1["Node-1 入口节点<br/>公网 115.120.208.241<br/>内网 192.168.0.94<br/>NameNode / DataNode<br/>Spark Worker / FastAPI / React"]
  N2["Node-2 调度节点<br/>公网 1.94.113.240<br/>内网 192.168.1.87<br/>Spark Master / Worker<br/>DataNode / Kafka Broker"]
  N3["Node-3 计算存储节点<br/>公网 123.60.106.233<br/>内网 192.168.1.19<br/>DataNode / Spark Worker<br/>Kafka Broker"]

  FE["React 前端<br/>3001"]
  API["FastAPI<br/>8001 full / 8002 stream"]
  SP["Spark Standalone<br/>spark://node2:7077"]
  KF["Kafka 集群<br/>9092"]
  HDFS["HDFS + Delta Lake<br/>hdfs://node1:9000/lake"]

  N1 <-->|HDFS block replication| N2
  N2 <-->|HDFS / Kafka / Spark| N3
  FE -->|HTTP API| API
  API -->|Delta query| HDFS
  SP -->|ETL / SQL| HDFS
  KF -->|Kafka topics| HDFS

  classDef server fill:#eef5ff,stroke:#3b82f6,stroke-width:1px,color:#111827;
  classDef service fill:#f8fafc,stroke:#64748b,stroke-width:1px,color:#111827;
  classDef store fill:#ecfdf5,stroke:#22c55e,stroke-width:1px,color:#111827;
  class N1,N2,N3 server;
  class FE,API,SP,KF service;
  class HDFS store;
```

## 图 2：Delta Lake 三层存储设计

```mermaid
flowchart LR
  B["Bronze 原始层<br/>Kafka 原始消息<br/>价格原始快照<br/>offset / ingest_time"]
  S["Silver 治理层<br/>点位映射<br/>缺失治理<br/>时间对齐<br/>设备/主题标准化"]
  G["Gold 服务层<br/>报表<br/>预测<br/>收益<br/>运行建议<br/>前端宽表"]

  BP["/lake/stream/bronze<br/>bronze_streaming<br/>bronze_price_raw"]
  SP["/lake/full|stream/silver<br/>silver_point_fact<br/>silver_chiller_status<br/>silver_price_dim"]
  GP["/lake/full|stream/gold<br/>gold_system_supply_hourly<br/>gold_system_report_*<br/>forecast / revenue / advice"]
  CTL["/lake/control<br/>etl_watermark<br/>stream_last_batch_point_fact<br/>控制微批只处理新增数据"]

  B -->|清洗治理| S
  S -->|聚合建模| G
  B --- BP
  S --- SP
  G --- GP
  S -.水位与最近批次.-> CTL
  CTL -.增量控制.-> G

  classDef bronze fill:#fff7ed,stroke:#f97316,stroke-width:1px,color:#111827;
  classDef silver fill:#ecfeff,stroke:#06b6d4,stroke-width:1px,color:#111827;
  classDef gold fill:#ecfdf5,stroke:#22c55e,stroke-width:1px,color:#111827;
  classDef control fill:#fefce8,stroke:#eab308,stroke-width:1px,color:#111827;
  class B,BP bronze;
  class S,SP silver;
  class G,GP gold;
  class CTL control;
```

## 图 3：Kafka 到 Gold 的模拟流式微批链路

```mermaid
flowchart LR
  P["Kafka 生产者<br/>历史数据压缩重放"]
  K["Kafka topics<br/>sensor_*"]
  B["stream Bronze<br/>bronze_streaming"]
  S["stream Silver<br/>silver_point_fact"]
  G["stream Gold<br/>报表 / 预测 / 收益"]

  O["latest offset<br/>避免重放 backlog"]
  W["etl_watermark<br/>ingest_time 水位"]
  L["last_batch<br/>只重算受影响窗口"]

  P --> K
  K -->|Structured Streaming| B
  B -->|5 分钟微批| S
  S -->|Delta merge| G
  O -.控制.-> B
  W -.控制.-> S
  L -.控制.-> G

  classDef stream fill:#fff7ed,stroke:#f97316,stroke-width:1px,color:#111827;
  classDef lake fill:#ecfdf5,stroke:#22c55e,stroke-width:1px,color:#111827;
  classDef control fill:#fefce8,stroke:#eab308,stroke-width:1px,color:#111827;
  class P,K stream;
  class B,S,G lake;
  class O,W,L control;
```

## 图 4：前后端查询与展示链路

```mermaid
flowchart LR
  subgraph UI["React 前端页面"]
    D["监测大屏"]
    DEV["设备级查询"]
    TH["主题级查询"]
    RP["综合报表"]
    FC["供能预测"]
    RV["收益预测"]
  end

  MODE["模式切换<br/>ENERGY_DATA_MODE<br/>full / stream"]
  API["FastAPI<br/>full:8001 / stream:8002"]
  DA["PySpark Data Access<br/>按路径读取 Delta"]
  GOLD["Gold Delta 表<br/>hourly / report / forecast / revenue"]

  D --> API
  DEV --> API
  TH --> API
  RP --> API
  FC --> API
  RV --> API
  MODE --> API
  API --> DA
  DA --> GOLD

  classDef page fill:#eef5ff,stroke:#3b82f6,stroke-width:1px,color:#111827;
  classDef api fill:#faf5ff,stroke:#a855f7,stroke-width:1px,color:#111827;
  classDef data fill:#ecfdf5,stroke:#22c55e,stroke-width:1px,color:#111827;
  class D,DEV,TH,RP,FC,RV page;
  class MODE,API,DA api;
  class GOLD data;
```
