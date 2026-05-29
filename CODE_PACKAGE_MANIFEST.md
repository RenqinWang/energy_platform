# 代码、脚本与配置文件打包清单

本文件说明 `code_scripts_configs_energy_platform.tar.gz` 的打包范围。

## 已纳入

- `backend/`：FastAPI 后端源码、依赖声明、启动脚本和后端说明文档。
- `frontend-new/`：React/Vite 前端源码、静态资源、构建配置、依赖锁文件和已构建 `dist` 目录。
- `data-ingestion/`：Kafka 入湖、价格数据同步和字典解析脚本。
- `data-processing/`：full/stream 数据处理、Silver 层治理、Gold 层报表/预测/收益/建议生成脚本。
- `scripts/`：HDFS/Spark/Kafka/前后端启动停止、流式重置、微批处理、验证和检查脚本。
- `models/`：当前已训练的预测模型文件。
- 根目录关键文件：`README.md`、`.gitignore`、`DATA_METRICS_EXPLANATION.md`。
- 关键文档：最终系统说明、部署指南、任务五验证、数据完整性、前端展示说明、项目备忘录和第一阶段设计方案。

## 已排除

- Git 仓库元数据：`.git/`。
- 前端依赖目录：`frontend-new/node_modules/`。
- 后端虚拟环境：`backend/venv/`。
- Python 缓存：`__pycache__/`、`*.pyc`。
- 运行日志：`*.log`、`logs/`。
- 大量测试中间产物：`reports/`。
- 最终提交目录自身：`final_submission/`。
- 过期归档脚本与归档文档：`scripts/archive/`、`docs/archive/`。
- 作业 PDF 和原始/私有数据湖 dump。

## 解压与运行

```bash
tar -xzf code_scripts_configs_energy_platform.tar.gz
cd energy-platform
```

前端依赖需要重新安装：

```bash
cd frontend-new
npm install
npm run dev -- --host 0.0.0.0 --port 3001
```

后端依赖需要重新安装：

```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

