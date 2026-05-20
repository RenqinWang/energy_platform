# 后端依赖安装指南

## 问题说明

后端API服务依赖以下Python包：
- fastapi==0.104.1
- uvicorn[standard]==0.24.0
- pydantic==2.5.0
- pyspark==3.5.0
- delta-spark==3.0.0
- python-multipart==0.0.6

首次部署时需要从PyPI下载这些包及其依赖，可能需要5-10分钟。

---

## 方案1: 在线安装（推荐用于开发环境）

### 首次安装

```bash
cd /home/student/energy-platform/backend

# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate

# 安装依赖（首次需要下载）
pip install -r requirements.txt

# 启动服务
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 后续启动（无需重新安装）

```bash
cd /home/student/energy-platform/backend
source venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000
```

---

## 方案2: 离线部署（推荐用于生产环境）

### 步骤1: 在有网络的机器上打包依赖

```bash
cd /home/student/energy-platform/backend

# 下载所有依赖到本地目录
pip download -r requirements.txt -d ./packages/

# 打包
tar -czf backend-packages.tar.gz packages/
```

### 步骤2: 传输到生产环境

```bash
# 使用scp传输
scp backend-packages.tar.gz student@production-server:/home/student/

# 或使用U盘等离线方式
```

### 步骤3: 在生产环境安装

```bash
cd /home/student/energy-platform/backend

# 解压
tar -xzf backend-packages.tar.gz

# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 从本地安装（无需网络）
pip install --no-index --find-links=./packages/ -r requirements.txt

# 启动服务
uvicorn main:app --host 0.0.0.0 --port 8000
```

---

## 方案3: 使用系统Python（不推荐）

如果不想使用虚拟环境，可以直接安装到系统Python：

```bash
# 全局安装（需要sudo权限）
sudo pip3 install -r requirements.txt

# 启动服务
cd /home/student/energy-platform/backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

**注意**: 不推荐此方案，因为可能与系统其他Python应用冲突。

---

## 方案4: 使用Docker容器（推荐用于生产环境）

### 创建Dockerfile

```dockerfile
FROM python:3.10-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# 设置Java环境变量
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 暴露端口
EXPOSE 8000

# 启动命令
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### 构建和运行

```bash
cd /home/student/energy-platform/backend

# 构建镜像
docker build -t energy-platform-backend:latest .

# 运行容器
docker run -d \
  --name energy-backend \
  --network host \
  -v /home/student/energy-platform/backend:/app \
  energy-platform-backend:latest

# 查看日志
docker logs -f energy-backend
```

---

## 依赖包大小参考

| 包名 | 大小 | 说明 |
|------|------|------|
| fastapi | ~70KB | Web框架 |
| uvicorn | ~50KB | ASGI服务器 |
| pydantic | ~300KB | 数据验证 |
| pyspark | ~300MB | Spark Python API |
| delta-spark | ~20MB | Delta Lake支持 |
| 其他依赖 | ~100MB | 间接依赖 |
| **总计** | **~420MB** | 首次下载大小 |

---

## 加速下载技巧

### 使用国内镜像源

```bash
# 临时使用清华镜像
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

# 或永久配置
pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
```

### 常用镜像源

- 清华: https://pypi.tuna.tsinghua.edu.cn/simple
- 阿里云: https://mirrors.aliyun.com/pypi/simple/
- 中科大: https://pypi.mirrors.ustc.edu.cn/simple/
- 豆瓣: https://pypi.douban.com/simple/

---

## 验证安装

```bash
# 激活虚拟环境
source venv/bin/activate

# 检查已安装的包
pip list

# 验证关键包
python -c "import fastapi; print(fastapi.__version__)"
python -c "import pyspark; print(pyspark.__version__)"
python -c "import delta; print('Delta Lake OK')"

# 测试API启动
uvicorn main:app --host 0.0.0.0 --port 8000 &
sleep 5
curl http://localhost:8000/health
```

---

## 常见问题

### Q1: pip install很慢怎么办？

**A**: 使用国内镜像源（见上文）或使用离线安装方案。

### Q2: 安装pyspark时报错？

**A**: 确保已安装Java 17：
```bash
java -version
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

### Q3: 虚拟环境占用空间太大？

**A**: 虚拟环境约占500MB，这是正常的。如果空间紧张，可以：
- 使用Docker容器
- 使用系统Python（不推荐）
- 清理pip缓存：`pip cache purge`

### Q4: 如何更新依赖？

**A**: 
```bash
source venv/bin/activate
pip install --upgrade -r requirements.txt
```

---

## 推荐方案总结

| 场景 | 推荐方案 | 优点 | 缺点 |
|------|----------|------|------|
| 开发环境 | 方案1（在线安装） | 简单快速 | 需要网络 |
| 生产环境（有网） | 方案1（在线安装） | 简单 | 首次较慢 |
| 生产环境（无网） | 方案2（离线部署） | 无需网络 | 需要预先打包 |
| 容器化部署 | 方案4（Docker） | 隔离性好 | 需要Docker |

**建议**: 
- 开发环境使用方案1
- 生产环境首次部署使用方案1，后续使用已安装的虚拟环境
- 如果生产环境无网络，使用方案2
- 如果追求标准化部署，使用方案4

---

**最后更新**: 2026年5月21日
