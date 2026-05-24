# 前端部署说明

## 部署信息

- **公网访问地址**: http://115.120.208.241:3000
- **后端API地址**: http://localhost:8001
- **部署方式**: Vite Preview Server
- **部署时间**: 2026-05-23

## 已实现的页面

### 1. 系统级综合展示 (/system) - 默认首页
- 系统健康度评分
- 在线设备统计
- 总供冷量、总电耗统计
- 设备状态网格
- 系统能耗趋势图（最近24小时）
- 设备运行状态表格
- 今日运行报告
- **实时更新**: 每10秒自动刷新数据

### 2. 设备级数据查询 (/device)
- 设备选择器
- 时间范围选择（1h/6h/24h/7d/自定义）
- 统计卡片（记录数、平均供冷量、平均电耗）
- 能耗时序图（供冷量、电耗）
- 详细数据表格（支持排序、分页）
- **实时更新控制**: 
  - 自动刷新开关（默认开启）
  - 每30秒自动刷新数据
  - 显示最后更新时间
  - 实时更新状态指示器
- **数据质量提示**: 
  - 显示有效数据率
  - 推荐使用数据完整的设备
  - 数据质量颜色标识

### 3. 主题级数据查询 (/theme)
- 主题选择（供冷、电耗）
- 多设备选择器
- 时间范围选择
- 统计卡片（前4个设备）
- 多设备对比图表
- 统计汇总表格

### 4. 供能趋势预测 (/forecast) - 优化版
- 设备选择器
- 预测概览（平均、峰值、谷值）
- 未来24小时供冷量预测曲线
- 置信区间显示
- 智能运行建议（按风险等级分类）
- 建议详情（风险等级、建议类型、证据指标）

## 技术栈

- **框架**: React 18 + TypeScript 5
- **构建工具**: Vite 8
- **UI组件**: Ant Design 5
- **图表库**: ECharts 5
- **路由**: React Router 6
- **HTTP客户端**: Axios
- **时间处理**: Day.js

## 部署步骤

### 1. 构建生产版本
```bash
cd /home/student/energy-platform/frontend-new
npm run build
```

### 2. 启动预览服务器
```bash
# 停止开发服务器（如果正在运行）
lsof -ti:3000 | xargs kill -9

# 启动预览服务器（后台运行）
nohup npm run preview -- --host 0.0.0.0 --port 3000 > preview.log 2>&1 &
```

### 3. 验证部署
```bash
# 本地访问测试
curl -I http://localhost:3000/

# 公网访问测试
curl -I http://115.120.208.241:3000/

# API代理测试
curl http://localhost:3000/api/equipment
```

## 服务管理

### 查看服务状态
```bash
# 查看进程
ps aux | grep "vite preview"

# 查看端口占用
netstat -tlnp | grep :3000

# 查看日志
tail -f preview.log
```

### 停止服务
```bash
lsof -ti:3000 | xargs kill -9
```

### 重启服务
```bash
# 停止服务
lsof -ti:3000 | xargs kill -9

# 重新构建（如果有代码更新）
npm run build

# 启动服务
nohup npm run preview -- --host 0.0.0.0 --port 3000 > preview.log 2>&1 &
```

## 已修复的问题

### 1. TypeScript类型错误
- 修复了SupplyCurve类型定义，移除了不存在的字段（heating_supply_kwh、electricity_consumption_kwh、cooling_cop、heating_cop）
- 使用实际API返回的字段（energy_consumption_kwh、cooling_supply_kwh）
- 修复了DailyReport类型使用（从单个对象改为数组）

### 2. 组件导入错误
- 添加了createTimeSeriesChartOption函数到chart.ts
- 修复了未使用的导入（FireOutlined、formatEnergy、getStations）

### 3. Vite配置错误
- 修复了manualChunks配置，从对象格式改为函数格式

### 4. 类型声明错误
- 修复了ReactNode导入（使用type-only import）
- 修复了NodeJS.Timeout类型（使用ReturnType<typeof setInterval>）
- 修复了ECharts实例类型（添加null初始值）

## API端点

所有API请求通过Vite代理转发到后端：

- `GET /api/equipment` - 获取设备列表
- `GET /api/supply-curve` - 获取供能曲线数据
- `GET /api/daily-report` - 获取日报表
- `GET /api/forecast` - 获取预测数据
- `GET /api/advice` - 获取运行建议

## 性能优化

- 代码分割：React、Ant Design、ECharts分别打包
- 生产构建大小：
  - react-vendor: 94.16 kB (gzip: 31.21 kB)
  - antd-vendor: 1,144.49 kB (gzip: 346.70 kB)
  - echarts-vendor: 1,118.58 kB (gzip: 371.05 kB)
  - index: 66.84 kB (gzip: 23.81 kB)

## 注意事项

1. **端口80需要root权限**：当前使用端口3000，如需使用端口80，需要使用sudo或配置Nginx反向代理
2. **后端API必须运行**：确保后端API服务在localhost:8001运行
3. **防火墙配置**：确保云服务器安全组允许3000端口入站流量
4. **进程管理**：建议使用PM2或systemd管理生产环境进程

## 后续优化建议

1. **使用Nginx反向代理**：
   - 安装Nginx
   - 配置反向代理到端口3000
   - 启用HTTPS
   - 配置缓存策略

2. **使用进程管理器**：
   - 安装PM2: `npm install -g pm2`
   - 使用PM2管理进程: `pm2 start npm --name "energy-frontend" -- run preview`
   - 配置开机自启: `pm2 startup && pm2 save`

3. **性能监控**：
   - 添加前端性能监控
   - 配置错误日志收集
   - 添加用户行为分析

## 访问地址

**生产环境**: http://115.120.208.241:3000

页面路由：
- 系统级展示: http://115.120.208.241:3000/system
- 设备级查询: http://115.120.208.241:3000/device
- 主题级查询: http://115.120.208.241:3000/theme
- 供能预测: http://115.120.208.241:3000/forecast
