# 智慧能源网监测平台 - 前端

基于 React 19 + Vite 8 + TypeScript 6 + Ant Design 5 + ECharts 5 构建的智慧能源网监测平台前端应用。

## 🚀 快速开始

### 安装依赖
```bash
npm install
```

### 启动开发服务器
```bash
npm run dev
```

访问: http://localhost:3000

### 构建生产版本
```bash
npm run build
```

## 📁 项目结构

```
src/
├── api/                    # API接口封装
│   ├── client.ts          # Axios客户端
│   ├── equipment.ts       # 设备API
│   ├── forecast.ts        # 预测API
│   └── advice.ts          # 建议API
├── components/            # 组件
│   ├── Layout/           # 布局组件
│   ├── Charts/           # 图表组件
│   └── Common/           # 通用组件
├── pages/                # 页面
│   ├── DeviceQuery/      # 设备级查询
│   ├── ThemeQuery/       # 主题级查询
│   ├── SystemOverview/   # 系统级展示
│   └── Forecast/         # 供能预测
├── hooks/                # 自定义Hooks
├── types/                # TypeScript类型
├── utils/                # 工具函数
├── App.tsx               # 根组件
├── main.tsx              # 入口文件
└── router.tsx            # 路由配置
```

## 🎯 功能模块

### ✅ 已实现
- [x] 项目初始化和配置
- [x] API客户端封装
- [x] 通用组件（Layout, Charts, Loading等）
- [x] 供能趋势预测页面
- [x] 路由配置

### 🚧 开发中
- [ ] 设备级数据查询页面
- [ ] 主题级数据查询页面
- [ ] 系统级综合展示页面

## 🔧 技术栈

- **框架**: React 19
- **构建工具**: Vite 8
- **语言**: TypeScript 6
- **UI库**: Ant Design 5
- **图表**: ECharts 5
- **路由**: React Router 6
- **HTTP**: Axios
- **时间**: Day.js

## 📡 API配置

开发环境API代理配置在 `vite.config.ts`:

```typescript
proxy: {
  '/api': {
    target: 'http://localhost:8001',
    changeOrigin: true
  }
}
```

## 🌐 部署

### 开发环境
```bash
npm run dev
```

### 生产环境
```bash
# 构建
npm run build

# 预览
npm run preview
```

### Nginx配置
```nginx
server {
    listen 80;
    server_name 115.120.208.241;

    location / {
        root /home/student/energy-platform/frontend-new/dist;
        try_files $uri $uri/ /index.html;
    }

    location /api {
        proxy_pass http://localhost:8001;
        proxy_set_header Host $host;
    }
}
```

## 📝 开发说明

### 添加新页面
1. 在 `src/pages/` 创建页面组件
2. 在 `src/router.tsx` 添加路由
3. 在 `src/components/Layout/Header.tsx` 添加菜单项

### 添加新API
1. 在 `src/types/` 定义类型
2. 在 `src/api/` 添加API函数
3. 在页面中使用

## 🐛 故障排除

### 依赖安装失败
```bash
rm -rf node_modules package-lock.json
npm install
```

### 开发服务器启动失败
检查端口3000是否被占用:
```bash
lsof -i :3000
```

### API连接失败
确保后端服务运行在端口8001:
```bash
curl http://localhost:8001/health
```

## 📄 License

MIT
