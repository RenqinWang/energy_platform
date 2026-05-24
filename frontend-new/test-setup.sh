#!/bin/bash
# 前端测试脚本

echo "=========================================="
echo "智慧能源网监测平台 - 前端测试"
echo "=========================================="
echo ""

# 1. 检查项目结构
echo "1. 检查项目结构..."
if [ -d "/home/student/energy-platform/frontend-new/src" ]; then
    echo "✅ 项目目录存在"
    echo "   文件统计:"
    echo "   - TypeScript文件: $(find /home/student/energy-platform/frontend-new/src -name "*.ts" -o -name "*.tsx" | wc -l)"
    echo "   - API模块: $(ls /home/student/energy-platform/frontend-new/src/api/*.ts 2>/dev/null | wc -l)"
    echo "   - 类型定义: $(ls /home/student/energy-platform/frontend-new/src/types/*.ts 2>/dev/null | wc -l)"
    echo "   - Hooks: $(ls /home/student/energy-platform/frontend-new/src/hooks/*.ts 2>/dev/null | wc -l)"
    echo "   - 工具函数: $(ls /home/student/energy-platform/frontend-new/src/utils/*.ts 2>/dev/null | wc -l)"
else
    echo "❌ 项目目录不存在"
    exit 1
fi
echo ""

# 2. 检查依赖安装
echo "2. 检查依赖安装..."
cd /home/student/energy-platform/frontend-new
if [ -d "node_modules" ]; then
    echo "✅ node_modules 存在"
    if [ -f "node_modules/.package-lock.json" ]; then
        echo "✅ 依赖已安装"
    else
        echo "⚠️  依赖可能未完全安装"
    fi
else
    echo "❌ node_modules 不存在，需要运行 npm install"
fi
echo ""

# 3. 检查后端API
echo "3. 检查后端API..."
if curl -s -f http://localhost:8001/health > /dev/null 2>&1; then
    echo "✅ 后端API运行中 (http://localhost:8001)"
else
    echo "❌ 后端API未运行"
    echo "   请先启动后端: cd /home/student/energy-platform/backend && ./start_api.sh"
fi
echo ""

# 4. 显示下一步
echo "=========================================="
echo "下一步操作:"
echo "=========================================="
echo "1. 等待依赖安装完成"
echo "2. 启动后端API (如果未运行)"
echo "3. 启动开发服务器: npm run dev"
echo "4. 访问 http://localhost:3000 查看测试页面"
echo ""
