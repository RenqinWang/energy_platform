#!/bin/bash
# 前端构建和部署脚本

set -e

echo "=========================================="
echo "智慧能源网监测平台 - 前端构建部署"
echo "=========================================="
echo ""

# 1. 检查依赖
echo "1. 检查依赖..."
if [ ! -d "node_modules" ]; then
    echo "❌ node_modules不存在，正在安装依赖..."
    npm install
else
    echo "✅ 依赖已安装"
fi
echo ""

# 2. TypeScript类型检查
echo "2. TypeScript类型检查..."
if npx tsc --noEmit; then
    echo "✅ 类型检查通过"
else
    echo "❌ 类型检查失败"
    exit 1
fi
echo ""

# 3. 构建生产版本
echo "3. 构建生产版本..."
if npm run build; then
    echo "✅ 构建成功"
else
    echo "❌ 构建失败"
    exit 1
fi
echo ""

# 4. 检查构建产物
echo "4. 检查构建产物..."
if [ -d "dist" ]; then
    echo "✅ dist目录已生成"
    echo "   文件统计:"
    echo "   - HTML文件: $(find dist -name "*.html" | wc -l)"
    echo "   - JS文件: $(find dist -name "*.js" | wc -l)"
    echo "   - CSS文件: $(find dist -name "*.css" | wc -l)"
    echo "   - 总大小: $(du -sh dist | cut -f1)"
else
    echo "❌ dist目录不存在"
    exit 1
fi
echo ""

# 5. 部署选项
echo "=========================================="
echo "部署选项:"
echo "=========================================="
echo ""
echo "选项1: 使用Nginx部署"
echo "--------------------------------------"
echo "sudo cp -r dist /var/www/energy-platform"
echo "sudo chown -R www-data:www-data /var/www/energy-platform"
echo ""
echo "Nginx配置 (/etc/nginx/sites-available/energy-platform):"
echo "server {"
echo "    listen 80;"
echo "    server_name 115.120.208.241;"
echo ""
echo "    location / {"
echo "        root /var/www/energy-platform;"
echo "        try_files \$uri \$uri/ /index.html;"
echo "    }"
echo ""
echo "    location /api {"
echo "        proxy_pass http://localhost:8001;"
echo "        proxy_set_header Host \$host;"
echo "    }"
echo "}"
echo ""
echo "启用配置:"
echo "sudo ln -s /etc/nginx/sites-available/energy-platform /etc/nginx/sites-enabled/"
echo "sudo nginx -t"
echo "sudo systemctl reload nginx"
echo ""
echo "选项2: 使用Vite预览"
echo "--------------------------------------"
echo "npm run preview"
echo ""
echo "选项3: 手动部署到其他服务器"
echo "--------------------------------------"
echo "scp -r dist user@server:/path/to/deploy"
echo ""
echo "=========================================="
echo "构建完成！"
echo "=========================================="
