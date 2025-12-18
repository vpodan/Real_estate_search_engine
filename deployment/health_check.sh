#!/bin/bash

# Скрипт для проверки здоровья MCP сервера
# Использование: ./health_check.sh

echo "🏥 Проверка здоровья MCP сервера..."
echo ""

# Цвета
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Функция проверки
check_service() {
    local service_name=$1
    local check_command=$2
    
    if eval $check_command > /dev/null 2>&1; then
        echo -e "${GREEN}✅ $service_name: OK${NC}"
        return 0
    else
        echo -e "${RED}❌ $service_name: FAIL${NC}"
        return 1
    fi
}

# Проверка Docker
check_service "Docker" "docker --version"

# Проверка Docker Compose
check_service "Docker Compose" "docker compose version"

# Проверка контейнеров
echo ""
echo "📦 Статус контейнеров:"
docker compose ps

# Проверка MCP сервера
echo ""
if docker compose ps | grep -q "mcp-server.*Up"; then
    echo -e "${GREEN}✅ MCP сервер запущен${NC}"
else
    echo -e "${RED}❌ MCP сервер не запущен${NC}"
fi

# Проверка MongoDB
if docker compose ps | grep -q "mongo.*Up"; then
    echo -e "${GREEN}✅ MongoDB запущен${NC}"
else
    echo -e "${RED}❌ MongoDB не запущен${NC}"
fi

# Проверка порта 10000
echo ""
if nc -z localhost 10000 2>/dev/null; then
    echo -e "${GREEN}✅ Порт 10000 доступен${NC}"
else
    echo -e "${RED}❌ Порт 10000 недоступен${NC}"
fi

# Использование ресурсов
echo ""
echo "💾 Использование ресурсов:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

# Дисковое пространство
echo ""
echo "📊 Дисковое пространство:"
df -h | grep -E "Filesystem|/$|/home"

# Последние логи
echo ""
echo "📝 Последние 10 строк логов MCP сервера:"
docker compose logs --tail=10 mcp-server

echo ""
echo "✨ Проверка завершена!"

