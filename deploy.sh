#!/bin/bash

# Скрипт для развертывания MCP сервера на OVH Cloud
# Использование: ./deploy.sh

set -e

echo "🚀 Начало развертывания MCP сервера..."

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Проверка наличия .env файла
if [ ! -f .env ]; then
    echo -e "${RED}❌ Файл .env не найден!${NC}"
    echo -e "${YELLOW}Создайте файл .env на основе .env.example${NC}"
    echo "cp .env.example .env"
    echo "Затем отредактируйте .env и добавьте ваши настройки"
    exit 1
fi

# Проверка наличия Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker не установлен!${NC}"
    echo "Установите Docker: https://docs.docker.com/engine/install/"
    exit 1
fi

# Проверка наличия Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose не установлен!${NC}"
    echo "Установите Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi

echo -e "${GREEN}✅ Docker и Docker Compose установлены${NC}"

# Остановка существующих контейнеров
echo -e "${YELLOW}🛑 Остановка существующих контейнеров...${NC}"
docker-compose down

# Сборка образов
echo -e "${YELLOW}🔨 Сборка Docker образов...${NC}"
docker-compose build --no-cache

# Запуск контейнеров
echo -e "${YELLOW}▶️  Запуск контейнеров...${NC}"
docker-compose up -d

# Ожидание запуска сервисов
echo -e "${YELLOW}⏳ Ожидание запуска сервисов...${NC}"
sleep 10

# Проверка статуса контейнеров
echo -e "${YELLOW}📊 Статус контейнеров:${NC}"
docker-compose ps

# Проверка логов
echo -e "${YELLOW}📝 Последние логи:${NC}"
docker-compose logs --tail=20

echo -e "${GREEN}✅ Развертывание завершено!${NC}"
echo -e "${GREEN}🌐 MCP сервер доступен на http://localhost:10000${NC}"
echo ""
echo "Полезные команды:"
echo "  docker-compose logs -f              # Просмотр логов в реальном времени"
echo "  docker-compose ps                   # Статус контейнеров"
echo "  docker-compose restart mcp-server   # Перезапуск MCP сервера"
echo "  docker-compose down                 # Остановка всех контейнеров"
echo "  docker-compose up -d                # Запуск контейнеров в фоне"

