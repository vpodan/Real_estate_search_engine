# 🚀 Руководство по развертыванию MCP сервера на OVH Cloud

Это полное руководство по развертыванию вашего Real Estate MCP сервера на OVH Cloud с использованием виртуальной машины и Docker.

## 📋 Содержание

1. [Подготовка OVH Cloud](#1-подготовка-ovh-cloud)
2. [Настройка виртуальной машины](#2-настройка-виртуальной-машины)
3. [Установка необходимого ПО](#3-установка-необходимого-по)
4. [Развертывание проекта](#4-развертывание-проекта)
5. [Настройка безопасности](#5-настройка-безопасности)
6. [Мониторинг и обслуживание](#6-мониторинг-и-обслуживание)

---

## 1. Подготовка OVH Cloud

### 1.1 Создание виртуальной машины

1. Войдите в [OVH Cloud Control Panel](https://www.ovh.com/manager/)
2. Перейдите в раздел **Public Cloud** → **Instances**
3. Нажмите **Create an instance**
4. Выберите конфигурацию:
   - **Модель**: B2-7 (2 vCPU, 7GB RAM) или выше
   - **Регион**: Ближайший к вашим пользователям
   - **ОС**: Ubuntu 22.04 LTS
   - **Хранилище**: минимум 40GB SSD

### 1.2 Настройка SSH ключа

Создайте SSH ключ (если еще не создан):

```bash
ssh-keygen -t rsa -b 4096 -C "your_email@example.com"
```

Добавьте публичный ключ в OVH при создании инстанса.

### 1.3 Настройка группы безопасности

Откройте следующие порты:
- **22** (SSH)
- **80** (HTTP)
- **443** (HTTPS)
- **10000** (MCP Server)

---

## 2. Настройка виртуальной машины

### 2.1 Подключение к серверу

```bash
ssh ubuntu@YOUR_SERVER_IP
```

Замените `YOUR_SERVER_IP` на IP адрес вашего сервера из OVH панели.

### 2.2 Обновление системы

```bash
sudo apt update && sudo apt upgrade -y
```

### 2.3 Настройка firewall

```bash
# Установка ufw (если не установлен)
sudo apt install ufw -y

# Разрешаем SSH
sudo ufw allow 22/tcp

# Разрешаем HTTP/HTTPS
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Разрешаем MCP сервер
sudo ufw allow 10000/tcp

# Включаем firewall
sudo ufw enable

# Проверяем статус
sudo ufw status
```

---

## 3. Установка необходимого ПО

### 3.1 Установка Docker

```bash
# Удаляем старые версии
sudo apt remove docker docker-engine docker.io containerd runc

# Устанавливаем зависимости
sudo apt install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# Добавляем официальный GPG ключ Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Добавляем репозиторий Docker
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Устанавливаем Docker
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Добавляем текущего пользователя в группу docker
sudo usermod -aG docker $USER

# Применяем изменения группы
newgrp docker

# Проверяем установку
docker --version
docker compose version
```

### 3.2 Установка Git

```bash
sudo apt install -y git
git --version
```

### 3.3 Установка nginx (опционально, для reverse proxy)

```bash
sudo apt install -y nginx
sudo systemctl enable nginx
sudo systemctl start nginx
```

---

## 4. Развертывание проекта

### 4.1 Клонирование репозитория

```bash
# Создаем директорию для проектов
mkdir -p ~/projects
cd ~/projects

# Клонируем репозиторий (замените на ваш URL)
git clone https://github.com/YOUR_USERNAME/llm_whatsapp_bot.git
cd llm_whatsapp_bot
```

**Альтернативный способ**: Загрузите файлы с помощью `scp`:

```bash
# На вашем локальном компьютере (Windows PowerShell)
scp -r C:\Vova\IT\Seminarium_2\llm_whatsapp_bot ubuntu@YOUR_SERVER_IP:~/projects/
```

### 4.2 Настройка переменных окружения

```bash
# Создаем .env файл
cp .env.example .env

# Редактируем .env
nano .env
```

Добавьте ваши настройки:

```env
OPENAI_API_KEY=sk-your-actual-openai-api-key-here
MONGODB_URI=mongodb://mongo:27017
MONGODB_DB=real_estate
PORT=10000
```

Сохраните файл: `Ctrl+X`, затем `Y`, затем `Enter`

### 4.3 Создание необходимых директорий

```bash
# Создаем директорию для ChromaDB
mkdir -p chroma_real_estate

# Устанавливаем права доступа
chmod -R 755 chroma_real_estate
```

### 4.4 Запуск проекта

```bash
# Делаем скрипт исполняемым
chmod +x deploy.sh

# Запускаем развертывание
./deploy.sh
```

**Альтернативный способ** (без скрипта):

```bash
# Сборка и запуск контейнеров
docker compose build
docker compose up -d

# Проверка статуса
docker compose ps

# Просмотр логов
docker compose logs -f
```

### 4.5 Проверка работы сервера

```bash
# Проверяем статус контейнеров
docker compose ps

# Проверяем логи MCP сервера
docker compose logs mcp-server

# Проверяем логи MongoDB
docker compose logs mongo

# Тестируем доступность
curl http://localhost:10000
```

---

## 5. Настройка безопасности

### 5.1 Настройка nginx как reverse proxy

Создайте конфигурацию nginx:

```bash
sudo nano /etc/nginx/sites-available/mcp-server
```

Добавьте следующую конфигурацию:

```nginx
server {
    listen 80;
    server_name YOUR_DOMAIN_OR_IP;

    location / {
        proxy_pass http://localhost:10000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Увеличиваем таймауты для длительных запросов
        proxy_connect_timeout 600;
        proxy_send_timeout 600;
        proxy_read_timeout 600;
        send_timeout 600;
    }
}
```

Активируйте конфигурацию:

```bash
# Создаем символическую ссылку
sudo ln -s /etc/nginx/sites-available/mcp-server /etc/nginx/sites-enabled/

# Проверяем конфигурацию
sudo nginx -t

# Перезапускаем nginx
sudo systemctl restart nginx
```

### 5.2 Установка SSL сертификата (Let's Encrypt)

**Важно**: Для SSL сертификата нужен домен. Если используете только IP, пропустите этот шаг.

```bash
# Установка certbot
sudo apt install -y certbot python3-certbot-nginx

# Получение сертификата
sudo certbot --nginx -d your-domain.com

# Автоматическое обновление сертификата
sudo certbot renew --dry-run
```

### 5.3 Настройка автоматического перезапуска

Создайте systemd сервис:

```bash
sudo nano /etc/systemd/system/mcp-server.service
```

Добавьте:

```ini
[Unit]
Description=Real Estate MCP Server
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/ubuntu/projects/llm_whatsapp_bot
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
```

Активируйте сервис:

```bash
sudo systemctl enable mcp-server.service
sudo systemctl start mcp-server.service
sudo systemctl status mcp-server.service
```

---

## 6. Мониторинг и обслуживание

### 6.1 Просмотр логов

```bash
# Все логи
docker compose logs -f

# Только MCP сервер
docker compose logs -f mcp-server

# Только MongoDB
docker compose logs -f mongo

# Последние 100 строк
docker compose logs --tail=100
```

### 6.2 Перезапуск сервисов

```bash
# Перезапуск всех сервисов
docker compose restart

# Перезапуск только MCP сервера
docker compose restart mcp-server

# Перезапуск только MongoDB
docker compose restart mongo
```

### 6.3 Обновление проекта

```bash
cd ~/projects/llm_whatsapp_bot

# Получаем последние изменения
git pull

# Пересобираем и перезапускаем
docker compose down
docker compose build --no-cache
docker compose up -d
```

### 6.4 Резервное копирование

```bash
# Создаем директорию для бэкапов
mkdir -p ~/backups

# Бэкап MongoDB
docker compose exec mongo mongodump --out /data/backup
docker cp real-estate-mongodb:/data/backup ~/backups/mongodb-$(date +%Y%m%d-%H%M%S)

# Бэкап ChromaDB
tar -czf ~/backups/chroma-$(date +%Y%m%d-%H%M%S).tar.gz chroma_real_estate/

# Автоматический бэкап (добавьте в crontab)
crontab -e
# Добавьте строку:
# 0 2 * * * cd ~/projects/llm_whatsapp_bot && docker compose exec mongo mongodump --out /data/backup
```

### 6.5 Мониторинг ресурсов

```bash
# Использование ресурсов контейнерами
docker stats

# Дисковое пространство
df -h

# Использование памяти
free -h

# Нагрузка на процессор
top
```

### 6.6 Очистка неиспользуемых ресурсов

```bash
# Удаление неиспользуемых образов
docker image prune -a

# Удаление неиспользуемых томов
docker volume prune

# Полная очистка Docker
docker system prune -a --volumes
```

---

## 📝 Полезные команды

### Управление Docker Compose

```bash
# Запуск в фоновом режиме
docker compose up -d

# Остановка
docker compose down

# Пересборка без кеша
docker compose build --no-cache

# Просмотр статуса
docker compose ps

# Вход в контейнер
docker compose exec mcp-server bash
docker compose exec mongo mongosh
```

### Проверка работы сервера

```bash
# Локальная проверка
curl http://localhost:10000

# Проверка с внешнего адреса
curl http://YOUR_SERVER_IP:10000

# Проверка MongoDB
docker compose exec mongo mongosh real_estate --eval "db.listings.countDocuments()"
```

---

## 🔧 Решение проблем

### Проблема: Контейнер не запускается

```bash
# Проверяем логи
docker compose logs mcp-server

# Проверяем конфигурацию
docker compose config

# Пересобираем образ
docker compose build --no-cache
docker compose up -d
```

### Проблема: Нет подключения к MongoDB

```bash
# Проверяем статус MongoDB
docker compose ps mongo

# Проверяем логи MongoDB
docker compose logs mongo

# Перезапускаем MongoDB
docker compose restart mongo
```

### Проблема: Недостаточно памяти

```bash
# Проверяем использование памяти
free -h
docker stats

# Увеличьте swap
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

### Проблема: Порт 10000 недоступен

```bash
# Проверяем, что порт открыт
sudo ufw status

# Проверяем, что сервис слушает порт
sudo netstat -tulpn | grep 10000

# Проверяем с внешнего адреса
telnet YOUR_SERVER_IP 10000
```

---

## 🌐 Доступ к серверу

После успешного развертывания, ваш MCP сервер будет доступен по адресу:

- **Прямой доступ**: `http://YOUR_SERVER_IP:10000`
- **Через nginx**: `http://YOUR_DOMAIN_OR_IP`
- **С SSL**: `https://YOUR_DOMAIN` (если настроили SSL)

---

## 📞 Контакты и поддержка

Если возникнут проблемы, проверьте:
1. Логи контейнеров: `docker compose logs -f`
2. Статус сервисов: `docker compose ps`
3. Доступность портов: `sudo ufw status`
4. Использование ресурсов: `docker stats`

---

**Готово!** Ваш MCP сервер развернут и готов к использованию! 🎉

