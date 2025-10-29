# Используем официальный образ Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все файлы проекта
COPY . .

# Создаем директорию для ChromaDB
RUN mkdir -p /app/chroma_real_estate

# Открываем порт
EXPOSE 10000

# Устанавливаем переменные окружения
ENV PORT=10000
ENV PYTHONUNBUFFERED=1

# Команда запуска сервера
CMD ["python", "mcp_real_estate_server.py"]

