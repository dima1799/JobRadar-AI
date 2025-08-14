FROM python:3.10-slim

# Установка системных библиотек
RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    build-essential \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Установка pip-зависимостей
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Копируем код проекта в контейнер
COPY . /app




