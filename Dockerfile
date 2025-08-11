FROM python:3.10-slim

# Создаём рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости (если нужны, можно дописать)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Сначала копируем только requirements.txt
COPY requirements.txt .

# Обновляем pip и ставим зависимости
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Теперь копируем весь код
COPY . .


