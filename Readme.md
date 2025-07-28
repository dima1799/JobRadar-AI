# 💼 JobRadar AI — RAG-помощник по свежим вакансиям

## 🧠 Описание

JobRadar AI — это AI-ассистент, который каждый день парсит вакансии с популярных сайтов (Habr Career, hh.ru, RemoteOK и др.), индексирует их в векторную базу, а затем позволяет пользователю **задавать естественные вопросы** и **получать ответы** с помощью **Retrieval-Augmented Generation (RAG)**.

Примеры запросов:
- "Какие требования к ML-инженеру в Берлине?"
- "Есть ли вакансии с RAG и LangChain?"
- "Какие технологии чаще всего требуют Python-разработчики?"

---

## ⚙️ Технологический стек

| Компонент             | Технологии                             |
|-----------------------|----------------------------------------|
| 🧱 Векторная база     | Chroma / Qdrant / FAISS                |
| 🤖 RAG                | LangChain / llama-index + LLM (Mistral, Saiga, GPT API) |
| 📡 Парсинг вакансий   | Requests / BeautifulSoup / Scrapy      |
| 📅 Планировщик        | Apache Airflow                         |
| 📦 Обёртка            | Docker + Docker Compose                |
| 🖥️ Интерфейс          | FastAPI / Streamlit                    |

---


---

## 🚀 План разработки (MVP)

### 📍 Этап 1: Сбор вакансий
- [ ] Написать парсер для Habr Career/hh (название, описание, теги, зарплата)
- [ ] Хранить сырые данные в JSON/SQLite

### 📍 Этап 2: Обработка и эмбеддинги
- [ ] Разбить текст вакансии на чанки
- [ ] Получить эмбеддинги (`sentence-transformers` или `GTE`)
- [ ] Сохранить в Chroma / Qdrant

### 📍 Этап 3: DAG в Airflow
- [ ] DAG: парсинг → обработка → сохранение эмбеддингов
- [ ] Настроить расписание `@daily`

### 📍 Этап 4: RAG-интерфейс
- [ ] FastAPI endpoint `/ask` с вопросом
- [ ] Получение релевантных вакансий по эмбеддингам
- [ ] Ответ через LLM (локальный или API)

### 📍 Этап 5: Docker Compose
- [ ] Собрать сервисы в `docker-compose.yml`
- [ ] Включить:
  - Airflow
  - FastAPI
  - Chroma

---

## 🐳 Быстрый старт

```bash
git clone https://github.com/dima1799/jobradar-ai.git
cd jobradar-ai

# Установка зависимостей
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Или через Docker:
docker-compose up --build


