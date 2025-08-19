# JobRadar-AI

🚀 Telegram-бот для поиска вакансий с помощью Retrieval-Augmented Generation (RAG).  
Бот ищет релевантные вакансии, хранящиеся в Qdrant, и формирует краткий ответ с помощью LLM (Together API).
@aijobradar_bot
---

## ✨ Основные возможности

- 🔍 Поиск вакансий по свободному запросу пользователя
- 🧠 Векторный поиск через [Qdrant](https://qdrant.tech/)
- 📝 Генерация ответа с помощью Together API
- 🧩 Эмбеддинг модель: [`deepvk/USER-bge-m3`](https://huggingface.co/deepvk/USER-bge-m3)
- ⚙️ ETL-пайплайн через Apache Airflow (ежедневная загрузка вакансий с hh.ru по prof_name=data scientist)
- 🐳 Контейнеризация с Docker Compose

---

## 🧩 Архитектура
```text
Telegram → run_bot.py → Qdrant (поиск вакансий) → LLM (Together API) → Ответ пользователю
                      ↑
                  Embeddings
                      ↑
        Airflow DAG (парсинг hh.ru → Qdrant)

## Структура 
├── app/
│   ├── run_bot.py           # Запуск Telegram-бота
│   ├── hh_parser.py         # Парсер вакансий hh.ru
│   └── upload_to_qdrant.py  # Загрузка эмбеддингов в Qdrant
├── dags/
│   └── daily_job_ingest.py  # DAG для Airflow
├── docker-compose.yml       # Сборка и запуск сервисов
├── Dockerfile               # Образ для бота
├── requirements.txt
├── .env                     
└── README.md

## TODO 
 Улучшить форматирование ответа (каждая вакансия отдельным сообщением)
 Добавить анти-дублирование вакансий

```text
