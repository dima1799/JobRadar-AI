# app/rag_engine.py

import os
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
from openai import OpenAI
from dotenv import load_dotenv

# 1. Загружаем API ключ из .env
load_dotenv()
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY")

# 2. Инициализация клиента Together AI
client_llm = OpenAI(
    api_key=TOGETHER_API_KEY,
    base_url="https://api.together.xyz/v1"
)

# 3. Qdrant и эмбеддинг модель
qdrant = QdrantClient(url="http://localhost:6333")
model = SentenceTransformer("deepvk/USER-bge-m3")

# 4. Функция генерации
def generate_response(prompt: str, max_tokens: int = 512):
    response = client_llm.chat.completions.create(
        model="mistralai/Mixtral-8x7B-Instruct-v0.1",
        messages=[
            {"role": "system", "content": "Ты — помощник по поиску работы."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.4,
        max_tokens=max_tokens,
    )
    return response.choices[0].message.content

# 5. Основная функция рекомендаций
def recommend_jobs(user_query: str, collection_name="vacancies", top_k=5):
    # Эмбеддинг запроса
    query_vector = model.encode([user_query])[0].tolist()

    # Поиск в Qdrant
    results = qdrant.search(
        collection_name=collection_name,
        query_vector=query_vector,
        limit=top_k
    )

    # Формируем контекст
    context = "\n\n".join([
        f"{r.payload['title']} в {r.payload.get('company', '-')}, "
        f"опыт: {r.payload.get('experience', 'не указано')}, "
        f"описание: {r.payload.get('description', 'не указано')}, "
        f"URL: {r.payload.get('url', '-')}"
        for r in results
    ])

    # Prompt
    prompt = f"""
Я ищу работу с предпочтениями: {user_query}.
Вот релевантные вакансии:

{context}

Проанализируй описания и выведи:
1. Стек и задачи по каждой вакансии
2. Ссылку на каждую вакансию отдельно
"""

    return generate_response(prompt)
