# app/main.py

from fastapi import FastAPI, Query
from app.rag_engine import recommend_jobs

app = FastAPI(
    title="JobRadar: RAG-помощник",
    description="Находит подходящие вакансии по описанию с помощью Qdrant и TogetherAI",
    version="1.0.0"
)

@app.get("/recommend")
def recommend(query: str = Query(..., description="Описание желаемой вакансии")):
    """
    Возвращает рекомендации на основе запроса и найденных вакансий
    """
    try:
        result = recommend_jobs(query)
        return {"query": query, "response": result}
    except Exception as e:
        return {"error": str(e)}
