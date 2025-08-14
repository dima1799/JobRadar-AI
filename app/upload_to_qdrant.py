from pathlib import Path
import os
import math
import pandas as pd
import uuid
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# ---- пути ----
CSV_PATH = Path(os.getenv("CSV_PATH", "/opt/airflow/data/vacancies_hh.csv"))
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
COLLECTION    = os.getenv("QDRANT_COLLECTION", "vacancies")
EMBED_MODEL   = os.getenv("EMBED_MODEL", "deepvk/USER-bge-m3")
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "8"))

def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV не найден: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    # ожидаем колонки из парсера: title, company, experience, description, url
    for col in ["title", "company", "experience", "description", "url"]:
        if col not in df.columns:
            df[col] = ""

    docs = (df["title"].fillna("") + ". " + df["description"].fillna("")).tolist()

    model = SentenceTransformer(EMBED_MODEL)
    dim = model.get_sentence_embedding_dimension()

    client = QdrantClient(url=QDRANT_URL)

    # создаём коллекцию при отсутствии
    try:
        client.get_collection(COLLECTION)
    except Exception:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE),
        )

    def upsert_chunk(vectors, start_idx):
        points = []
        for offset, vec in enumerate(vectors):
            row = df.iloc[start_idx + offset]
            payload = {
                "title": row.get("title", ""),
                "company": row.get("company", ""),
                "experience": row.get("experience", ""),
                "description": row.get("description", ""),
                "url": row.get("url", ""),
            }
            points.append(
                models.PointStruct(
                    id=str(uuid.uuid4()),  # гарантированно уникальный ID
                    vector=vec.tolist() if hasattr(vec, "tolist") else list(vec),
                    payload=payload,
                )
            )
        client.upsert(collection_name=COLLECTION, points=points)

    n = len(docs)
    steps = math.ceil(n / BATCH_SIZE)
    for s in tqdm(range(steps), desc="Upserting"):
        a = s * BATCH_SIZE
        b = min((s + 1) * BATCH_SIZE, n)
        vecs = model.encode(docs[a:b], batch_size=BATCH_SIZE, show_progress_bar=False)
        upsert_chunk(vecs, a)

    print(f"✅ Залили {n} документов в коллекцию '{COLLECTION}' ({QDRANT_URL})")
    print(f"📄 Источник CSV: {CSV_PATH}")

if __name__ == "__main__":
    main()
