from pathlib import Path
import os
import math
import pandas as pd
import uuid
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import gc
import hashlib


SAVE_VACANCIES_AIRFLOW_PATH = os.getenv("SAVE_VACANCIES_AIRFLOW_PATH")
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION")
EMBED_MODEL = os.getenv("EMBED_MODEL")
BATCH_SIZE = 2

def main():

    df = pd.read_csv(SAVE_VACANCIES_AIRFLOW_PATH)
    for col in ["title","professional_roles_name", "company","experience", "description", "url","area_name"]: #"schedule_name"
        if col not in df.columns:
            df[col] = ""

    model = SentenceTransformer(EMBED_MODEL)
    dim = model.get_sentence_embedding_dimension()

    client = QdrantClient(url=QDRANT_URL)

    try:
        client.get_collection(QDRANT_COLLECTION)
    except Exception:
        client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE),
        )

    def make_point_id(row: pd.Series) -> str:
        """
        Детерминированный ID:
        - основной ключ: url (для hh.ru содержит vacancy/<id>)
        - fallback: title + company (если url пустой)
        """
        url = str(row.get("url", "") or "").strip()
        if url:
            return hashlib.md5(url.encode("utf-8")).hexdigest()

        title = str(row.get("title", "") or "").strip().lower()
        company = str(row.get("company", "") or "").strip().lower()
        return hashlib.md5(f"{title}::{company}".encode("utf-8")).hexdigest()

    def upsert_chunk(vectors, start_idx):
        points = []
        for offset, vec in enumerate(vectors):
            row = df.iloc[start_idx + offset]
            payload = {
                "title": row.get("title", ""),
                "company": row.get("company", ""),
                "professional_roles_name": row.get("professional_roles_name", ""),
                "experience": row.get("experience", ""),
                "description": row.get("description", ""),
                "url": row.get("url", ""),
                "area_name": row.get("area_name", ""),
            }

            pid = make_point_id(row)

            points.append(
                models.PointStruct(
                    id=pid,  
                    vector=vec.tolist() if hasattr(vec, "tolist") else list(vec),
                    payload=payload,
                )
            )
        client.upsert(collection_name=QDRANT_COLLECTION, points=points)

    n = len(df)
    steps = math.ceil(n / BATCH_SIZE)
    for s in tqdm(range(steps), desc="Upserting"):
        a = s * BATCH_SIZE
        b = min((s + 1) * BATCH_SIZE, n)

        batch_docs = (df.iloc[a:b]["title"].fillna("") + ". " + df.iloc[a:b]["description"].fillna("")).tolist()

        vecs = model.encode(
        batch_docs,
        batch_size=BATCH_SIZE,
        show_progress_bar=False,
        normalize_embeddings=True,
        )
        upsert_chunk(vecs, a)

    del vecs, batch_docs
    gc.collect()

    print(f"✅ Залили {n} документов в коллекцию '{QDRANT_COLLECTION}' ({QDRANT_URL})")
    print(f"📄 Источник CSV: {SAVE_VACANCIES_AIRFLOW_PATH}")

if __name__ == "__main__":
    main()
