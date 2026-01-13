from qdrant_client import QdrantClient
import os
from make_short_card import make_short_card_embed
from sentence_transformers import SentenceTransformer

QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION")
MODEL_DIR = os.getenv("MODEL_DIR")
EMBED_MODEL = os.getenv("EMBED_MODEL")

client = QdrantClient(url=QDRANT_URL, prefer_grpc=False)
COLLECTION = QDRANT_COLLECTION

points, next_offset = client.scroll(
    collection_name=COLLECTION,
    limit=1,
    with_payload=True,
    with_vectors=False,
)

vacancies = []
for p in points:
    payload = p.payload or {}
    vacancies.append({
        "title": payload.get("title"),
        "company": payload.get("company"),
        "experience": payload.get("experience"),
        "description": payload.get("description"),
        "url": payload.get("url"),
    })

model = SentenceTransformer(MODEL_DIR if MODEL_DIR else EMBED_MODEL)

for vac in vacancies:
    text, debug = make_short_card_embed(vac, model)
    print("="*80)
    print(text)
    print("DEBUG:", {k: len(v) for k,v in debug.items()})
