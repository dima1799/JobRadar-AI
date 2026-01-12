import os, logging, asyncio
from typing import Dict, List, Optional, Tuple,Any
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
import httpx
from prometheus_client import start_http_server, Counter, Gauge 

from make_short_card import make_short_card_embed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("jobradar-bot")

QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION")
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EMBED_MODEL = os.getenv("EMBED_MODEL")
MODEL_DIR = os.getenv("MODEL_DIR") 

model = SentenceTransformer(MODEL_DIR if MODEL_DIR else EMBED_MODEL)
qdrant = QdrantClient(url=QDRANT_URL, prefer_grpc=False)

# === Метрики Prometheus ===
BOT_REQUESTS = Counter("bot_requests_total", "Общее количество запросов к боту")
BOT_ACTIVE_USERS = Gauge("bot_active_users", "Уникальные пользователи за сессию")
active_users = set()

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Напиши предпочтения по работе (например: 'ML engineer, NLP, удалёнка'), "
        "я подберу релевантные вакансии."
    )


def retrieve(query: str, k: int = 5) -> List[Dict[str, Any]]:
    vec = model.encode([query], normalize_embeddings=True)[0].tolist()

    hits = qdrant.query_points(
        collection_name=QDRANT_COLLECTION,
        query=vec,
        limit=k,
        with_payload=True,
        with_vectors=False,
    ).points

    items = []
    for h in hits:
        p = h.payload or {}
        items.append({
            "id": h.id,
            "score": h.score,
            "title": p.get("title") or p.get("name") or "-",
            "company": p.get("company") or p.get("employer") or "-",
            "experience": p.get("experience") or "-",
            "description": p.get("description") or "",
            "snippet": p.get("snippet") or "",
            "url": p.get("url") or p.get("alternate_url") or "-",
            "salary_text": p.get("salary_text") or p.get("salary_str") or "",
        })
    return items


def build_kb(url: str) -> InlineKeyboardMarkup:
    btn = InlineKeyboardButton("🔗 Открыть вакансию", url=url)
    return InlineKeyboardMarkup([[btn]])


async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = (update.message.text or "").strip()
    if not query:
        return

    BOT_REQUESTS.inc()
    active_users.add(update.effective_user.id)
    BOT_ACTIVE_USERS.set(len(active_users))

    await update.message.reply_text("🔎 Ищу подходящие вакансии…")

    docs = retrieve(query, k=5)
    if not docs:
        await update.message.reply_text("Пока ничего не нашёл. Попробуй уточнить запрос.")
        return

    # Отправляем вакансии (каждая — отдельным сообщением)
    for doc in docs:
        try:
            card_text, _debug = make_short_card_embed(doc, model)  # HTML текст
            kb = build_kb(doc["url"]) if doc.get("url", "").startswith("http") else None

            await update.message.reply_text(
                card_text[:3900],
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        except Exception:
            logger.exception("Ошибка при отправке вакансии в Telegram")


def main():
    if not TG_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")

    start_http_server(8000)

    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
