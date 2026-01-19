import os
import logging
from typing import Dict, List, Any

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

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

from prometheus_client import start_http_server, Counter, Gauge

from make_short_card import make_short_card_embed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("jobradar-bot")

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "vacancies")
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
        "я подберу релевантные вакансии.\n\n"
    )


def retrieve(query: str, k: int = 5, fetch: int = 50):
    vec = model.encode([query], normalize_embeddings=True)[0].tolist()
    
    flt = Filter(
        must=[
        FieldCondition(
            key="is_active",
            match=MatchValue(value=True)
                )
            ]
        )
    hits = qdrant.query_points(
        collection_name=QDRANT_COLLECTION,
        query=vec,
        limit=fetch,
        with_payload=True,
        with_vectors=False,
        query_filter=flt, 
    ).points

    seen = set()
    items = []
    for h in hits:
        p = h.payload or {}
        url = (p.get("url") or p.get("alternate_url") or "").strip()
        title = (p.get("title") or p.get("name") or "").strip().lower()
        company = (p.get("company") or p.get("employer") or "").strip().lower()

        key = url if url else f"{title}::{company}"
        if not key or key in seen:
            continue
        seen.add(key)

        items.append({
            "id": h.id,
            "score": h.score,
            "title": p.get("title") or p.get("name") or "-",
            "company": p.get("company") or p.get("employer") or "-",
            "experience": p.get("experience") or "-",
            "description": p.get("description") or "",
            "snippet": p.get("snippet") or "",
            "url": url or "-",
            "salary_text": p.get("salary_text") or p.get("salary_str") or "",
        })
        if len(items) >= k:
            break

    return items


def build_nav_kb(idx: int, total: int, url: str) -> InlineKeyboardMarkup:
    prev_btn = InlineKeyboardButton("⬅️", callback_data="nav:prev")
    next_btn = InlineKeyboardButton("➡️", callback_data="nav:next")
    counter = InlineKeyboardButton(f"{idx+1}/{total}", callback_data="nav:noop")

    rows = [[prev_btn, counter, next_btn]]
    if isinstance(url, str) and url.startswith("http"):
        rows.append([InlineKeyboardButton("🔗 Открыть вакансию", url=url)])

    return InlineKeyboardMarkup(rows)


async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = (update.message.text or "").strip()
    if not query:
        return

    BOT_REQUESTS.inc()
    if update.effective_user:
        active_users.add(update.effective_user.id)
        BOT_ACTIVE_USERS.set(len(active_users))

    await update.message.reply_text("🔎 Ищу подходящие вакансии…")

    docs = retrieve(query, k=5)
    if not docs:
        await update.message.reply_text("Пока ничего не нашёл. Попробуй уточнить запрос.")
        return

    # сохраняем результаты для листания
    ctx.user_data["results"] = docs
    ctx.user_data["idx"] = 0

    doc0 = docs[0]
    card_text, _debug = make_short_card_embed(doc0, model)
    kb = build_nav_kb(0, len(docs), doc0.get("url", ""))

    await update.message.reply_text(
        card_text[:3900],
        parse_mode="HTML",
        reply_markup=kb,
        disable_web_page_preview=True,
    )


async def on_nav(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    data = q.data or ""
    if not data.startswith("nav:"):
        return

    docs = ctx.user_data.get("results")
    if not docs:
        return

    if data == "nav:noop":
        return

    idx = int(ctx.user_data.get("idx", 0))
    total = len(docs)

    if data == "nav:next":
        idx = min(idx + 1, total - 1)
    elif data == "nav:prev":
        idx = max(idx - 1, 0)

    ctx.user_data["idx"] = idx
    doc = docs[idx]

    card_text, _debug = make_short_card_embed(doc, model)
    kb = build_nav_kb(idx, total, doc.get("url", ""))

    await q.edit_message_text(
        card_text[:3900],
        parse_mode="HTML",
        reply_markup=kb,
        disable_web_page_preview=True,
    )


def main():
    if not TG_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")

    start_http_server(8000)

    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_nav))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
