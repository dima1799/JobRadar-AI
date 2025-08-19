import os, logging, asyncio
from typing import List
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("jobradar-bot")

QDRANT_URL_BOT = os.getenv("QDRANT_URL_BOT")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION")
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EMBED_MODEL = os.getenv("EMBED_MODEL")
MODEL_DIR = os.getenv("MODEL_DIR") 

model = SentenceTransformer(MODEL_DIR if MODEL_DIR else EMBED_MODEL)

qdrant = QdrantClient(url=QDRANT_URL_BOT)

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Напиши предпочтения по работе (например: 'ML engineer, NLP, удалёнка'), "
        "я подберу релевантные вакансии."
    )

def retrieve(query: str, k: int = 5):
    vec = model.encode([query])[0].tolist()
    hits = qdrant.search(collection_name=QDRANT_COLLECTION, query_vector=vec, limit=k)
    items = []
    for h in hits:
        p = h.payload or {}
        items.append({
            "score": h.score,
            "title": p.get("title", "-"),
            "company": p.get("company", "-"),
            "experience": p.get("experience", "-"),
            "description": p.get("description", "-")[:1200],
            "url": p.get("url", "-"),
        })
    return items

async def call_together(prompt: str, model_name: str = "meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo", max_tokens: int = 600):
    if not TOGETHER_API_KEY:
        return "🛑 TOGETHER_API_KEY не задан."
    url = "https://api.together.xyz/v1/chat/completions"
    headers = {"Authorization": f"Bearer {TOGETHER_API_KEY}", "Content-Type": "application/json"}
    body = {
        "model": model_name,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": max_tokens,
    }

    proxy_url = os.getenv("TOGETHER_PROXY")  
    proxies = {"all://": proxy_url} if proxy_url else None

    async with httpx.AsyncClient(proxies=proxies,timeout=60) as client:
        r = await client.post(url, headers=headers, json=body)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]

def build_prompt(prefs: str, docs: List[dict]) -> str:
    ctx = "\n\n".join(
        f"- {d['title']} — {d['company']} (опыт: {d['experience']})\n"
        f"  Описание: {d['description']}\n"
        f"  URL: {d['url']}"
        for d in docs
    )
    return (
        "Ты помощник по поиску работы. Пользователь описал предпочтения по вакансии.\n"
        "Проанализируй найденные вакансии и кратко перечисли стек, предполагаемые задачи и дай рекомендации.\n"
        "Структура ответа: 1) краткий обзор; 2) список вакансий с тезисами\n\n"
        f"Предпочтения пользователя: {prefs}\n\n"
        f"Релевантные вакансии:\n{ctx}"
    )

async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = (update.message.text or "").strip()
    if not query:
        return
    await update.message.reply_text("🔎 Ищу подходящие вакансии…")
    docs = retrieve(query, k=5)
    if not docs:
        await update.message.replyText("Пока ничего не нашёл. Попробуй уточнить запрос.")
        return
    prompt = build_prompt(query, docs)
    answer = await call_together(prompt)
    # добавим ссылки в конце
    links = "\n".join(f"• {d['title']} — {d['url']}" for d in docs)
    out = f"{answer}\n\n🔗 Ссылки:\n{links}"
    await update.message.reply_text(out[:3900], disable_web_page_preview=True)

def main():
    if not TG_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")
    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
