import os
import logging
from typing import Dict, List, Any, Optional, Set

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
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

QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EMBED_MODEL = os.getenv("EMBED_MODEL")
MODEL_DIR = os.getenv("MODEL_DIR")

model = SentenceTransformer(MODEL_DIR if MODEL_DIR else EMBED_MODEL)
qdrant = QdrantClient(url=QDRANT_URL, prefer_grpc=False)

# === Метрики Prometheus ===
BOT_REQUESTS = Counter("bot_requests_total", "Общее количество запросов к боту")
BOT_ACTIVE_USERS = Gauge("bot_active_users", "Уникальные пользователи за сессию")
active_users = set()

# === кеш фильтров ===
ROLES_CACHE: List[str] = []
AREAS_CACHE: List[str] = []
PAGE_SIZE = 10

# === меню ===
MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton("🔎 Векторный поиск"), KeyboardButton("🎛 По фильтрам")],
        [KeyboardButton("ℹ️ Помощь")],
    ],
    resize_keyboard=True,
)


def active_filter() -> Filter:
    return Filter(must=[FieldCondition(key="is_active", match=MatchValue(value=True))])


def refresh_filters_cache(limit_points: int = 20000) -> None:
    """Собирает уникальные professional_roles_name (str) и area_name (str) из Qdrant."""
    global ROLES_CACHE, AREAS_CACHE

    roles: Set[str] = set()
    areas: Set[str] = set()

    offset = None
    seen_points = 0

    while True:
        points, offset = qdrant.scroll(
            collection_name=QDRANT_COLLECTION,
            limit=256,
            offset=offset,
            with_payload=True,
            with_vectors=False,
            scroll_filter=active_filter(),
        )
        if not points:
            break

        for pt in points:
            p = pt.payload or {}

            r = p.get("professional_roles_name")
            if isinstance(r, str):
                r = r.strip()
                if r:
                    roles.add(r)

            a = p.get("area_name")
            if isinstance(a, str):
                a = a.strip()
                if a:
                    areas.add(a)

        seen_points += len(points)
        if offset is None or seen_points >= limit_points:
            break

    ROLES_CACHE = sorted(roles)
    AREAS_CACHE = sorted(areas)

    logger.info(f"Filters cache refreshed: roles={len(ROLES_CACHE)} areas={len(AREAS_CACHE)}")


def build_list_kb(kind: str, items: List[str], page: int = 0) -> InlineKeyboardMarkup:
    start = page * PAGE_SIZE
    chunk = items[start:start + PAGE_SIZE]

    rows = [[InlineKeyboardButton(x, callback_data=f"flt:{kind}:pick:{page}:{x}")] for x in chunk]

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"flt:{kind}:page:{page-1}"))
    if start + PAGE_SIZE < len(items):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"flt:{kind}:page:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("🔄 Обновить список", callback_data="flt:refresh")])
    rows.append([InlineKeyboardButton("❌ Сбросить", callback_data="flt:reset")])

    return InlineKeyboardMarkup(rows)


def build_nav_kb(idx: int, total: int, url: str) -> InlineKeyboardMarkup:
    prev_btn = InlineKeyboardButton("⬅️", callback_data="nav:prev")
    next_btn = InlineKeyboardButton("➡️", callback_data="nav:next")
    counter = InlineKeyboardButton(f"{idx+1}/{total}", callback_data="nav:noop")

    rows = [[prev_btn, counter, next_btn]]
    if isinstance(url, str) and url.startswith("http"):
        rows.append([InlineKeyboardButton("🔗 Открыть вакансию", url=url)])
    return InlineKeyboardMarkup(rows)


def retrieve(query: str, k: int = 5, fetch: int = 50) -> List[Dict[str, Any]]:
    vec = model.encode([query], normalize_embeddings=True)[0].tolist()

    hits = qdrant.query_points(
        collection_name=QDRANT_COLLECTION,
        query=vec,
        limit=fetch,
        with_payload=True,
        with_vectors=False,
        query_filter=active_filter(),
    ).points

    
    seen = set()
    items: List[Dict[str, Any]] = []
    for h in hits:
        p = h.payload or {}
        url = (p.get("url") or p.get("alternate_url") or "").strip()
        title_key = (p.get("title") or p.get("name") or "").strip().lower()
        company_key = (p.get("company") or p.get("employer") or "").strip().lower()

        key = url if url else f"{title_key}::{company_key}"
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
            "area_name": (p.get("area_name") or ""),
            "professional_roles_name": (p.get("professional_roles_name") or ""),
        })
        if len(items) >= k:
            break

    return items


def retrieve_by_filters(role: Optional[str], area: Optional[str], limit: int = 400) -> List[Dict[str, Any]]:
    must = [FieldCondition(key="is_active", match=MatchValue(value=True))]

    if role:
        must.append(FieldCondition(key="professional_roles_name", match=MatchValue(value=role)))
    if area:
        must.append(FieldCondition(key="area_name", match=MatchValue(value=area)))

    flt = Filter(must=must)

    points, _ = qdrant.scroll(
        collection_name=QDRANT_COLLECTION,
        limit=limit,
        with_payload=True,
        with_vectors=False,
        scroll_filter=flt,
    )

    seen = set()
    out: List[Dict[str, Any]] = []
    for pt in points:
        p = pt.payload or {}
        url = (p.get("url") or p.get("alternate_url") or "").strip()
        title_key = (p.get("title") or p.get("name") or "").strip().lower()
        company_key = (p.get("company") or p.get("employer") or "").strip().lower()

        key = url if url else f"{title_key}::{company_key}"
        if not key or key in seen:
            continue
        seen.add(key)

        out.append({
            "id": pt.id,
            "title": p.get("title") or p.get("name") or "-",
            "company": p.get("company") or p.get("employer") or "-",
            "experience": p.get("experience") or "-",
            "description": p.get("description") or "",
            "snippet": p.get("snippet") or "",
            "url": url or "-",
            "salary_text": p.get("salary_text") or p.get("salary_str") or "",
            "area_name": (p.get("area_name") or ""),
            "professional_roles_name": (p.get("professional_roles_name") or ""),
        })

    return out


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["mode"] = None
    await update.message.reply_text(
        "Выбери режим поиска:",
        reply_markup=MAIN_MENU,
    )


async def on_filters_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["mode"] = "filters"
    ctx.user_data.pop("flt_role", None)
    ctx.user_data.pop("flt_area", None)

    if not ROLES_CACHE or not AREAS_CACHE:
        refresh_filters_cache()

    if not ROLES_CACHE:
        await update.message.reply_text("Ролей пока не нашёл в базе 😕", reply_markup=MAIN_MENU)
        return

    await update.message.reply_text("Выбери роль:", reply_markup=build_list_kb("role", ROLES_CACHE, 0))


async def on_vector_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["mode"] = "vector"
    await update.message.reply_text("Ок, напиши запрос (например: 'NLP, удалёнка'):", reply_markup=MAIN_MENU)


async def on_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔎 Векторный поиск — вводишь текст и получаешь топ вакансий.\n"
        "🎛 По фильтрам — выбираешь роль и город, потом листаешь.\n\n"
        "Если что — нажми /start чтобы вернуться в меню.",
        reply_markup=MAIN_MENU,
    )


async def on_filters_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    data = q.data or ""
    if not data.startswith("flt:"):
        return

    await q.answer()

    if data == "flt:refresh":
        refresh_filters_cache()
        await q.edit_message_text("Выбери роль:", reply_markup=build_list_kb("role", ROLES_CACHE, 0))
        return

    if data == "flt:reset":
        ctx.user_data.pop("flt_role", None)
        ctx.user_data.pop("flt_area", None)
        await q.edit_message_text("Фильтры сброшены. Выбери роль:", reply_markup=build_list_kb("role", ROLES_CACHE, 0))
        return

    parts = data.split(":", 3)
    if len(parts) < 3:
        return

    kind = parts[1]

    if parts[2] == "page" and len(parts) == 4:
        page = int(parts[3])
        items = ROLES_CACHE if kind == "role" else AREAS_CACHE
        await q.edit_message_text(
            "Выбери роль:" if kind == "role" else "Выбери город:",
            reply_markup=build_list_kb(kind, items, page),
        )
        return

    if parts[2] == "pick" and len(parts) == 4:
        rest = parts[3]
        if ":" not in rest:
            return
        _page_s, value = rest.split(":", 1)
        value = value.strip()

        if kind == "role":
            ctx.user_data["flt_role"] = value
            if not AREAS_CACHE:
                refresh_filters_cache()
            await q.edit_message_text(
                f"Роль: {value}\nТеперь выбери город:",
                reply_markup=build_list_kb("area", AREAS_CACHE, 0),
            )
            return

        if kind == "area":
            ctx.user_data["flt_area"] = value
            role = ctx.user_data.get("flt_role")
            area = ctx.user_data.get("flt_area")

            await q.edit_message_text(f"Ищу вакансии: роль={role}, город={area}…")

            docs = retrieve_by_filters(role=role, area=area, limit=600)
            if not docs:
                await q.edit_message_text("Ничего не нашёл. Выбери роль:", reply_markup=build_list_kb("role", ROLES_CACHE, 0))
                return

            ctx.user_data["results"] = docs[:50]
            ctx.user_data["idx"] = 0

            doc0 = ctx.user_data["results"][0]
            card_text, _debug = make_short_card_embed(doc0, model)
            kb = build_nav_kb(0, len(ctx.user_data["results"]), doc0.get("url", ""))

            await q.edit_message_text(
                card_text[:3900],
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            return


async def on_nav(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    data = q.data or ""
    if not data.startswith("nav:"):
        return

    await q.answer()

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


async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text:
        return

    if text == "🔎 Векторный поиск":
        await on_vector_entry(update, ctx)
        return

    if text == "🎛 По фильтрам":
        await on_filters_entry(update, ctx)
        return

    if text == "ℹ️ Помощь":
        await on_help(update, ctx)
        return

    mode = ctx.user_data.get("mode")

    if mode != "vector":
        await update.message.reply_text("Сначала выбери режим в меню 👇", reply_markup=MAIN_MENU)
        return

    BOT_REQUESTS.inc()
    if update.effective_user:
        active_users.add(update.effective_user.id)
        BOT_ACTIVE_USERS.set(len(active_users))

    await update.message.reply_text("🔎 Ищу подходящие вакансии…", reply_markup=MAIN_MENU)

    docs = retrieve(text, k=5)
    if not docs:
        await update.message.reply_text("Пока ничего не нашёл. Попробуй уточнить запрос.", reply_markup=MAIN_MENU)
        return

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


def main():
    if not TG_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан")

    start_http_server(8000)

    app = Application.builder().token(TG_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    app.add_handler(CallbackQueryHandler(on_filters_callback, pattern=r"^flt:"))
    app.add_handler(CallbackQueryHandler(on_nav, pattern=r"^nav:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    try:
        refresh_filters_cache()
    except Exception:
        logger.exception("Failed to refresh filters cache at startup")

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
