from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import os 

import numpy as np
from sentence_transformers import SentenceTransformer

MODEL_DIR = os.getenv("MODEL_DIR")
EMBED_MODEL = os.getenv("EMBED_MODEL")
_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+")
_BULLET_PREFIX = re.compile(r"^\s*[-•*\u2022]\s+")

def split_sentences(text: str) -> List[str]:
    """Минимально-адекватное разбиение вакансий на предложения."""
    if not text:
        return []
    # чуть чистим пробелы, но без тяжелого html-парсинга
    t = re.sub(r"\s+", " ", text).strip()
    parts = _SENT_SPLIT.split(t)
    out: List[str] = []
    for p in parts:
        p = p.strip()
        p = _BULLET_PREFIX.sub("", p)
        # отсекаем мусор и слишком длинные
        if 25 <= len(p) <= 240:
            out.append(p)
    return out


def l2_normalize(x: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(x, axis=1, keepdims=True) + 1e-12
    return x / n


@dataclass
class Anchor:
    name: str
    queries: List[str]
    k: int


DEFAULT_ANCHORS = [
    Anchor(
        name="duties",
        queries=[
            "Обязанности и задачи на позиции",
            "Что нужно делать на работе",
            "Responsibilities and duties",
        ],
        k=2,
    ),
    Anchor(
        name="company",
        queries=[
            "О компании: кто мы и чем занимаемся",
            "About the company",
            "О нас",
        ],
        k=1,
    ),
    Anchor(
        name="requirements",
        queries=[
            "Требования: стек технологий и навыки",
            "Tech stack and requirements",
            "Must have skills",
        ],
        k=2,
    ),
]


def pick_top_sentences(
    sents: List[str],
    sent_emb: np.ndarray,
    model: SentenceTransformer,
    anchor: Anchor,
    used_idx: set[int],
) -> List[str]:
    """Выбирает top-k предложений под якорь по cosine similarity."""
    if not sents:
        return []

    # эмбеддим якоря и усредняем
    a_emb = model.encode(anchor.queries, normalize_embeddings=True)
    a_vec = np.mean(a_emb, axis=0, keepdims=True)  # (1, d)
    # cosine т.к. всё нормализовано: dot
    scores = (sent_emb @ a_vec.T).reshape(-1)

    # сортировка по score desc
    order = np.argsort(-scores)

    picked: List[str] = []
    for idx in order:
        if idx in used_idx:
            continue
        s = sents[int(idx)]
        # лёгкая защита от "пустых" общих фраз
        if len(s) < 30:
            continue
        picked.append(s)
        used_idx.add(int(idx))
        if len(picked) >= anchor.k:
            break
    return picked


def shorten(s: str, max_len: int = 170) -> str:
    s = s.strip()
    if len(s) <= max_len:
        return s
    s2 = s[:max_len]
    # обрежем по пробелу
    if " " in s2:
        s2 = s2.rsplit(" ", 1)[0]
    return s2 + "…"


def make_short_card_embed(
    vac: Dict,
    model: SentenceTransformer,
    anchors: List[Anchor] = DEFAULT_ANCHORS,
) -> Tuple[str, Dict[str, List[str]]]:
    """
    Возвращает:
      - текст карточки (HTML-friendly для Telegram)
      - debug dict с выбранными предложениями по блокам
    Ожидаемые поля:
      title/name, snippet/description, company/employer, url/alternate_url, salary/salary_text
    """
    title = (vac.get("title") or vac.get("name") or "Вакансия").strip()
    company = (vac.get("company") or vac.get("employer") or "").strip()
    url = (vac.get("url") or vac.get("alternate_url") or vac.get("link") or "").strip()

    # зарплату лучше хранить отдельным полем при парсинге; тут просто подхватим строку
    salary = vac.get("salary_text") or vac.get("salary_str") or vac.get("salary")  # может быть dict — тогда покажется некрасиво
    if isinstance(salary, dict):
        salary = None

    text = (vac.get("description") or "") + " " + (vac.get("snippet") or "")
    sents = split_sentences(text)

    # если текста мало — fallback
    if len(sents) < 3:
        lines = [f"💼 <b>{title}</b>"]
        if salary:
            lines.append(f"💰 <b>{salary}</b>")
        if company:
            lines.append(f"🏢 {company}")
        if url:
            lines.append("")
            lines.append("👇 Полное описание — по кнопке ниже")
        return "\n".join(lines).strip(), {"fallback": sents}

    # эмбеддим предложения (нормализуем, чтобы cosine = dot)
    sent_emb = model.encode(sents, normalize_embeddings=True)

    used_idx: set[int] = set()
    chosen: Dict[str, List[str]] = {}

    for a in anchors:
        chosen[a.name] = pick_top_sentences(sents, sent_emb, model, a, used_idx)

    # Собираем карточку
    lines: List[str] = []
    lines.append(f"💼 <b>{title}</b>")
    if salary:
        lines.append(f"💰 <b>{salary}</b>")
    if company:
        lines.append(f"🏢 {company}")
    lines.append("")

    # "Стек/требования" — берём 1–2 предложения и режем
    req = chosen.get("requirements", [])
    if req:
        lines.append("🧰 <b>Стек / требования:</b>")
        for r in req:
            lines.append(f"• {shorten(r)}")
        lines.append("")

    duties = chosen.get("duties", [])
    if duties:
        lines.append("🛠 <b>Что делать:</b>")
        for d in duties:
            lines.append(f"• {shorten(d)}")
        lines.append("")

    comp = chosen.get("company", [])
    if comp:
        lines.append(f"📌 <b>О компании:</b> {shorten(comp[0], 200)}")
    else:
        lines.append("📌 <b>О компании:</b> Компания под NDA.")

    if url:
        lines.append("")
        lines.append("👇 Полное описание — по кнопке ниже")

    return "\n".join(lines).strip(), chosen


# --- пример инициализации модели (делай один раз при старте бота) ---
# model = SentenceTransformer(MODEL_DIR if MODEL_DIR else EMBED_MODEL)
