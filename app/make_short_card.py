from __future__ import annotations
from typing import Dict, Tuple

def _escape_html(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def make_short_card_embed(vac: Dict, model=None, max_len: int = 3500) -> Tuple[str, Dict]:
    title = (vac.get("title") or vac.get("name") or "Вакансия").strip()
    company = (vac.get("company") or vac.get("employer") or "").strip()
    url = (vac.get("url") or vac.get("alternate_url") or vac.get("link") or "").strip()

    salary = vac.get("salary_text") or vac.get("salary_str") or vac.get("salary")
    if isinstance(salary, dict):
        salary = None

    desc = (vac.get("description") or vac.get("snippet") or "").strip()
    desc = _escape_html(desc)

    lines = []
    lines.append(f"💼 <b>{_escape_html(title)}</b>")
    if company:
        lines.append(f"🏢 {_escape_html(company)}")
    if salary:
        lines.append(f"💰 <b>{_escape_html(str(salary))}</b>")
    lines.append("")
    if desc:
        if len(desc) > max_len:
            desc = desc[:max_len].rsplit(" ", 1)[0] + "…"
        lines.append(desc)
    else:
        lines.append("Описание не указано.")

    if url:
        lines.append("")
        lines.append("👇 Полное описание — по кнопке ниже")

    return "\n".join(lines).strip(), {"mode": "full_description"}
