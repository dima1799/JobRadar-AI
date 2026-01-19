import os
import re
import time
from datetime import datetime, timezone
from typing import Optional, List, Any, Dict, Tuple

import httpx
from qdrant_client import QdrantClient

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "vacancies")

SCROLL_LIMIT = int(os.getenv("QDRANT_SCROLL_LIMIT", "256"))
UPDATE_BATCH = int(os.getenv("QDRANT_UPDATE_BATCH", "128"))
SLEEP_SEC = float(os.getenv("HH_SLEEP_SEC", "0.35"))

HH_API = "https://api.hh.ru/vacancies/{}"
HH_ID_RE = re.compile(r"/vacancy/(\d+)", re.IGNORECASE)


def extract_hh_id(url: str) -> Optional[str]:
    if not url:
        return None
    m = HH_ID_RE.search(url)
    return m.group(1) if m else None


def fetch_meta(hc: httpx.Client, hh_id: str) -> Tuple[Optional[List[str]], Optional[str]]:
    """
    returns (roles_names, area_name)
    """
    r = hc.get(HH_API.format(hh_id))
    if r.status_code == 404:
        return None, None

    r.raise_for_status()
    data = r.json()

    # professional roles
    roles = data.get("professional_roles") or []
    role_names = []
    for rr in roles:
        name = (rr or {}).get("name")
        if name:
            role_names.append(str(name).strip())

    # uniq preserve order
    seen = set()
    uniq_roles = []
    for n in role_names:
        if n not in seen:
            seen.add(n)
            uniq_roles.append(n)

    # area
    area_name = None
    area = data.get("area")
    if isinstance(area, dict):
        area_name = area.get("name")

    return (uniq_roles or None), area_name


def flush_payload(q: QdrantClient, ids: List[Any], payload: Dict):
    for i in range(0, len(ids), UPDATE_BATCH):
        q.set_payload(
            collection_name=QDRANT_COLLECTION,
            payload=payload,
            points=ids[i:i + UPDATE_BATCH],
        )


def main():
    q = QdrantClient(url=QDRANT_URL, prefer_grpc=False)

    offset = None
    points_seen = 0
    checked = 0
    updated = 0
    skipped_no_hh = 0
    skipped_http = 0

    with httpx.Client(timeout=20, headers={"User-Agent": "JobRadar-AI backfill"}) as hc:
        while True:
            points, offset = q.scroll(
                collection_name=QDRANT_COLLECTION,
                limit=SCROLL_LIMIT,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )

            if not points:
                break

            points_seen += len(points)

            # группируем по одинаковому payload чтобы меньше set_payload дергать
            groups: Dict[Tuple[str, str], List[Any]] = {}

            for pt in points:
                p = pt.payload or {}
                url = (p.get("url") or p.get("alternate_url") or "").strip()

                hh_id = extract_hh_id(url)
                if not hh_id:
                    skipped_no_hh += 1
                    continue

                try:
                    roles, area_name = fetch_meta(hc, hh_id)
                    checked += 1
                except Exception:
                    skipped_http += 1
                    continue

                time.sleep(SLEEP_SEC)

                # если HH ничего не вернул — пропускаем
                if not roles and not area_name:
                    continue

                roles_str = ", ".join(roles) if roles else ""
                area_str = area_name or ""

                key = (roles_str, area_str)
                groups.setdefault(key, []).append(pt.id)

            for (roles_str, area_str), ids in groups.items():
                payload = {
                    "professional_roles_name": roles_str,
                    "area_name": area_str,
                }

                flush_payload(q, ids, payload)
                updated += len(ids)

            if offset is None:
                break

    print(
        f"points_seen={points_seen} checked_hh={checked} "
        f"updated={updated} skipped_no_hh={skipped_no_hh} skipped_http={skipped_http}"
    )


if __name__ == "__main__":
    main()
