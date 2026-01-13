import os
import re
import time
from datetime import datetime, timezone
from typing import Optional, List, Any, Tuple

import httpx
from qdrant_client import QdrantClient

QDRANT_URL = os.getenv("QDRANT_URL")
COLLECTION = os.getenv("QDRANT_COLLECTION")

HH_API = "https://api.hh.ru/vacancies/{}"
HH_ID_RE = re.compile(r"/vacancy/(\d+)", re.IGNORECASE)


SLEEP_SEC = float(os.getenv("HH_VALIDATE_SLEEP_SEC", "0.35"))
SCROLL_LIMIT = int(os.getenv("QDRANT_SCROLL_LIMIT", "256"))
UPDATE_BATCH = int(os.getenv("QDRANT_UPDATE_BATCH", "128"))



def extract_hh_id(url: str) -> Optional[str]:
    if not url:
        return None
    m = HH_ID_RE.search(url)
    return m.group(1) if m else None


def check_archived(hc: httpx.Client, hh_id: str) -> Tuple[bool, str]:
    """
    returns (inactive, reason)
    inactive if archived=true OR 404
    """
    r = hc.get(HH_API.format(hh_id))
    if r.status_code == 404:
        return True, "404"
    r.raise_for_status()
    data = r.json()
    if bool(data.get("archived", False)):
        return True, "archived_true"
    return False, "active"


def flush_payload(q: QdrantClient, ids: List[Any], payload: dict):
    for i in range(0, len(ids), UPDATE_BATCH):
        q.set_payload(
            collection_name=COLLECTION,
            payload=payload,
            points=ids[i:i + UPDATE_BATCH],
        )


def main():
    q = QdrantClient(url=QDRANT_URL, prefer_grpc=False)

    offset = None
    total_points = 0
    total_checked = 0
    total_activated = 0
    total_deactivated = 0
    total_skipped = 0

    now = datetime.now(timezone.utc).isoformat()

    with httpx.Client(timeout=20, headers={"User-Agent": "JobRadar-AI validator"}) as hc:
        while True:
            points, offset = q.scroll(
                collection_name=COLLECTION,
                limit=SCROLL_LIMIT,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )

            if not points:
                break

            total_points += len(points)

            activate_ids: List[Any] = []
            deactivate_ids: List[Any] = []

            for pt in points:
                p = pt.payload or {}
                url = (p.get("url") or p.get("alternate_url") or "").strip()
                hh_id = extract_hh_id(url)

                if not hh_id:
                    total_skipped += 1
                    continue

                try:
                    inactive, _reason = check_archived(hc, hh_id)
                except Exception:
                    total_skipped += 1
                    continue

                total_checked += 1
                if inactive:
                    deactivate_ids.append(pt.id)
                else:
                    activate_ids.append(pt.id)

                time.sleep(SLEEP_SEC)

            if activate_ids:
                flush_payload(
                    q,
                    activate_ids,
                    {"is_active": True, "archived": False, "archived_checked_at": now},
                )
                total_activated += len(activate_ids)

            if deactivate_ids:
                flush_payload(
                    q,
                    deactivate_ids,
                    {"is_active": False, "archived": True, "archived_checked_at": now},
                )
                total_deactivated += len(deactivate_ids)

            if offset is None:
                break

    print(
        f"points_seen={total_points} checked_hh={total_checked} "
        f"activated={total_activated} deactivated={total_deactivated} skipped={total_skipped}"
    )


if __name__ == "__main__":
    main()
