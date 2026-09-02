from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests

from app.netguard import limited_get


class NinnescahProviderError(RuntimeError):
    """Controlled error for Ninnescah provider failures."""


NINNESCAH_MAP_URL = os.getenv(
    "NINNESCAH_MAP_URL",
    "https://ninnescah.ebill.coop/maps/external_outage_web_map/",
).strip()
NINNESCAH_SUMMARY_URL = os.getenv("NINNESCAH_SUMMARY_URL", "").strip()
NINNESCAH_REQUEST_TIMEOUT = (3.0, 12.0)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "WeatherPower-Ninnescah-Provider/1.0",
            "Accept": "text/html,application/json,text/plain,*/*",
            "Referer": NINNESCAH_MAP_URL,
        }
    )
    return s


def _default_summary_url() -> Optional[str]:
    # For known NISC-hosted web maps, summary feeds are hosted on outagemap-data.cloud.coop
    host = (urlparse(NINNESCAH_MAP_URL).hostname or "").lower()
    if not host:
        return None
    slug = host.split(".", 1)[0].strip()
    if not slug:
        return None
    return f"https://outagemap-data.cloud.coop/{slug}/Hosted_Outage_Map/summary.json"


def _extract_total_customers_out(text: str) -> Optional[int]:
    m = re.search(r"\|\s*Total\s*\|\s*(\d+)\s*\|", text, flags=re.IGNORECASE)
    if m:
        return int(m.group(1))

    m2 = re.search(r"Total\s*[:|]\s*(\d+)", text, flags=re.IGNORECASE)
    if m2:
        return int(m2.group(1))

    return None


def _extract_last_updated(text: str) -> Optional[str]:
    m = re.search(r"Last Updated\s*:\s*([^\n\r<]+)", text, flags=re.IGNORECASE)
    if not m:
        return None
    raw = m.group(1).strip()
    return raw or None


def _extract_summary_total(payload: Dict[str, Any]) -> Optional[int]:
    outages = payload.get("outages")
    if not isinstance(outages, list):
        return None
    total = 0
    seen = False
    for item in outages:
        if not isinstance(item, dict):
            continue
        n = item.get("nbrOut")
        if isinstance(n, (int, float)):
            total += int(n)
            seen = True
        elif isinstance(n, str) and n.strip().isdigit():
            total += int(n.strip())
            seen = True
    return total if seen else 0


def fetch_ninnescah_outages(lat: float, lon: float, max_radius_km: float = 32.18688, debug: bool = False) -> Dict[str, Any]:
    """
    Fetch utility-wide outage summary for Ninnescah web map.

    Since this source is summary-only (no per-outage geometry endpoint exposed),
    return a synthetic nearest outage at the queried location when total outages > 0.
    """
    s = _session()
    url = NINNESCAH_SUMMARY_URL or _default_summary_url() or NINNESCAH_MAP_URL

    try:
        resp = limited_get(s, url, timeout=NINNESCAH_REQUEST_TIMEOUT)
        resp.raise_for_status()
        content_type = (resp.headers.get("content-type") or "").lower()
        text = resp.text
    except Exception as e:
        raise NinnescahProviderError(f"Ninnescah fetch failed: {type(e).__name__}: {e}") from e

    total_out: Optional[int] = None
    last_updated: Optional[str] = None

    if "json" in content_type or url.lower().endswith(".json"):
        try:
            payload = resp.json()
        except Exception:
            payload = None
        if isinstance(payload, dict):
            total_out = _extract_summary_total(payload)
            lu = payload.get("lastUpdate")
            if isinstance(lu, (int, float)):
                last_updated = (
                    datetime.fromtimestamp(float(lu) / 1000.0, tz=timezone.utc)
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z")
                )

    if total_out is None:
        total_out = _extract_total_customers_out(text)
        last_updated = last_updated or _extract_last_updated(text)

    if total_out is None:
        raise NinnescahProviderError(
            "Ninnescah summary parse failed. Set NINNESCAH_SUMMARY_URL to a summary feed endpoint if available."
        )

    outage = {
        "id": "NINNESCAH-SUMMARY",
        "outage_id": "NINNESCAH-SUMMARY",
        "cluster": True,
        "customers_out": total_out,
        "n_out": total_out,
        "etr": None,
        "cause": "Utility summary outage count",
        "start_time": last_updated or datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "latitude": lat,
        "longitude": lon,
        "distance_km": 0.0,
        "provider": "NINNESCAH_RURAL_ELECTRIC",
        "raw": {
            "source_url": url,
            "total_customers_out": total_out,
            "last_updated": last_updated,
        },
    }

    if debug:
        print(f"Ninnescah total customers out: {total_out}")

    if total_out > 0:
        return {"nearest": outage, "outages": [outage]}
    return {"nearest": None, "outages": []}
