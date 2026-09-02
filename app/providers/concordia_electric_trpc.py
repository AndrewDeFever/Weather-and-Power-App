from __future__ import annotations

import math
import os
from typing import Any, Dict, List, Optional

import requests

from app.netguard import limited_get


class ConcordiaProviderError(RuntimeError):
    """Controlled error for Concordia provider failures."""


CONCORDIA_OUTAGES_URL = os.getenv(
    "CONCORDIA_OUTAGES_URL",
    "https://cecdata.com/trpc/outage.publicOutages",
).strip()
CONCORDIA_TIMEOUT = (3.0, 12.0)


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r_km = 6371.0088
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return r_km * c


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "WeatherPower-Concordia-Provider/1.0",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://cecdata.com/outageMap.html",
            "Origin": "https://cecdata.com",
        }
    )
    return s


def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = payload.get("result") if isinstance(payload, dict) else None
    data = result.get("data") if isinstance(result, dict) else None
    return data if isinstance(data, list) else []


def fetch_city_of_concordia_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 32.18688,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Fetch outage points from Concordia's tRPC endpoint.

    Returns:
      {"nearest": <outage|None>, "outages": [<outage>...]}
    """
    s = _session()

    try:
        resp = limited_get(s, CONCORDIA_OUTAGES_URL, timeout=CONCORDIA_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        raise ConcordiaProviderError(f"Concordia fetch failed: {type(e).__name__}: {e}") from e

    rows = _extract_rows(payload)
    outages: List[Dict[str, Any]] = []

    for item in rows:
        if not isinstance(item, dict):
            continue

        line_asset = item.get("lineAsset") if isinstance(item.get("lineAsset"), dict) else {}
        coords = line_asset.get("coordinates")
        if not isinstance(coords, list) or len(coords) < 2:
            continue

        lon_raw = _to_float(coords[0])
        lat_raw = _to_float(coords[1])
        if lon_raw is None or lat_raw is None:
            continue
        if abs(lat_raw) > 90.0 or abs(lon_raw) > 180.0:
            continue

        d_km = _distance_km(lat, lon, lat_raw, lon_raw)
        if d_km > max_radius_km:
            continue

        outage_id = item.get("id") or line_asset.get("id")

        outages.append(
            {
                "id": str(outage_id) if outage_id is not None else "",
                "outage_id": str(outage_id) if outage_id is not None else "",
                "cluster": False,
                "customers_out": None,
                "n_out": None,
                "etr": None,
                "cause": None,
                "start_time": None,
                "latitude": lat_raw,
                "longitude": lon_raw,
                "distance_km": d_km,
                "provider": "CITY_OF_CONCORDIA_ELECTRIC",
                "raw": item,
            }
        )

    outages.sort(key=lambda o: float(o.get("distance_km") or 1e9))
    nearest = outages[0] if outages else None

    if debug:
        print(f"Concordia outages in radius: {len(outages)}")

    return {"nearest": nearest, "outages": outages}
