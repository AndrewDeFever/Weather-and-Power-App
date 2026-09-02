from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from app.netguard import limited_post


class EpeProviderError(RuntimeError):
    """Controlled error for El Paso Electric provider failures."""


EPE_OUTAGE_URL = os.getenv("EPE_OUTAGE_URL", "https://starlit.epelectric.com/OmsApi/GetOutages").strip()
EPE_API_KEY = os.getenv("EPE_API_KEY", "").strip()
EPE_ENCRYPTION_KEY = os.getenv("EPE_ENCRYPTION_KEY", "").strip()
EPE_REQUEST_TIMEOUT = (3.0, 8.0)


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * (math.sin(dlon / 2) ** 2)
    )
    c = 2 * math.asin(math.sqrt(a))
    return r_km * c


def _parse_local_dt(s: Any) -> Optional[str]:
    if not isinstance(s, str) or not s.strip():
        return None
    text = s.strip()
    fmts = (
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
    )
    for fmt in fmts:
        try:
            dt = datetime.strptime(text, fmt)
            # Source string does not include timezone. Keep deterministic output in UTC format.
            return dt.replace(tzinfo=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except ValueError:
            continue
    return None


def _decrypt_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise EpeProviderError("Unexpected outage payload type")

    data = payload.get("data")
    iv = payload.get("iv")
    if not (isinstance(data, str) and isinstance(iv, str)):
        # Some environments may eventually return plain JSON.
        return payload

    if not EPE_ENCRYPTION_KEY:
        raise EpeProviderError("Missing EPE_ENCRYPTION_KEY")
    key_bytes = EPE_ENCRYPTION_KEY.encode("utf-8")[:32].ljust(32, b"0")
    aesgcm = AESGCM(key_bytes)
    try:
        plaintext = aesgcm.decrypt(bytes.fromhex(iv), bytes.fromhex(data), None)
    except Exception as e:
        raise EpeProviderError(f"Failed to decrypt EPE payload: {type(e).__name__}: {e}") from e

    try:
        return json.loads(plaintext.decode("utf-8"))
    except Exception as e:
        raise EpeProviderError(f"Failed to parse decrypted EPE JSON: {type(e).__name__}: {e}") from e


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "WeatherPower-EPE-Provider/1.0",
            "Accept": "application/json,text/plain,*/*",
            "Content-Type": "application/json",
            "x-api-key": EPE_API_KEY,
        }
    )
    return s


def _request_epe_outages(debug: bool = False) -> Dict[str, Any]:
    if not EPE_API_KEY:
        raise EpeProviderError("Missing EPE_API_KEY")

    s = _session()
    if debug:
        print(f"POST {EPE_OUTAGE_URL}")

    try:
        resp = limited_post(
            s,
            EPE_OUTAGE_URL,
            json={},
            timeout=EPE_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        raise EpeProviderError(f"EPE outage request failed: {type(e).__name__}: {e}") from e

    return _decrypt_payload(raw)


def _normalize_outage(entry: Dict[str, Any], qlat: float, qlon: float) -> Optional[Dict[str, Any]]:
    try:
        lat = float(entry.get("latitude"))
        lon = float(entry.get("longitude"))
    except Exception:
        return None

    customers = entry.get("customersAffected")
    try:
        customers_out = int(customers) if customers is not None else None
    except Exception:
        customers_out = None

    d_km = _distance_km(qlat, qlon, lat, lon)
    return {
        "id": entry.get("outageNo"),
        "outage_id": str(entry.get("outageNo")) if entry.get("outageNo") is not None else None,
        "cluster": False,
        "customers_out": customers_out,
        "n_out": customers_out,
        "etr": _parse_local_dt(entry.get("etr")),
        "start_time": _parse_local_dt(entry.get("timeStamp")),
        "cause": entry.get("status"),
        "crew_status": entry.get("status"),
        "zipcode": entry.get("zipcode"),
        "latitude": lat,
        "longitude": lon,
        "distance_km": d_km,
        "provider": "EPE",
        "raw": entry,
    }


def fetch_epe_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 50.0,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Fetch nearby El Paso Electric outages from EPE's native backend.

    Returns:
        {"nearest": outage|None, "outages": [outage, ...]}
    """
    payload = _request_epe_outages(debug=debug)
    outages_raw = payload.get("outages") if isinstance(payload, dict) else None
    if not isinstance(outages_raw, list):
        return {"nearest": None, "outages": []}

    normalized: List[Dict[str, Any]] = []
    for item in outages_raw:
        if not isinstance(item, dict):
            continue
        o = _normalize_outage(item, lat, lon)
        if not o:
            continue
        if o.get("distance_km") is not None and float(o["distance_km"]) <= max_radius_km:
            normalized.append(o)

    normalized.sort(key=lambda x: float(x.get("distance_km") or 1e9))
    nearest = normalized[0] if normalized else None
    return {"nearest": nearest, "outages": normalized}
