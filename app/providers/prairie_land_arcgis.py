from __future__ import annotations

import math
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse

import requests

from app.netguard import limited_get


class PrairieLandProviderError(RuntimeError):
    """Controlled error for Prairie Land provider failures."""


PRAIRIE_LAND_MAP_URL = os.getenv("PRAIRIE_LAND_MAP_URL", "https://prairielandelectric.outagemap.coop/#/").strip()
PRAIRIE_LAND_ARCGIS_QUERY_URL = os.getenv("PRAIRIE_LAND_ARCGIS_QUERY_URL", "").strip()
PRAIRIE_LAND_SUMMARY_URL = os.getenv("PRAIRIE_LAND_SUMMARY_URL", "").strip()
PRAIRIE_LAND_REQUEST_TIMEOUT = (3.0, 12.0)
PRAIRIE_LAND_DISCOVERY_TTL_S = int(os.getenv("PRAIRIE_LAND_DISCOVERY_TTL_S", "900"))

_DISCOVERY_CACHE: Dict[str, Any] = {"ts": 0.0, "query_urls": []}


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


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def _iso8601_utc(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return (
                datetime.fromtimestamp(ts, tz=timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        except Exception:
            return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        text = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except ValueError:
            return None

    return None


def _normalize_query_url(url: str) -> str:
    base = url.split("?", 1)[0].rstrip("/")
    if base.endswith("/query"):
        return base
    return f"{base}/query"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "WeatherPower-PrairieLand-Provider/1.0",
            "Accept": "application/json,text/plain,*/*",
            "Referer": PRAIRIE_LAND_MAP_URL,
        }
    )
    return s


def _default_summary_url() -> Optional[str]:
    parsed = urlparse(PRAIRIE_LAND_MAP_URL)
    host = (parsed.hostname or "").lower()
    if not host:
        return None
    slug = host.split(".", 1)[0].strip()
    if not slug:
        return None
    return f"https://outagemap-data.cloud.coop/{slug}/Hosted_Outage_Map/summary.json"


def _webmercator_to_latlon(x: float, y: float) -> Tuple[float, float]:
    lon = (x / 20037508.34) * 180.0
    lat = (y / 20037508.34) * 180.0
    lat = 180.0 / math.pi * (2.0 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
    return lat, lon


def _fetch_summary_outages(s: requests.Session, qlat: float, qlon: float, max_radius_km: float) -> Optional[Dict[str, Any]]:
    summary_url = PRAIRIE_LAND_SUMMARY_URL or _default_summary_url()
    if not summary_url:
        return None

    resp = limited_get(s, summary_url, timeout=PRAIRIE_LAND_REQUEST_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict):
        return None

    raw_outages = payload.get("outages")
    if not isinstance(raw_outages, list):
        return None

    outages: List[Dict[str, Any]] = []
    for item in raw_outages:
        if not isinstance(item, dict):
            continue
        x = item.get("x")
        y = item.get("y")
        n = _to_int(item.get("nbrOut"))
        if x is None or y is None:
            continue
        try:
            lat, lon = _webmercator_to_latlon(float(x), float(y))
        except Exception:
            continue
        d_km = _distance_km(qlat, qlon, lat, lon)
        if d_km > max_radius_km:
            continue
        outages.append(
            {
                "id": str(item.get("id") or ""),
                "outage_id": str(item.get("id") or ""),
                "cluster": False,
                "customers_out": n,
                "n_out": n,
                "etr": None,
                "cause": None,
                "crew_status": "assigned" if item.get("crewAssigned") else None,
                "start_time": _iso8601_utc(item.get("timeOff")),
                "latitude": lat,
                "longitude": lon,
                "distance_km": d_km,
                "provider": "PRAIRIE_LAND_ELECTRIC",
                "raw": item,
            }
        )

    outages.sort(key=lambda o: float(o.get("distance_km") or 1e9))
    nearest = outages[0] if outages else None
    return {"nearest": nearest, "outages": outages}


def _extract_feature_layers(text: str) -> List[str]:
    decoded = text.replace(r"\u002F", "/").replace(r"\/", "/").replace(r"\u003A", ":")
    pat = re.compile(r"https?://[A-Za-z0-9._/-]+/(?:FeatureServer|MapServer)/\d+", re.IGNORECASE)
    urls = {m.group(0) for m in pat.finditer(decoded)}
    return sorted(urls)


def _discover_query_urls(s: requests.Session, debug: bool = False) -> List[str]:
    if PRAIRIE_LAND_ARCGIS_QUERY_URL:
        urls = [_normalize_query_url(u.strip()) for u in PRAIRIE_LAND_ARCGIS_QUERY_URL.split(",") if u.strip()]
        return sorted(dict.fromkeys(urls))

    now = time.time()
    cached_urls = _DISCOVERY_CACHE.get("query_urls")
    cached_ts = float(_DISCOVERY_CACHE.get("ts", 0.0))
    if isinstance(cached_urls, list) and cached_urls and (now - cached_ts) < PRAIRIE_LAND_DISCOVERY_TTL_S:
        return list(cached_urls)

    map_url = PRAIRIE_LAND_MAP_URL.replace("#", "") or "https://prairielandelectric.outagemap.coop/"
    resp = limited_get(s, map_url, timeout=PRAIRIE_LAND_REQUEST_TIMEOUT)
    resp.raise_for_status()
    html = resp.text

    urls: List[str] = []
    script_srcs = re.findall(r'<script[^>]+src="([^"]+)"', html)
    for src in script_srcs:
        full = urljoin(map_url, src)
        try:
            js = limited_get(s, full, timeout=PRAIRIE_LAND_REQUEST_TIMEOUT).text
        except Exception:
            continue
        urls.extend(_extract_feature_layers(js))

    out = sorted({_normalize_query_url(u) for u in urls})
    if out:
        _DISCOVERY_CACHE["ts"] = now
        _DISCOVERY_CACHE["query_urls"] = list(out)
    if debug:
        print(f"Prairie Land discovered query URLs: {out}")
    return out


def _extract_customers(attrs: Dict[str, Any]) -> Optional[int]:
    for key in ("CUSTOMERS_OUT", "CUSTOMERS", "AFFECTED", "OUTAGE_CUSTOMERS", "NUM_CUSTOMERS"):
        v = attrs.get(key)
        n = _to_int(v)
        if n is not None:
            return n
    return None


def _extract_attr(attrs: Dict[str, Any], keys: Sequence[str]) -> Optional[str]:
    for key in keys:
        v = attrs.get(key)
        if v is not None:
            txt = str(v).strip()
            if txt:
                return txt
    return None


def _normalize_feature(feature: Dict[str, Any], qlat: float, qlon: float) -> Optional[Dict[str, Any]]:
    attrs = feature.get("attributes") if isinstance(feature.get("attributes"), dict) else {}
    geom = feature.get("geometry") if isinstance(feature.get("geometry"), dict) else {}

    try:
        lat = float(geom.get("y"))
        lon = float(geom.get("x"))
    except Exception:
        return None

    if abs(lat) > 90.0 or abs(lon) > 180.0:
        return None

    outage_id = _extract_attr(attrs, ("OUTAGE_ID", "OBJECTID", "ID", "FID")) or ""
    customers = _extract_customers(attrs)
    cause = _extract_attr(attrs, ("CAUSE", "CAUSE_DESC", "REASON"))
    start_time = _iso8601_utc(attrs.get("START_TIME") or attrs.get("OUTAGE_START") or attrs.get("START"))
    etr = _iso8601_utc(attrs.get("ETR") or attrs.get("EST_RESTORE") or attrs.get("RESTORE_TIME"))
    crew = _extract_attr(attrs, ("CREW_STATUS", "STATUS", "OUTAGE_STATUS"))

    d_km = _distance_km(qlat, qlon, lat, lon)
    return {
        "id": str(outage_id),
        "outage_id": str(outage_id),
        "cluster": False,
        "customers_out": customers,
        "n_out": customers,
        "etr": etr,
        "cause": cause,
        "crew_status": crew,
        "start_time": start_time,
        "latitude": lat,
        "longitude": lon,
        "distance_km": d_km,
        "provider": "PRAIRIE_LAND_ELECTRIC",
        "raw": feature,
    }


def _query_features(s: requests.Session, query_url: str, lat: float, lon: float, max_radius_km: float) -> List[Dict[str, Any]]:
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "distance": str(max_radius_km * 1000.0),
        "units": "esriSRUnit_Meter",
        "outSR": "4326",
    }
    resp = limited_get(s, query_url, params=params, timeout=PRAIRIE_LAND_REQUEST_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()
    features = payload.get("features") if isinstance(payload, dict) else None
    return features if isinstance(features, list) else []


def fetch_prairie_land_outages(lat: float, lon: float, max_radius_km: float = 32.18688, debug: bool = False) -> Dict[str, Any]:
    """Fetch Prairie Land outages from ArcGIS query endpoints."""
    s = _session()

    query_urls = _discover_query_urls(s, debug=debug)
    if not query_urls:
        summary_result = _fetch_summary_outages(s, lat, lon, max_radius_km)
        if summary_result is not None:
            return summary_result
        raise PrairieLandProviderError("No ArcGIS query endpoints discovered. Set PRAIRIE_LAND_ARCGIS_QUERY_URL explicitly.")

    errors: List[str] = []
    outages: List[Dict[str, Any]] = []

    for query_url in query_urls:
        try:
            features = _query_features(s, query_url, lat, lon, max_radius_km)
        except Exception as e:
            errors.append(f"{query_url}: {type(e).__name__}: {e}")
            continue

        for feature in features:
            if not isinstance(feature, dict):
                continue
            normalized = _normalize_feature(feature, lat, lon)
            if not normalized:
                continue
            if float(normalized.get("distance_km") or 1e9) <= max_radius_km:
                outages.append(normalized)

    if not outages and errors and len(errors) == len(query_urls):
        raise PrairieLandProviderError("; ".join(errors[:3]))

    outages.sort(key=lambda o: float(o.get("distance_km") or 1e9))
    nearest = outages[0] if outages else None
    return {"nearest": nearest, "outages": outages}
