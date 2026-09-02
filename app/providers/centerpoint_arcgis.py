from __future__ import annotations

import math
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import requests

from app.netguard import limited_get


class CenterPointProviderError(RuntimeError):
    """Controlled error for CenterPoint provider failures."""


CENTERPOINT_MAP_URL = os.getenv("CENTERPOINT_MAP_URL", "https://tracker.centerpointenergy.com/map").strip()
CENTERPOINT_ARCGIS_QUERY_URL = os.getenv("CENTERPOINT_ARCGIS_QUERY_URL", "").strip()
CENTERPOINT_REGION = os.getenv("CENTERPOINT_REGION", "texas").strip() or "texas"
CENTERPOINT_EVENTS_URL = os.getenv("CENTERPOINT_EVENTS_URL", "").strip()
CENTERPOINT_REQUEST_TIMEOUT = (3.0, 12.0)


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
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
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


def _web_mercator_to_wgs84(x: float, y: float) -> Tuple[float, float]:
    lon = (x / 20037508.34) * 180.0
    lat = (y / 20037508.34) * 180.0
    lat = 180.0 / math.pi * (2.0 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
    return lat, lon


def _feature_latlon(feature: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    geom = feature.get("geometry") if isinstance(feature, dict) else None
    attrs = feature.get("attributes") if isinstance(feature, dict) else None
    attrs = attrs if isinstance(attrs, dict) else {}

    candidates: List[Tuple[Any, Any]] = []
    if isinstance(geom, dict):
        candidates.append((geom.get("y"), geom.get("x")))
    candidates.extend(
        [
            (attrs.get("LAT"), attrs.get("LON")),
            (attrs.get("LATITUDE"), attrs.get("LONGITUDE")),
            (attrs.get("Y"), attrs.get("X")),
        ]
    )

    for lat_raw, lon_raw in candidates:
        try:
            lat = float(lat_raw)
            lon = float(lon_raw)
        except Exception:
            continue

        if abs(lat) <= 90.0 and abs(lon) <= 180.0:
            return lat, lon

        if abs(lat) > 90.0 or abs(lon) > 180.0:
            wgs_lat, wgs_lon = _web_mercator_to_wgs84(lon, lat)
            if abs(wgs_lat) <= 90.0 and abs(wgs_lon) <= 180.0:
                return wgs_lat, wgs_lon

    return None


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "WeatherPower-CenterPoint-Provider/1.0",
            "Accept": "application/json,text/plain,*/*",
            "Referer": CENTERPOINT_MAP_URL,
            "Origin": "https://tracker.centerpointenergy.com",
        }
    )
    return s


def _candidate_events_urls() -> List[str]:
    if CENTERPOINT_EVENTS_URL:
        urls = [u.strip() for u in CENTERPOINT_EVENTS_URL.split(",") if u.strip()]
        return urls

    region = CENTERPOINT_REGION
    return [
        f"https://centerpoint.datacapable.com/datacapable/v2/cache/p/centerpoint/r/{region}/map/events",
        f"https://centerpoint.datacapable.com/datacapable/v2/p/centerpoint/r/{region}/map/events",
    ]


def _parse_additional_properties(item: Dict[str, Any]) -> Dict[str, Any]:
    raw = item.get("additionalProperties") if isinstance(item, dict) else None
    if not isinstance(raw, list):
        return {}

    out: Dict[str, Any] = {}
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("property") or "").strip().upper()
        if not key:
            continue
        value = entry.get("value")
        if isinstance(value, list) and value:
            out[key] = value[0]
        else:
            out[key] = value
    return out


def _normalize_datacapable_event(item: Dict[str, Any], qlat: float, qlon: float) -> Optional[Dict[str, Any]]:
    try:
        lat = float(item.get("latitude"))
        lon = float(item.get("longitude"))
    except Exception:
        return None

    props = _parse_additional_properties(item)
    customers = _to_int(item.get("numPeople"))
    etr_raw = item.get("estimatedRestoreTime") or item.get("etr") or props.get("ETR")
    cause_raw = item.get("cause") or item.get("status") or props.get("CAUSE")
    status_raw = item.get("status") or props.get("STATUS")
    start_raw = item.get("startTime")

    d_km = _distance_km(qlat, qlon, lat, lon)
    return {
        "id": str(item.get("identifier") or item.get("id") or ""),
        "outage_id": str(item.get("identifier") or item.get("id") or ""),
        "cluster": False,
        "customers_out": customers,
        "n_out": customers,
        "etr": _iso8601_utc(etr_raw),
        "start_time": _iso8601_utc(start_raw),
        "cause": str(cause_raw) if cause_raw is not None else None,
        "crew_status": str(status_raw) if status_raw is not None else None,
        "zipcode": props.get("AREA_ZIP"),
        "latitude": lat,
        "longitude": lon,
        "distance_km": d_km,
        "provider": "CENTERPOINT",
        "raw": item,
    }


def _fetch_datacapable_outages(
    s: requests.Session,
    lat: float,
    lon: float,
    max_radius_km: float,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    urls = _candidate_events_urls()
    errors: List[str] = []

    for events_url in urls:
        try:
            resp = limited_get(s, events_url, timeout=CENTERPOINT_REQUEST_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            errors.append(f"{events_url}: {type(e).__name__}: {e}")
            continue

        if not isinstance(payload, list):
            errors.append(f"{events_url}: unexpected payload type {type(payload).__name__}")
            continue

        rows: List[Dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            normalized = _normalize_datacapable_event(item, lat, lon)
            if not normalized:
                continue
            if float(normalized.get("distance_km") or 1e9) <= max_radius_km:
                rows.append(normalized)

        if debug:
            print(f"CenterPoint datacapable URL {events_url} yielded {len(rows)} rows in radius")
        return rows

    if errors:
        raise CenterPointProviderError("; ".join(errors[:3]))
    return []


def _normalize_query_url(url: str) -> str:
    base = url.split("?", 1)[0].rstrip("/")
    if base.endswith("/query"):
        return base
    return f"{base}/query"


def _extract_feature_layers(blobs: Iterable[str]) -> List[str]:
    pat = re.compile(r"https://[A-Za-z0-9._/-]+/(?:FeatureServer|MapServer)/\d+", re.IGNORECASE)
    found: List[str] = []
    for blob in blobs:
        text = (blob or "").replace("\\/", "/")
        found.extend(pat.findall(text))

    uniq: List[str] = []
    seen = set()
    for u in found:
        n = _normalize_query_url(u)
        if n not in seen:
            uniq.append(n)
            seen.add(n)

    def score(url: str) -> int:
        lower = url.lower()
        score_val = 0
        for k in ("outage", "incident", "electric", "centerpoint", "damage"):
            if k in lower:
                score_val += 2
        if "featureserver" in lower:
            score_val += 1
        return score_val

    return sorted(uniq, key=score, reverse=True)


def _discover_query_urls(s: requests.Session, debug: bool = False) -> List[str]:
    if CENTERPOINT_ARCGIS_QUERY_URL:
        env_urls = [x.strip() for x in CENTERPOINT_ARCGIS_QUERY_URL.split(",") if x.strip()]
        return [_normalize_query_url(u) for u in env_urls]

    try:
        page = limited_get(s, CENTERPOINT_MAP_URL, timeout=CENTERPOINT_REQUEST_TIMEOUT)
        page.raise_for_status()
    except Exception as e:
        raise CenterPointProviderError(f"CenterPoint map request failed: {type(e).__name__}: {e}") from e

    html = page.text
    blobs = [html]

    script_srcs = re.findall(r"<script[^>]+src=[\"']([^\"']+)[\"']", html, flags=re.IGNORECASE)
    for src in script_srcs[:12]:
        js_url = src if src.lower().startswith("http") else urljoin(CENTERPOINT_MAP_URL, src)
        try:
            js = limited_get(s, js_url, timeout=CENTERPOINT_REQUEST_TIMEOUT)
            if js.status_code == 200 and js.text:
                blobs.append(js.text)
        except Exception:
            continue

    urls = _extract_feature_layers(blobs)
    if debug:
        print(f"CenterPoint ArcGIS query candidates: {urls}")
    if not urls:
        raise CenterPointProviderError(
            "Could not discover CenterPoint ArcGIS feature layer URLs. Set CENTERPOINT_ARCGIS_QUERY_URL."
        )
    return urls


def _query_features(
    s: requests.Session,
    query_url: str,
    lat: float,
    lon: float,
    max_radius_km: float,
) -> List[Dict[str, Any]]:
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "4326",
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "spatialRel": "esriSpatialRelIntersects",
        "distance": str(max_radius_km),
        "units": "esriSRUnit_Kilometer",
        "resultRecordCount": "2000",
    }

    resp = limited_get(s, query_url, params=params, timeout=CENTERPOINT_REQUEST_TIMEOUT)
    if resp.status_code != 200:
        raise CenterPointProviderError(f"CenterPoint ArcGIS query failed: {resp.status_code} {query_url}")

    payload = resp.json()
    if not isinstance(payload, dict):
        raise CenterPointProviderError("CenterPoint ArcGIS response was not a JSON object")

    if payload.get("error"):
        err = payload.get("error")
        msg = err.get("message") if isinstance(err, dict) else str(err)
        raise CenterPointProviderError(f"CenterPoint ArcGIS error: {msg}")

    features = payload.get("features")
    return features if isinstance(features, list) else []


def _best_attr(attrs: Dict[str, Any], keys: Sequence[str]) -> Any:
    if not isinstance(attrs, dict):
        return None
    for k in keys:
        if k in attrs and attrs[k] not in (None, ""):
            return attrs[k]
    return None


def _normalize_feature(feature: Dict[str, Any], qlat: float, qlon: float) -> Optional[Dict[str, Any]]:
    attrs = feature.get("attributes") if isinstance(feature, dict) else None
    attrs = attrs if isinstance(attrs, dict) else {}
    ll = _feature_latlon(feature)
    if not ll:
        return None
    lat, lon = ll

    outage_id = _best_attr(attrs, ["OUTAGE_ID", "OUTAGEID", "INCIDENTID", "OBJECTID", "GLOBALID", "FID"])
    customers = _best_attr(
        attrs,
        [
            "CUSTOMERS_OUT",
            "CUSTOMERSAFFECTED",
            "CUSTOMERS_AFFECTED",
            "AFFECTEDCUSTOMERS",
            "N_OUT",
            "CUST_OUT",
            "OUTAGE_COUNT",
        ],
    )
    etr = _best_attr(attrs, ["ETR", "EST_RESTORE", "RESTORE_TIME", "ESTIMATEDRESTORATION"])
    start_time = _best_attr(attrs, ["START_TIME", "OUTAGE_START", "CREATIONDATE", "REPORTED_TIME", "CREATED_DATE"])
    cause = _best_attr(attrs, ["CAUSE", "OUTAGE_CAUSE", "STATUS", "TYPE"])

    d_km = _distance_km(qlat, qlon, lat, lon)
    return {
        "id": str(outage_id) if outage_id is not None else None,
        "outage_id": str(outage_id) if outage_id is not None else None,
        "cluster": False,
        "customers_out": _to_int(customers),
        "n_out": _to_int(customers),
        "etr": _iso8601_utc(etr),
        "start_time": _iso8601_utc(start_time),
        "cause": str(cause) if cause is not None else None,
        "latitude": lat,
        "longitude": lon,
        "distance_km": d_km,
        "provider": "CENTERPOINT",
        "raw": feature,
    }


def fetch_centerpoint_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 50.0,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Fetch nearby CenterPoint outages from ArcGIS endpoints used by the public map.

    Returns:
        {"nearest": outage|None, "outages": [outage, ...]}
    """
    s = _session()
    all_outages: List[Dict[str, Any]] = []
    datacapable_error: Optional[Exception] = None
    datacapable_succeeded = False

    try:
        all_outages = _fetch_datacapable_outages(
            s,
            lat=lat,
            lon=lon,
            max_radius_km=max_radius_km,
            debug=debug,
        )
        datacapable_succeeded = True
    except Exception as e:
        datacapable_error = e

    if (not datacapable_succeeded) and (not all_outages):
        query_urls = _discover_query_urls(s, debug=debug)
        errors: List[str] = []
        for query_url in query_urls[:8]:
            try:
                features = _query_features(s, query_url, lat, lon, max_radius_km)
            except Exception as e:
                errors.append(f"{query_url}: {type(e).__name__}: {e}")
                continue

            for f in features:
                if not isinstance(f, dict):
                    continue
                normalized = _normalize_feature(f, lat, lon)
                if not normalized:
                    continue
                if float(normalized.get("distance_km") or 1e9) <= max_radius_km:
                    all_outages.append(normalized)

            if all_outages:
                break

        if not all_outages and errors:
            msg_parts = []
            if datacapable_error is not None:
                msg_parts.append(f"datacapable: {type(datacapable_error).__name__}: {datacapable_error}")
            msg_parts.append("; ".join(errors[:3]))
            raise CenterPointProviderError(" | ".join(msg_parts))

    uniq: Dict[Tuple[str, float, float], Dict[str, Any]] = {}
    for o in all_outages:
        oid = str(o.get("outage_id") or "")
        latv = round(float(o.get("latitude") or 0.0), 6)
        lonv = round(float(o.get("longitude") or 0.0), 6)
        uniq[(oid, latv, lonv)] = o

    outages = list(uniq.values())
    outages.sort(key=lambda x: float(x.get("distance_km") or 1e9))
    nearest = outages[0] if outages else None
    return {"nearest": nearest, "outages": outages}
