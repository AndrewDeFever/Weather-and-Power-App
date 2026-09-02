# app/providers/aep_kubra.py
"""
AEP Power Texas (American Electric Power - Texas) — Kubra StormCenter provider module
=====================================================================================

Provider-only module. No FastAPI/UI/weather.

Primary entrypoint:
    fetch_aep_outages(
        lat, lon,
        max_radius_km=50.0,
        max_zoom=12,
        neighbor_depth=1,
        drill_neighbor_depth=1,
        debug=False,
        fast=False,
        stop_after=10,
        max_tile_fetches=None,
    )

Return:
    { "nearest": <outage|null>, "outages": [<outage>...] }

Outage fields (must match OG&E/PSO/Evergy/Oncor/Austin):
    id, cluster, customers_out, n_out, etr, etr_confidence, cause, comments,
    crew_status, start_time, latitude, longitude, distance_km

AEP-specific config:
- Outage map: https://outagemap.aeptexas.com/
- StormCenter ID: 3ff6812b-90d8-40cd-97a6-76633226f27b
- View ID: 6022fe09-5259-4763-892f-5f57463fa6a5
- Service areas endpoint: https://kubra.io/regions/c1bfd09b-c905-43e1-91e8-5ea69d186b64/71500943-67a0-4a68-8c8f-8ca5738727cd/serviceareas.json

Key behaviors:
- Auto-discover layer/qkh/layout/entry zoom using currentState + bounded probes.
- Decode geom.p (encoded polyline) to coordinates.
- Drill clusters down to max_zoom.
- Never return cluster rows in final outages list.
- Normalize localized dicts (prefer EN-US, ignore 'orig').
- Normalize timestamps to strict ISO-8601 UTC (seconds + Z).
- Performance hardening: cache state+scheme, cap drill, distance-gate cluster drilling.
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
import urllib3

from app.netguard import limited_get


class AepKubraError(RuntimeError):
    """Controlled error for AEP Kubra provider failures."""


KUBRA_BASE = "https://kubra.io"
AEP_MAP_URL = "https://outagemap.aeptexas.com/"

STORMCENTER_ID = "3ff6812b-90d8-40cd-97a6-76633226f27b"
VIEW_ID = "6022fe09-5259-4763-892f-5f57463fa6a5"

CURRENT_STATE_URL = (
    f"{KUBRA_BASE}/stormcenter/api/v1/stormcenters/{STORMCENTER_ID}"
    f"/views/{VIEW_ID}/currentState?preview=false"
)

SERVICE_AREAS_URL = (
    "https://kubra.io/regions/c1bfd09b-c905-43e1-91e8-5ea69d186b64/"
    "71500943-67a0-4a68-8c8f-8ca5738727cd/serviceareas.json"
)

DISCOVERY_ZOOMS: Sequence[int] = (8, 9, 10, 11, 12, 13, 14)
QKH_STRATEGIES: Sequence[str] = ("last3_rev", "last3", "first3", "first3_rev", "last4_rev")
LAYOUTS: Sequence[str] = ("flat", "split2")
QUICK_SCHEME_CANDIDATES: Sequence[Tuple[int, str]] = (
    (10, "last3_rev"),
    (11, "last3_rev"),
    (12, "last3_rev"),
    (10, "last3"),
    (11, "last3"),
)

REQUEST_TIMEOUT = (3.0, 5.0)
DISCOVERY_BUDGET_SECONDS = 3.0
DISCOVERY_MAX_PROBES = 50
DISCOVERY_NEIGHBOR_DEPTH = 1
DRILL_MAX_PER_CLUSTER = 5
DISTANCE_GATE_KM = 100.0

_AEP_SSL_VERIFY_ENV = os.getenv("AEP_SSL_VERIFY", "true").strip().lower()
AEP_SSL_VERIFY = _AEP_SSL_VERIFY_ENV in ("1", "true", "yes", "on")
AEP_SSL_CA_BUNDLE = os.getenv("AEP_SSL_CA_BUNDLE", "").strip() or None
if not AEP_SSL_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "WeatherPower-AepKubraProvider/1.0",
        "Accept": "application/json,text/plain,*/*",
    })
    return s


def _get_json(s: requests.Session, url: str, debug: bool = False) -> Any:
    if debug:
        print(f"GET {url}")
    verify_arg: Any = AEP_SSL_CA_BUNDLE if AEP_SSL_CA_BUNDLE else AEP_SSL_VERIFY
    r = limited_get(s, url, timeout=REQUEST_TIMEOUT, verify=verify_arg)
    r.raise_for_status()
    return r.json()


def _quadkey_from_tile(x: int, y: int, z: int) -> str:
    qk = ""
    for i in range(z - 1, -1, -1):
        mask = 1 << i
        digit = 0
        if x & mask:
            digit += 1
        if y & mask:
            digit += 2
        qk += str(digit)
    return qk


def _qkh_last3_rev(qk: str) -> str:
    return qk[-3:][::-1] if len(qk) >= 3 else qk[::-1]


def _qkh_last3(qk: str) -> str:
    return qk[-3:] if len(qk) >= 3 else qk


def _qkh_first3(qk: str) -> str:
    return qk[:3] if len(qk) >= 3 else qk


def _qkh_first3_rev(qk: str) -> str:
    return qk[:3][::-1] if len(qk) >= 3 else qk[::-1]


def _qkh_last4_rev(qk: str) -> str:
    return qk[-4:][::-1] if len(qk) >= 4 else qk[::-1]


QKH_FUNCS = {
    "last3_rev": _qkh_last3_rev,
    "last3": _qkh_last3,
    "first3": _qkh_first3,
    "first3_rev": _qkh_first3_rev,
    "last4_rev": _qkh_last4_rev,
}


def _tile_from_latlng(lat: float, lon: float, z: int) -> Tuple[int, int]:
    n = 1 << z
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.log(math.tan(math.radians(lat)) + 1.0 / math.cos(math.radians(lat))) / math.pi) / 2.0 * n)
    x = max(0, min(x, n - 1))
    y = max(0, min(y, n - 1))
    return x, y


def _latlng_from_tile(x: int, y: int, z: int) -> Tuple[float, float]:
    n = 1.0 / (1 << z)
    lon = x * n * 360.0 - 180.0 + (n * 180.0)
    lat_rad = math.atan(math.sinh(math.pi * (1.0 - 2.0 * y * n)))
    lat = math.degrees(lat_rad)
    return lat, lon


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R_KM = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * (math.sin(dlon / 2) ** 2))
    c = 2 * math.asin(math.sqrt(a))
    return R_KM * c


def _decode_polyline(encoded: str) -> List[Tuple[float, float]]:
    inv = 1.0 / 1e5
    decoded = []
    prev_lat = 0
    prev_lon = 0
    i = 0
    while i < len(encoded):
        ll_idx = 0
        for shift in range(0, 32, 5):
            b = ord(encoded[i]) - 63
            i += 1
            ll_idx |= (b & 31) << shift
            if not (b & 32):
                break
        dlat = ~ll_idx >> 1 if ll_idx & 1 else ll_idx >> 1
        prev_lat += dlat
        ll_idx = 0
        for shift in range(0, 32, 5):
            b = ord(encoded[i]) - 63
            i += 1
            ll_idx |= (b & 31) << shift
            if not (b & 32):
                break
        dlon = ~ll_idx >> 1 if ll_idx & 1 else ll_idx >> 1
        prev_lon += dlon
        decoded.append((prev_lat * inv, prev_lon * inv))
    return decoded


def _localize(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    if key not in d:
        return default
    v = d[key]
    if not isinstance(v, dict):
        return v
    if "en-US" in v:
        return v["en-US"]
    if "en" in v:
        return v["en"]
    for k, val in v.items():
        if k != "orig":
            return val
    return default


def _normalize_timestamp(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return None
    ts = ts.strip()
    if ts.endswith("Z") and "T" in ts:
        try:
            dt = datetime.fromisoformat(ts.rstrip("Z"))
            return dt.replace(microsecond=0, tzinfo=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "") + "Z"
        except (ValueError, AttributeError):
            return ts
    return ts


def fetch_aep_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 50.0,
    max_zoom: int = 12,
    neighbor_depth: int = 1,
    drill_neighbor_depth: int = 1,
    debug: bool = False,
    fast: bool = False,
    stop_after: int = 10,
    max_tile_fetches: Optional[int] = None,
) -> Dict[str, Any]:
    t0 = time.time()
    s = _session()
    outages: List[Dict[str, Any]] = []
    visited_urls: set = set()
    try:
        if debug:
            print(f"[AEP] Fetching currentState from {CURRENT_STATE_URL}")

        current_state = _get_json(s, CURRENT_STATE_URL, debug=debug)
        cluster_template = current_state.get("data", {}).get("cluster_interval_generation_data", "")
        if debug:
            print(f"[AEP] Cluster template: {cluster_template}")
        if not isinstance(cluster_template, str) or not cluster_template.strip():
            raise AepKubraError("currentState missing cluster_interval_generation_data")

        scheme = None
        for entry_zoom, qkh_strategy in QUICK_SCHEME_CANDIDATES:
            qx, qy = _tile_from_latlng(lat, lon, entry_zoom)
            url = _build_cluster_url(cluster_template, entry_zoom, qx, qy, qkh_strategy)
            if _probe_url(s, url, debug=debug):
                scheme = (cluster_template, entry_zoom, qkh_strategy)
                if debug:
                    print(f"[AEP] Quick scheme matched: {scheme}")
                break

        if not scheme:
            if debug:
                print("[AEP] Quick scheme failed, running full discovery...")
            scheme = _discover_scheme(s, cluster_template, lat, lon, debug=debug)

        if not scheme:
            scheme = (cluster_template, 9, "last3_rev")
            if debug:
                print(f"[AEP] Falling back to default scheme: template={cluster_template}, zoom=9, qkh=last3_rev")

        cluster_template, entry_zoom, qkh_strategy = scheme
        if debug:
            print(f"[AEP] Using scheme: template={cluster_template}, zoom={entry_zoom}, qkh={qkh_strategy}")

        x, y = _tile_from_latlng(lat, lon, entry_zoom)
        if debug:
            print(f"[AEP] Entry tile at z={entry_zoom}: ({x}, {y})")

        tiles_to_fetch = {(x, y, entry_zoom)}
        for dx in range(-neighbor_depth, neighbor_depth + 1):
            for dy in range(-neighbor_depth, neighbor_depth + 1):
                if dx == 0 and dy == 0:
                    continue
                tiles_to_fetch.add((x + dx, y + dy, entry_zoom))

        for tx, ty, tz in tiles_to_fetch:
            if max_tile_fetches and len(visited_urls) >= max_tile_fetches:
                break
            _fetch_tile(s, tx, ty, tz, cluster_template, qkh_strategy, outages, visited_urls, lat, lon, max_radius_km, debug=debug)

        if max_zoom > entry_zoom and drill_neighbor_depth >= 0:
            clusters_to_drill = []
            for outage in outages:
                if outage.get("cluster"):
                    dist_km = outage.get("distance_km", float("inf"))
                    if dist_km <= DISTANCE_GATE_KM:
                        clusters_to_drill.append(outage)

            drilled_count = 0
            for cluster in clusters_to_drill:
                if drilled_count >= DRILL_MAX_PER_CLUSTER:
                    break
                if max_tile_fetches and len(visited_urls) >= max_tile_fetches:
                    break

                cluster_lat = cluster.get("latitude")
                cluster_lon = cluster.get("longitude")
                if not (cluster_lat and cluster_lon):
                    continue

                for drill_z in range(entry_zoom + 1, min(max_zoom + 1, entry_zoom + 5)):
                    dx, dy = _tile_from_latlng(cluster_lat, cluster_lon, drill_z)

                    for ddx in range(-drill_neighbor_depth, drill_neighbor_depth + 1):
                        for ddy in range(-drill_neighbor_depth, drill_neighbor_depth + 1):
                            if max_tile_fetches and len(visited_urls) >= max_tile_fetches:
                                break
                            _fetch_tile(
                                s, dx + ddx, dy + ddy, drill_z, cluster_template, qkh_strategy,
                                outages, visited_urls, lat, lon, max_radius_km, debug=debug
                            )

                    drilled_count += 1
                    if fast and len(outages) >= stop_after:
                        break

                if fast and len(outages) >= stop_after:
                    break

        final_outages = [o for o in outages if not o.get("cluster")]
        seen = set()
        deduped = []
        for o in final_outages:
            oid = o.get("id")
            if oid and oid in seen:
                continue
            seen.add(oid)
            deduped.append(o)

        deduped.sort(key=lambda o: o.get("distance_km", float("inf")))
        nearest = deduped[0] if deduped else None

        if debug:
            elapsed = time.time() - t0
            print(f"[AEP] Fetch complete: {len(deduped)} outages in {elapsed:.2f}s ({len(visited_urls)} tiles)")

        return {"nearest": nearest, "outages": deduped}

    except Exception as e:
        if debug:
            print(f"[AEP] Error: {type(e).__name__}: {e}")
        raise AepKubraError(f"fetch_aep_outages failed: {type(e).__name__}: {e}") from e


def _build_cluster_url(cluster_template: str, zoom: int, x: int, y: int, qkh_strategy: str) -> str:
    qk = _quadkey_from_tile(x, y, zoom)
    qkh_func = QKH_FUNCS.get(qkh_strategy, _qkh_last3_rev)
    qkh = qkh_func(qk)
    return f"{KUBRA_BASE}/{cluster_template.replace('{qkh}', qkh)}/public/cluster-1/{qk}.json"


def _probe_url(s: requests.Session, url: str, debug: bool = False) -> bool:
    try:
        verify_arg: Any = AEP_SSL_CA_BUNDLE if AEP_SSL_CA_BUNDLE else AEP_SSL_VERIFY
        r = limited_get(s, url, timeout=REQUEST_TIMEOUT, verify=verify_arg)
        return r.status_code == 200
    except Exception:
        return False


def _discover_scheme(
    s: requests.Session,
    cluster_template: str,
    lat: float,
    lon: float,
    debug: bool = False,
) -> Optional[Tuple[str, int, str]]:
    t0 = time.time()
    probes_tried = 0

    for entry_zoom in DISCOVERY_ZOOMS:
        if time.time() - t0 > DISCOVERY_BUDGET_SECONDS:
            if debug:
                print(f"[AEP] Discovery timeout after {probes_tried} probes")
            break

        x, y = _tile_from_latlng(lat, lon, entry_zoom)

        for qkh_strat in QKH_STRATEGIES:
            for dx in range(-DISCOVERY_NEIGHBOR_DEPTH, DISCOVERY_NEIGHBOR_DEPTH + 1):
                for dy in range(-DISCOVERY_NEIGHBOR_DEPTH, DISCOVERY_NEIGHBOR_DEPTH + 1):
                    if probes_tried >= DISCOVERY_MAX_PROBES:
                        return None

                    tx = x + dx
                    ty = y + dy
                    n = 1 << entry_zoom
                    if tx < 0 or ty < 0 or tx >= n or ty >= n:
                        continue

                    url = _build_cluster_url(cluster_template, entry_zoom, tx, ty, qkh_strat)
                    probes_tried += 1

                    if _probe_url(s, url, debug=debug):
                        if debug:
                            print(f"[AEP] Discovery found: z={entry_zoom}, qkh={qkh_strat}, dx={dx}, dy={dy}")
                        return (cluster_template, entry_zoom, qkh_strat)

    return None


def _fetch_tile(
    s: requests.Session,
    x: int,
    y: int,
    z: int,
    cluster_template: str,
    qkh_strategy: str,
    outages: List[Dict[str, Any]],
    visited_urls: set,
    query_lat: float,
    query_lon: float,
    max_radius_km: float,
    debug: bool = False,
) -> None:
    url = _build_cluster_url(cluster_template, z, x, y, qkh_strategy)

    if url in visited_urls:
        return

    visited_urls.add(url)

    try:
        data = _get_json(s, url, debug=debug)
    except Exception as e:
        if debug:
            print(f"[AEP] Tile fetch failed: {url}: {e}")
        return

    file_data = data.get("file_data", [])
    if not isinstance(file_data, list):
        return

    for item in file_data:
        desc = item.get("desc", {})
        geom = item.get("geom", {})

        if not isinstance(desc, dict) or not isinstance(geom, dict):
            continue

        geom_p = geom.get("p")
        if isinstance(geom_p, list):
            geom_p = geom_p[0] if geom_p else None
        if not isinstance(geom_p, str) or not geom_p:
            continue

        try:
            coords = _decode_polyline(geom_p)
        except Exception:
            continue

        if not coords:
            continue

        avg_lat = sum(c[0] for c in coords) / len(coords)
        avg_lon = sum(c[1] for c in coords) / len(coords)

        dist_km = _distance_km(query_lat, query_lon, avg_lat, avg_lon)
        if dist_km > max_radius_km:
            continue

        cust_a = desc.get("cust_a") if isinstance(desc.get("cust_a"), dict) else None
        customers_out = desc.get("customers_out")
        if customers_out is None and cust_a is not None:
            customers_out = cust_a.get("val")

        outage = {
            "id": desc.get("id") or desc.get("inc_id") or item.get("id"),
            "cluster": bool(desc.get("cluster")),
            "customers_out": customers_out,
            "n_out": desc.get("n_out"),
            "etr": _localize(desc, "etr"),
            "etr_confidence": _localize(desc, "etr_confidence"),
            "cause": _localize(desc, "cause"),
            "comments": _localize(desc, "comments"),
            "crew_status": _localize(desc, "crew_status"),
            "start_time": _normalize_timestamp(_localize(desc, "start_time")),
            "latitude": avg_lat,
            "longitude": avg_lon,
            "distance_km": dist_km,
        }

        outage = {k: v for k, v in outage.items() if v is not None}
        outages.append(outage)
