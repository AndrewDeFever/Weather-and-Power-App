from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

from app.netguard import limited_get

_log = logging.getLogger("wnp.austin_kubra")

# ============================================================
# Austin Energy (Kubra) provider with sanity gate + full tiles
# ============================================================

# --- Locked anchors from working public tile URL ---
AUSTIN_DATASET_UUID = "20d293c6-08fd-41b3-b96b-d2f522c74990"
AUSTIN_LOCKED_STATE_UUID = "9374d7e4-e078-4f44-b6f4-74cdc63f4acf"
AUSTIN_ENTRY_CLUSTER_LEVEL = 1

# Cache for last-known-good state UUID (rotation fallback)
STATE_UUID_CACHE_FILE = os.getenv("AUSTIN_STATE_UUID_CACHE_FILE", "/tmp/austin_energy_state_uuid.json")

# Fast-path overrides for environments where Austin state UUID / zoom are stable.
AUSTIN_STATE_UUID = os.getenv("AUSTIN_STATE_UUID", "").strip() or None
AUSTIN_ENTRY_ZOOM = os.getenv("AUSTIN_ENTRY_ZOOM", "").strip() or None

# Kubra config endpoint you provided (used for sanity gate)
CONFIG_URL = (
    "https://kubra.io/stormcenter/api/v1/stormcenters/"
    "dd9c446f-f6b8-43f9-8f80-83f5245c60a1/"
    "views/76446308-a901-4fa3-849c-3dd569933a51/"
    "configuration/53b6bbf9-126a-43cd-8eb5-eca49ade8eb4"
    "?preview=false"
)

CURRENT_STATE_URL = (
    "https://kubra.io/stormcenter/api/v1/stormcenters/"
    "dd9c446f-f6b8-43f9-8f80-83f5245c60a1/"
    "views/76446308-a901-4fa3-849c-3dd569933a51/"
    "currentState?preview=false"
)

DEFAULT_HEADERS = {
    "User-Agent": "WeatherPower-AustinEnergyKubraProvider/2.0",
    "Accept": "application/json,text/plain,*/*",
}

# TLS controls for environments with SSL interception.
# Default is secure verification. Set AUSTIN_SSL_VERIFY=false only when required.
_AUSTIN_SSL_VERIFY_ENV = os.getenv("AUSTIN_SSL_VERIFY", "true").strip().lower()
AUSTIN_SSL_VERIFY = _AUSTIN_SSL_VERIFY_ENV in ("1", "true", "yes", "on")
AUSTIN_SSL_CA_BUNDLE = os.getenv("AUSTIN_SSL_CA_BUNDLE", "").strip() or None
if not AUSTIN_SSL_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

AUSTIN_SANITY_TIMEOUT_S = float(os.getenv("AUSTIN_SANITY_TIMEOUT_S", "3.0"))
AUSTIN_DISCOVERY_TIMEOUT_S = float(os.getenv("AUSTIN_DISCOVERY_TIMEOUT_S", "3.0"))
AUSTIN_TILE_TIMEOUT_S = float(os.getenv("AUSTIN_TILE_TIMEOUT_S", "4.0"))
AUSTIN_WARM_LAT = float(os.getenv("AUSTIN_WARM_LAT", "30.2672"))
AUSTIN_WARM_LON = float(os.getenv("AUSTIN_WARM_LON", "-97.7431"))
AUSTIN_WARM_MAX_ZOOM = int(os.getenv("AUSTIN_WARM_MAX_ZOOM", "12"))

OUTAGE_KEYS = {
    "id",
    "cluster",
    "customers_out",
    "n_out",
    "etr",
    "etr_confidence",
    "cause",
    "comments",
    "crew_status",
    "start_time",
    "latitude",
    "longitude",
    "distance_km",
}


# --------------------------
# Debug / controlled errors
# --------------------------
def _dbg(debug: bool, msg: str) -> None:
    if debug:
        print(msg, flush=True)


def _err(msg: str) -> RuntimeError:
    return RuntimeError(f"AustinEnergyProviderError: {msg}")


@dataclass
class _Timers:
    t0: float
    sanity: float = 0.0
    discovery: float = 0.0
    fetch: float = 0.0
    drill: float = 0.0

    def total(self) -> float:
        return time.perf_counter() - self.t0


# --------------------------
# General parsing helpers
# --------------------------
def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_time(val: Any) -> Optional[str]:
    if val is None:
        return None

    if isinstance(val, (int, float)):
        ts = float(val)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return _utc_iso(datetime.fromtimestamp(ts, tz=timezone.utc))
        except Exception:
            return None

    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if re.fullmatch(r"\d{10,13}", s):
            try:
                n = int(s)
                if n > 1e12:
                    n //= 1000
                return _utc_iso(datetime.fromtimestamp(n, tz=timezone.utc))
            except Exception:
                return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return _utc_iso(dt)
        except Exception:
            return None

    return None


def _norm_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    if isinstance(val, dict):
        for k in ("EN-US", "en-US", "en", "default", "orig"):
            if k in val and isinstance(val[k], str) and val[k].strip():
                return val[k].strip()
        for v in val.values():
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _safe_int(val: Any) -> Optional[int]:
    try:
        if val is None:
            return None
        if isinstance(val, bool):
            return int(val)
        if isinstance(val, (int, float)):
            return int(val)
        if isinstance(val, str) and val.strip():
            return int(float(val.strip()))
    except Exception:
        return None
    return None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# --------------------------
# Polyline decoding (geom)
# --------------------------
def _decode_polyline(s: str) -> List[Tuple[float, float]]:
    coords: List[Tuple[float, float]] = []
    index = 0
    lat = 0
    lon = 0

    while index < len(s):
        result = 0
        shift = 0
        while True:
            b = ord(s[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        result = 0
        shift = 0
        while True:
            b = ord(s[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlon = ~(result >> 1) if (result & 1) else (result >> 1)
        lon += dlon

        coords.append((lat / 1e5, lon / 1e5))

    return coords


def _centroid_from_geom(geom: Any) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(geom, dict):
        return None, None

    # Point polyline
    p = geom.get("p")
    if isinstance(p, list) and p and isinstance(p[0], str):
        pts = _decode_polyline(p[0])
        if pts:
            return pts[0][0], pts[0][1]

    # Area polyline (polygon-ish)
    a = geom.get("a")
    if isinstance(a, list) and a and isinstance(a[0], str):
        pts = _decode_polyline(a[0])
        if pts:
            return (
                sum(x for x, _ in pts) / len(pts),
                sum(y for _, y in pts) / len(pts),
            )

    return None, None


# --------------------------
# Quadkeys + neighbors
# --------------------------
def _quadkey_from_latlon(lat: float, lon: float, zoom: int) -> str:
    lat = max(min(lat, 85.05112878), -85.05112878)
    n = 2**zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1.0 / math.cos(lat_rad))) / math.pi) / 2.0 * n)

    q = []
    for i in range(zoom, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if x & mask:
            digit += 1
        if y & mask:
            digit += 2
        q.append(str(digit))
    return "".join(q)


def _quadkey_from_tile(x: int, y: int, zoom: int) -> str:
    q = []
    for i in range(zoom, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if x & mask:
            digit += 1
        if y & mask:
            digit += 2
        q.append(str(digit))
    return "".join(q)


def _neighbors_for_quadkey(qk: str, depth: int) -> List[str]:
    if depth <= 0:
        return [qk]

    zoom = len(qk)
    x = y = 0
    for i, c in enumerate(qk):
        bit = zoom - i - 1
        mask = 1 << bit
        d = int(c)
        if d & 1:
            x |= mask
        if d & 2:
            y |= mask

    out = []
    n = 2**zoom
    for dx in range(-depth, depth + 1):
        for dy in range(-depth, depth + 1):
            xx = x + dx
            yy = y + dy
            if 0 <= xx < n and 0 <= yy < n:
                out.append(_quadkey_from_tile(xx, yy, zoom))
    return out


# --------------------------
# Kubra tile URL scheme
# --------------------------
def _shard_dir(qkh: str) -> str:
    # reverse(last3(qkh)) -> "021"
    return qkh[-3:][::-1]


def _tile_url(state_uuid: str, cluster_level: int, qkh: str) -> str:
    shard = _shard_dir(qkh)
    return (
        f"https://kubra.io/cluster-data/{shard}/"
        f"{AUSTIN_DATASET_UUID}/{state_uuid}/public/"
        f"cluster-{cluster_level}/{qkh}.json"
    )


# --------------------------
# State UUID cache helpers
# --------------------------
def _load_cached_state_context() -> Tuple[Optional[str], Optional[int]]:
    try:
        with open(STATE_UUID_CACHE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        state_uuid = obj.get("state_uuid")
        entry_zoom = obj.get("entry_zoom")
        return (
            state_uuid if isinstance(state_uuid, str) and state_uuid.strip() else None,
            _safe_int(entry_zoom),
        )
    except Exception:
        return None, None


def _save_cached_state_context(state_uuid: str, entry_zoom: Optional[int] = None) -> None:
    try:
        payload: Dict[str, Any] = {"state_uuid": state_uuid}
        if isinstance(entry_zoom, int):
            payload["entry_zoom"] = entry_zoom
        with open(STATE_UUID_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception:
        pass


# --------------------------
# HTTP helpers
# --------------------------
def _http_get_json(session: requests.Session, url: str, debug: bool, timeout: float = AUSTIN_TILE_TIMEOUT_S) -> Optional[dict]:
    _dbg(debug, f"PROBE GET {url}")
    try:
        verify_arg: Any = AUSTIN_SSL_CA_BUNDLE if AUSTIN_SSL_CA_BUNDLE else AUSTIN_SSL_VERIFY
        r = limited_get(session, url, headers=DEFAULT_HEADERS, timeout=timeout, verify=verify_arg)
    except Exception as e:
        _dbg(debug, f"PROBE FAIL {url} exc={type(e).__name__}")
        return None
    _dbg(debug, f"PROBE {'SUCCESS' if r.ok else 'FAIL'} {url} status={r.status_code}")
    if not r.ok:
        return None
    try:
        j = r.json()
        return j if isinstance(j, dict) else None
    except Exception:
        return None


# --------------------------
# SANITY GATE (0 outages => return empty)
# --------------------------
def _fetch_interval_blob(session: requests.Session, debug: bool) -> Optional[dict]:
    cfg = _http_get_json(session, CONFIG_URL, debug=debug, timeout=AUSTIN_SANITY_TIMEOUT_S)
    if not isinstance(cfg, dict):
        return None

    path = ((cfg.get("data") or {}).get("interval_generation_data"))
    if not isinstance(path, str) or not path.strip():
        return None

    path = path.strip().lstrip("/")

    # Kubra deployments vary; probe bounded variants
    candidates = [
        f"https://kubra.io/{path}",
        f"https://kubra.io/{path}.json",
        f"https://kubra.io/{path}/public",
        f"https://kubra.io/{path}/public.json",
    ]
    for url in candidates:
        blob = _http_get_json(session, url, debug=debug, timeout=AUSTIN_DISCOVERY_TIMEOUT_S)
        if isinstance(blob, dict):
            return blob
    return None


def _fetch_current_state_uuid(session: requests.Session, debug: bool) -> Optional[str]:
    cur = _http_get_json(session, CURRENT_STATE_URL, debug=debug, timeout=10.0)
    if not isinstance(cur, dict):
        return None

    # Most deployments expose the UUID in data.currentStateId/stateId.
    data = cur.get("data")
    if not isinstance(data, dict):
        data = cur

    for key in ("currentStateId", "stateId", "stateUUID", "state_uuid"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    # Austin currentState commonly embeds the active state UUID in a cluster path:
    # cluster-data/{qkh}/<dataset_uuid>/<state_uuid>
    cluster_path = data.get("cluster_interval_generation_data")
    if isinstance(cluster_path, str) and cluster_path.strip():
        parts = [p for p in cluster_path.strip().split("/") if p]
        if len(parts) >= 4:
            candidate = parts[3]
            if re.fullmatch(r"[0-9a-fA-F-]{36}", candidate):
                return candidate

    # Fallback: interval_generation_data often ends with the active UUID.
    interval_path = data.get("interval_generation_data")
    if isinstance(interval_path, str) and interval_path.strip():
        tail = interval_path.strip().split("/")[-1]
        if re.fullmatch(r"[0-9a-fA-F-]{36}", tail):
            return tail

    # Last-resort shallow scan for any UUID-like value.
    for value in data.values():
        if isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F-]{36}", value.strip()):
            return value.strip()

    return None


def _find_int(blob: dict, keys: List[str]) -> Optional[int]:
    # direct
    for k in keys:
        v = blob.get(k)
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())

    # one-level nested
    for v in blob.values():
        if isinstance(v, dict):
            for k in keys:
                vv = v.get(k)
                if isinstance(vv, (int, float)):
                    return int(vv)
                if isinstance(vv, str) and vv.strip().isdigit():
                    return int(vv.strip())
    return None


def _get_kubra_totals(blob: dict) -> Tuple[Optional[int], Optional[int]]:
    outage_keys = [
        "activeOutages", "active_outages", "outagesActive", "outageCount",
        "active_outage_count", "totalOutages", "total_outages"
    ]
    cust_keys = [
        "totalAffectedCustomers", "affectedCustomers", "customersAffected",
        "total_customers_affected"
    ]
    return _find_int(blob, outage_keys), _find_int(blob, cust_keys)


# --------------------------
# Tile parsing / normalization
# --------------------------
def _ensure_outage_shape(o: Dict[str, Any]) -> Dict[str, Any]:
    for k in OUTAGE_KEYS:
        if k not in o:
            o[k] = None
    o["cluster"] = bool(o.get("cluster", False))
    if not o.get("id") or not isinstance(o["id"], str):
        o["id"] = "Unknown"
    return o


def _parse_tile_items(tile_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = tile_json.get("file_data")
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    return []


def _normalize_tile_item(item: Dict[str, Any]) -> Dict[str, Any]:
    # Austin schema often places fields under item["desc"]
    desc = item.get("desc")
    if isinstance(desc, dict):
        src = dict(desc)
        src.update(item)  # top-level wins
    else:
        src = item

    outage_id = src.get("inc_id") or src.get("incident_id") or src.get("ticket_id") or src.get("id")
    if isinstance(outage_id, dict):
        outage_id = _norm_str(outage_id)
    outage_id = outage_id.strip() if isinstance(outage_id, str) and outage_id.strip() else "Unknown"

    customers_out = None
    cust_a = src.get("cust_a") or src.get("custA") or src.get("customersAffected")
    if isinstance(cust_a, dict):
        customers_out = _safe_int(cust_a.get("val") or cust_a.get("value"))
    customers_out = customers_out if customers_out is not None else _safe_int(src.get("customers_out"))

    n_out = _safe_int(src.get("n_out") or src.get("nOut") or src.get("outageCount"))

    etr = _parse_time(src.get("etr") or src.get("estimatedRestorationTime"))
    start_time = _parse_time(src.get("start_time") or src.get("startTime"))

    crew_status = _norm_str(src.get("crew_status") or src.get("crewStatus"))
    cause = _norm_str(src.get("cause"))
    comments = _norm_str(src.get("comments"))

    etr_confidence = src.get("etr_confidence")
    if isinstance(etr_confidence, dict):
        etr_confidence = _norm_str(etr_confidence)
    elif not isinstance(etr_confidence, (str, type(None))):
        etr_confidence = None

    lat, lon = _centroid_from_geom(src.get("geom") or item.get("geom"))

    o = {
        "id": outage_id,
        "cluster": bool(src.get("cluster", False)),
        "customers_out": customers_out,
        "n_out": n_out,
        "etr": etr,
        "etr_confidence": etr_confidence,
        "cause": cause,
        "comments": comments,
        "crew_status": crew_status,
        "start_time": start_time,
        "latitude": lat,
        "longitude": lon,
        "distance_km": None,
    }
    return _ensure_outage_shape(o)


# --------------------------
# Fetch tiles + drill clusters
# --------------------------
def _probe_entry_zoom(
    session: requests.Session,
    state_uuid: str,
    lat: float,
    lon: float,
    max_zoom: int,
    debug: bool,
) -> int:
    # bounded candidate list; Austin commonly starts around 10 and may expose
    # detail only at deeper quadkeys on the same cluster level.
    candidates = [10, 11, 12, 13, 14, 9, 8]
    candidates = [z for z in candidates if 1 <= z <= max_zoom]
    fallback_zoom: Optional[int] = None

    for z in candidates:
        qkh = _quadkey_from_latlon(lat, lon, z)
        url = _tile_url(state_uuid, AUSTIN_ENTRY_CLUSTER_LEVEL, qkh)
        j = _http_get_json(session, url, debug=debug, timeout=AUSTIN_TILE_TIMEOUT_S)

        if not isinstance(j, dict):
            continue

        if fallback_zoom is None:
            fallback_zoom = z

        items = _parse_tile_items(j)
        if not items:
            _dbg(debug, f"PROBE HIT (no features) entry_zoom={z} url={url}")
            continue

        has_cluster = False
        has_incident = False
        for item in items:
            o = _normalize_tile_item(item)
            if o.get("cluster"):
                has_cluster = True
            else:
                has_incident = True

        if has_cluster or has_incident:
            _dbg(
                debug,
                f"PROBE SUCCESS entry_zoom={z} url={url} "
                f"features={len(items)} clusters={has_cluster} incidents={has_incident}",
            )
            return z

    if fallback_zoom is not None:
        _dbg(debug, f"PROBE FALLBACK entry_zoom={fallback_zoom} (dict found, no feature-rich tile)")
        return fallback_zoom

    raise _err("Could not discover entry zoom for Austin Energy cluster tiles")


def _fetch_tiles(
    session: requests.Session,
    state_uuid: str,
    cluster_level: int,
    zoom: int,
    lat: float,
    lon: float,
    neighbor_depth: int,
    debug: bool,
) -> Tuple[int, List[Dict[str, Any]]]:
    qkh0 = _quadkey_from_latlon(lat, lon, zoom)
    qkhs = _neighbors_for_quadkey(qkh0, neighbor_depth)

    tiles_fetched = 0
    out: List[Dict[str, Any]] = []

    for qkh in qkhs:
        url = _tile_url(state_uuid, cluster_level, qkh)
        j = _http_get_json(session, url, debug=debug, timeout=AUSTIN_TILE_TIMEOUT_S)
        if not isinstance(j, dict):
            continue

        tiles_fetched += 1
        for item in _parse_tile_items(j):
            out.append(_normalize_tile_item(item))

    return tiles_fetched, out


def _drill_clusters(
    session: requests.Session,
    state_uuid: str,
    entry_zoom: int,
    max_zoom: int,
    clusters: List[Dict[str, Any]],
    drill_neighbor_depth: int,
    debug: bool,
) -> List[Dict[str, Any]]:
    if not clusters or entry_zoom >= max_zoom:
        return []

    drilled: List[Dict[str, Any]] = []
    z = entry_zoom
    clvl = AUSTIN_ENTRY_CLUSTER_LEVEL

    while z < max_zoom:
        z += 1
        clvl += 1
        _dbg(debug, f"DRILL cluster z={z-1} -> z={z} (cluster-{clvl})")

        next_clusters: List[Dict[str, Any]] = []

        for c in clusters:
            clat = c.get("latitude")
            clon = c.get("longitude")
            if clat is None or clon is None:
                continue

            tiles_fetched, outs = _fetch_tiles(
                session=session,
                state_uuid=state_uuid,
                cluster_level=clvl,
                zoom=z,
                lat=float(clat),
                lon=float(clon),
                neighbor_depth=drill_neighbor_depth,
                debug=debug,
            )
            _dbg(debug, f"DRILL fetch: cluster-{clvl} zoom={z} tiles_fetched={tiles_fetched} features={len(outs)}")

            for o in outs:
                if o.get("cluster"):
                    next_clusters.append(o)
                else:
                    drilled.append(o)

        clusters = next_clusters
        if not clusters:
            break

    return drilled


def _drill_same_cluster_level(
    session: requests.Session,
    state_uuid: str,
    cluster_level: int,
    start_zoom: int,
    max_zoom: int,
    lat: float,
    lon: float,
    neighbor_depth: int,
    debug: bool,
) -> List[Dict[str, Any]]:
    """
    Some Austin deployments keep outage features on the same cluster level
    while increasing quadkey zoom depth (e.g. cluster-1 with 12-digit quadkeys).
    """
    if start_zoom >= max_zoom:
        return []

    out: List[Dict[str, Any]] = []
    for z in range(start_zoom + 1, max_zoom + 1):
        tiles_fetched, outs = _fetch_tiles(
            session=session,
            state_uuid=state_uuid,
            cluster_level=cluster_level,
            zoom=z,
            lat=lat,
            lon=lon,
            neighbor_depth=neighbor_depth,
            debug=debug,
        )
        _dbg(
            debug,
            f"SAME-LEVEL fetch: cluster-{cluster_level} zoom={z} tiles_fetched={tiles_fetched} features={len(outs)}",
        )
        for o in outs:
            if not o.get("cluster"):
                out.append(o)
    return out


def refresh_austin_discovery(debug: bool = False) -> None:
    """
    Warm Austin discovery context (state UUID + entry zoom) for faster first lookup.
    Never raises; logs warnings on failure.
    """
    try:
        session = requests.Session()
        state_candidates: List[str] = []

        current_state = _fetch_current_state_uuid(session, debug=debug)
        cached_state, _ = _load_cached_state_context()

        for candidate in (current_state, AUSTIN_STATE_UUID, cached_state, AUSTIN_LOCKED_STATE_UUID):
            if candidate and candidate not in state_candidates:
                state_candidates.append(candidate)

        last_error: Optional[str] = None
        for state_uuid in state_candidates:
            try:
                entry_zoom = _probe_entry_zoom(
                    session,
                    state_uuid,
                    AUSTIN_WARM_LAT,
                    AUSTIN_WARM_LON,
                    max_zoom=AUSTIN_WARM_MAX_ZOOM,
                    debug=debug,
                )
                _save_cached_state_context(state_uuid, entry_zoom)
                _log.info(
                    "Austin discovery warm-start ok: state_uuid=%s entry_zoom=%s",
                    state_uuid,
                    entry_zoom,
                )
                return
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"

        _log.warning("Austin discovery warm-start failed: %s", last_error or "no state candidates")
    except Exception as exc:
        _log.warning("Austin discovery warm-start failed: %s: %s", type(exc).__name__, exc)


# --------------------------
# PUBLIC FUNCTION
# --------------------------
def fetch_austin_energy_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 16.1,
    fallback_radius_km: float = 40.2,
    max_zoom: int = 14,
    neighbor_depth: int = 1,
    drill_neighbor_depth: int = 1,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Returns:
      { "nearest": <outage|null>, "outages": [<outage>...] }

    Controlled failure via RuntimeError("AustinEnergyProviderError: ...")
    """
    timers = _Timers(t0=time.perf_counter())
    session = requests.Session()
    if debug and not AUSTIN_SSL_VERIFY:
        _dbg(debug, "TLS verify disabled via AUSTIN_SSL_VERIFY=false")
    if debug and AUSTIN_SSL_CA_BUNDLE:
        _dbg(debug, f"TLS CA bundle override via AUSTIN_SSL_CA_BUNDLE={AUSTIN_SSL_CA_BUNDLE}")

    cached_state_uuid, cached_entry_zoom = _load_cached_state_context()
    state_candidates: List[str] = []
    for candidate in (AUSTIN_STATE_UUID, cached_state_uuid):
        if candidate and candidate not in state_candidates:
            state_candidates.append(candidate)

    # ---- fast path: reuse cached state + entry zoom when available ----
    chosen_state: Optional[str] = None
    entry_zoom: Optional[int] = None
    base_outs: List[Dict[str, Any]] = []

    fast_zoom: Optional[int] = None
    if AUSTIN_ENTRY_ZOOM:
        fast_zoom = _safe_int(AUSTIN_ENTRY_ZOOM)
    elif cached_entry_zoom is not None:
        fast_zoom = cached_entry_zoom

    if state_candidates and fast_zoom is not None:
        for state_uuid in state_candidates:
            t_fetch0 = time.perf_counter()
            tiles_fetched, outs = _fetch_tiles(
                session=session,
                state_uuid=state_uuid,
                cluster_level=AUSTIN_ENTRY_CLUSTER_LEVEL,
                zoom=fast_zoom,
                lat=lat,
                lon=lon,
                neighbor_depth=neighbor_depth,
                debug=debug,
            )
            timers.fetch += time.perf_counter() - t_fetch0
            _dbg(debug, f"fast-path fetch counts: tiles_fetched={tiles_fetched} feature_count={len(outs)}")
            if outs:
                chosen_state = state_uuid
                entry_zoom = fast_zoom
                base_outs = outs
                break

    # ---- sanity gate ----
    if chosen_state is None:
        t_sanity0 = time.perf_counter()
        blob = _fetch_interval_blob(session, debug=debug)
        if isinstance(blob, dict):
            active_outages, affected_customers = _get_kubra_totals(blob)
            _dbg(debug, f"SANITY: active_outages={active_outages} affected_customers={affected_customers}")
            if active_outages == 0 and (affected_customers in (0, None)):
                timers.sanity += time.perf_counter() - t_sanity0
                if debug:
                    _dbg(debug, f"timing summary: total={timers.total():.3f}s sanity={timers.sanity:.3f}s")
                return {"nearest": None, "outages": []}
        else:
            _dbg(debug, "SANITY: interval blob not available; proceeding with tile pipeline")
        timers.sanity += time.perf_counter() - t_sanity0

    # ---- state UUID candidates (rotation fallback) ----
    if chosen_state is None:
        current_state = _fetch_current_state_uuid(session, debug=debug)
        if current_state:
            # Prefer authoritative current state to avoid stale incident artifacts.
            state_candidates.append(current_state)
        else:
            # Only use static/cached fallbacks when current state is unavailable.
            state_candidates.append(AUSTIN_LOCKED_STATE_UUID)
            if cached_state_uuid and cached_state_uuid not in state_candidates:
                state_candidates.append(cached_state_uuid)

    if chosen_state is None:
        # ---- discovery + fetch using first working state uuid ----
        t_dis0 = time.perf_counter()
        last_probe_err: Optional[str] = None

        for state_uuid in state_candidates:
            try:
                z = _probe_entry_zoom(session, state_uuid, lat, lon, max_zoom=max_zoom, debug=debug)
            except Exception as e:
                last_probe_err = str(e)
                continue

            chosen_state = state_uuid
            entry_zoom = z
            break

        timers.discovery += time.perf_counter() - t_dis0

        if chosen_state is None or entry_zoom is None:
            raise _err(last_probe_err or "Could not initialize Austin tile probing")

        _dbg(debug, f"discovered dataset_uuid={AUSTIN_DATASET_UUID} state_uuid={chosen_state}")
        _dbg(debug, f"discovered shard_scheme=last3_rev entry_zoom={entry_zoom}")

        # persist the working state UUID and last known working zoom
        _save_cached_state_context(chosen_state, entry_zoom)

        # ---- base fetch ----
        t_fetch0 = time.perf_counter()
        tiles_fetched, outs = _fetch_tiles(
            session=session,
            state_uuid=chosen_state,
            cluster_level=AUSTIN_ENTRY_CLUSTER_LEVEL,
            zoom=entry_zoom,
            lat=lat,
            lon=lon,
            neighbor_depth=neighbor_depth,
            debug=debug,
        )
        timers.fetch += time.perf_counter() - t_fetch0
        _dbg(debug, f"fetch counts: tiles_fetched={tiles_fetched} feature_count={len(outs)}")
    else:
        _dbg(debug, f"fast-path discovered dataset_uuid={AUSTIN_DATASET_UUID} state_uuid={chosen_state}")
        _dbg(debug, f"fast-path entry_zoom={entry_zoom}")
        _save_cached_state_context(chosen_state, entry_zoom)

    if base_outs:
        outs = base_outs

    base_clusters = [o for o in outs if o.get("cluster") is True]
    base_incidents = [o for o in outs if not o.get("cluster")]

    # ---- drill clusters ----
    t_drill0 = time.perf_counter()
    drilled_incidents: List[Dict[str, Any]] = []
    if base_clusters and entry_zoom < max_zoom and (time.perf_counter() - timers.t0) <= 11.0:
        drilled_incidents = _drill_clusters(
            session=session,
            state_uuid=chosen_state,
            entry_zoom=entry_zoom,
            max_zoom=max_zoom,
            clusters=base_clusters,
            drill_neighbor_depth=drill_neighbor_depth,
            debug=debug,
        )

    # Austin can expose non-cluster incidents at deeper quadkeys but same cluster level.
    same_level_incidents: List[Dict[str, Any]] = []
    if (time.perf_counter() - timers.t0) <= 11.0:
        same_level_incidents = _drill_same_cluster_level(
            session=session,
            state_uuid=chosen_state,
            cluster_level=AUSTIN_ENTRY_CLUSTER_LEVEL,
            start_zoom=entry_zoom,
            max_zoom=max_zoom,
            lat=lat,
            lon=lon,
            neighbor_depth=drill_neighbor_depth,
            debug=debug,
        )
    timers.drill += time.perf_counter() - t_drill0

    normalized = base_incidents + drilled_incidents + same_level_incidents

    # ---- distance + radius filter ----
    with_dist: List[Dict[str, Any]] = []
    for o in normalized:
        if o.get("cluster"):
            continue
        if o.get("latitude") is None or o.get("longitude") is None:
            continue
        o["distance_km"] = _haversine_km(lat, lon, float(o["latitude"]), float(o["longitude"]))
        with_dist.append(o)

    within = [o for o in with_dist if o["distance_km"] is not None and o["distance_km"] <= max_radius_km]
    if not within and fallback_radius_km and fallback_radius_km > max_radius_km:
        within = [o for o in with_dist if o["distance_km"] is not None and o["distance_km"] <= fallback_radius_km]

    # ---- dedupe ----
    # Austin tiles can emit aliases of the same physical outage with different ids
    # across neighboring/deeper quadkeys. Collapse by a physical+time signature first,
    # then keep a stable representative.
    dedup: Dict[Tuple[int, int, Optional[str], Optional[str], Optional[int]], Dict[str, Any]] = {}
    for o in within:
        lat_i = int(round(float(o["latitude"]) * 1_000_000))
        lon_i = int(round(float(o["longitude"]) * 1_000_000))
        key = (
            lat_i,
            lon_i,
            o.get("start_time"),
            o.get("etr"),
            o.get("customers_out"),
        )

        if key not in dedup:
            dedup[key] = o
            continue

        # Prefer richer outage details, then a stable (lexicographically smaller) id.
        cur = dedup[key]
        cur_c = cur.get("customers_out") or 0
        new_c = o.get("customers_out") or 0
        if isinstance(new_c, int) and isinstance(cur_c, int) and new_c > cur_c:
            dedup[key] = o
            continue

        cur_id = str(cur.get("id") or "")
        new_id = str(o.get("id") or "")
        if new_id and (not cur_id or new_id < cur_id):
            dedup[key] = o

    within = list(dedup.values())
    within.sort(key=lambda x: (x["distance_km"] if x.get("distance_km") is not None else 1e9))
    nearest = within[0] if within else None

    if debug:
        _dbg(
            debug,
            f"timing summary: total={timers.total():.3f}s sanity={timers.sanity:.3f}s "
            f"discovery={timers.discovery:.3f}s fetch={timers.fetch:.3f}s drill={timers.drill:.3f}s",
        )

    return {"nearest": nearest, "outages": within}


if __name__ == "__main__":
    # quick local self-test
    t0 = time.perf_counter()
    res = fetch_austin_energy_outages(
        30.2672, -97.7431,
        max_radius_km=16.1,
        fallback_radius_km=40.2,
        max_zoom=12,
        neighbor_depth=1,
        drill_neighbor_depth=1,
        debug=True,
    )
    print(f"\nTotal outages returned: {len(res['outages'])}")
    print("Nearest:", res["nearest"])
    print(f"Elapsed: {time.perf_counter() - t0:.3f}s")
