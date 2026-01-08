# app/providers/austin_energy_kubra.py
"""
Austin Energy (Kubra) provider — LOCKED public tile anchors.

Locked (from confirmed working tile URL):
- dataset_uuid: 20d293c6-08fd-41b3-b96b-d2f522c74990
- state_uuid:   e32748cf-d34d-4844-8400-5340fba1a35b
- entry cluster level: cluster-1
- shard strategy: dir = reverse(last3(qkh))

We still:
- probe entry zoom (quadkey length) at runtime
- drill clusters down to max_zoom
- return only non-cluster outages
- never invent IDs (inc_id null => "Unknown")
- normalize localized strings ({"EN-US": ...})
- parse times to ISO-8601 UTC
"""

from __future__ import annotations

import json
import os
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests


# -------- Locked anchors (confirmed) --------
AUSTIN_DATASET_UUID = "20d293c6-08fd-41b3-b96b-d2f522c74990"
AUSTIN_STATE_UUID = "e32748cf-d34d-4844-8400-5340fba1a35b"
AUSTIN_STATE_UUID_CACHE_FILE = os.environ.get("AUSTIN_STATE_UUID_CACHE_FILE", "/tmp/austin_energy_state_uuid.json")
AUSTIN_ENTRY_CLUSTER_LEVEL = 1

# shard strategy: reverse(last3(qkh)) => directory like "021"
def _shard_dir(qkh: str) -> str:
    return qkh[-3:][::-1]


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

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NOC-AustinEnergyKubraProvider/1.2)",
    "Accept": "application/json,text/plain,*/*",
}


def _controlled_error(msg: str) -> RuntimeError:
    return RuntimeError(f"AustinEnergyProviderError: {msg}")


def _dbg(debug: bool, msg: str) -> None:
    if debug:
        print(msg, flush=True)


def _load_cached_state_uuid(debug: bool = False) -> Optional[str]:
    try:
        fp = AUSTIN_STATE_UUID_CACHE_FILE
        if not fp:
            return None
        if not os.path.exists(fp):
            return None
        with open(fp, "r", encoding="utf-8") as f:
            data = json.load(f)
        v = data.get("state_uuid")
        if isinstance(v, str) and re.fullmatch(r"[0-9a-fA-F-]{36}", v):
            _dbg(debug, f"loaded cached state_uuid={v} from {fp}")
            return v
    except Exception:
        # controlled: ignore cache read errors
        return None
    return None


def _save_cached_state_uuid(state_uuid: str, debug: bool = False) -> None:
    try:
        fp = AUSTIN_STATE_UUID_CACHE_FILE
        if not fp:
            return
        tmp = fp + ".tmp"
        os.makedirs(os.path.dirname(fp) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"state_uuid": state_uuid, "saved_at_utc": _utc_iso(datetime.now(timezone.utc))}, f)
        os.replace(tmp, fp)
        _dbg(debug, f"saved cached state_uuid={state_uuid} to {fp}")
    except Exception:
        # controlled: ignore cache write errors
        return


@dataclass
class _Timers:
    t0: float
    discovery: float = 0.0
    fetch: float = 0.0
    drill: float = 0.0

    def total(self) -> float:
        return time.perf_counter() - self.t0


# -----------------------------
# Core helpers
# -----------------------------
def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_any_time_to_utc_iso(val: Any) -> Optional[str]:
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


def _norm_localized_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    if isinstance(val, dict):
        for k in ("EN-US", "en-US", "en", "EN", "default", "orig"):
            if k in val and isinstance(val[k], str) and val[k].strip():
                return val[k].strip()
        for _, v in val.items():
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


# -----------------------------
# Quadkeys (qkh)
# -----------------------------
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


# -----------------------------
# Encoded polyline decoding (geom.p / geom.a)
# -----------------------------
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

    p = geom.get("p")
    if isinstance(p, list) and p and isinstance(p[0], str):
        pts = _decode_polyline(p[0])
        if pts:
            return pts[0][0], pts[0][1]

    a = geom.get("a")
    if isinstance(a, list) and a and isinstance(a[0], str):
        pts = _decode_polyline(a[0])
        if pts:
            return (
                sum(x for x, _ in pts) / len(pts),
                sum(y for _, y in pts) / len(pts),
            )
    return None, None


# -----------------------------
# HTTP
# -----------------------------
def _http_get(session: requests.Session, url: str, debug: bool, timeout: float = 10.0) -> requests.Response:
    _dbg(debug, f"PROBE GET {url}")
    r = session.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    _dbg(debug, f"PROBE {'SUCCESS' if r.ok else 'FAIL'} {url} status={r.status_code}")
    return r


# -----------------------------
# Tile URL + probing
# -----------------------------
def _tile_url(dataset_uuid: str, state_uuid: str, cluster_level: int, qkh: str) -> str:
    shard = _shard_dir(qkh)
    return f"https://kubra.io/cluster-data/{shard}/{dataset_uuid}/{state_uuid}/public/cluster-{cluster_level}/{qkh}.json"


def _probe_entry_zoom(
    session: requests.Session,
    dataset_uuid: str,
    state_uuid: str,
    cluster_level: int,
    lat: float,
    lon: float,
    max_zoom: int,
    debug: bool,
) -> int:
    # Bounded candidates; your sample qkh length=9, but we still verify.
    candidates = [9, 10, 11, 12, 8, 13, 14]
    candidates = [z for z in candidates if 1 <= z <= max_zoom]
    seen = set()
    candidates = [z for z in candidates if not (z in seen or seen.add(z))]

    for z in candidates:
        qkh = _quadkey_from_latlon(lat, lon, z)
        url = _tile_url(dataset_uuid, state_uuid, cluster_level, qkh)
        r = _http_get(session, url, debug, timeout=8.0)
        if not r.ok:
            continue
        try:
            _ = r.json()
            _dbg(debug, f"PROBE SUCCESS entry_zoom={z} url={url}")
            return z
        except Exception:
            continue

    raise _controlled_error("Could not discover entry zoom for Austin Energy cluster tiles")


# -----------------------------
# Tile parsing (Austin schema)
# -----------------------------
def _ensure_outage_shape(o: Dict[str, Any]) -> Dict[str, Any]:
    for k in OUTAGE_KEYS:
        if k not in o:
            o[k] = None
    o["cluster"] = bool(o.get("cluster", False))
    if not o.get("id") or not isinstance(o["id"], str):
        o["id"] = "Unknown"
    return o


def _parse_tile_items(tile_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(tile_json, dict):
        return []
    items = tile_json.get("file_data")
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    return []


def _normalize_tile_item(item: Dict[str, Any]) -> Dict[str, Any]:
    # Some tiles put outage fields under item["desc"]
    desc = item.get("desc")
    if isinstance(desc, dict):
        merged = dict(desc)
        merged.update(item)  # top-level wins if duplicates
        src = merged
    else:
        src = item

    outage_id = src.get("inc_id") or src.get("incident_id") or src.get("ticket_id") or src.get("id")
    if isinstance(outage_id, dict):
        outage_id = _norm_localized_str(outage_id)
    outage_id = outage_id.strip() if isinstance(outage_id, str) and outage_id.strip() else "Unknown"

    # customers_out: cust_a.val (sometimes nested)
    customers_out = None
    cust_a = src.get("cust_a") or src.get("custA") or src.get("customersAffected")  # still no guessing: alt key names only
    if isinstance(cust_a, dict):
        customers_out = _safe_int(cust_a.get("val") or cust_a.get("value"))
    customers_out = customers_out if customers_out is not None else _safe_int(src.get("customers_out"))

    n_out = _safe_int(src.get("n_out") or src.get("nOut") or src.get("outageCount"))

    etr = _parse_any_time_to_utc_iso(src.get("etr") or src.get("estimatedRestorationTime"))
    start_time = _parse_any_time_to_utc_iso(src.get("start_time") or src.get("startTime"))

    crew_status = _norm_localized_str(src.get("crew_status") or src.get("crewStatus"))
    cause = _norm_localized_str(src.get("cause"))
    comments = _norm_localized_str(src.get("comments"))

    etr_confidence = src.get("etr_confidence")
    if isinstance(etr_confidence, dict):
        etr_confidence = _norm_localized_str(etr_confidence)
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



def _fetch_tiles(
    session: requests.Session,
    dataset_uuid: str,
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
        url = _tile_url(dataset_uuid, state_uuid, cluster_level, qkh)
        r = _http_get(session, url, debug, timeout=8.0)
        if not r.ok:
            continue
        try:
            tile_json = r.json()
        except Exception:
            continue

        tiles_fetched += 1
        for item in _parse_tile_items(tile_json):
            out.append(_normalize_tile_item(item))

    return tiles_fetched, out


def _drill_clusters(
    session: requests.Session,
    dataset_uuid: str,
    state_uuid: str,
    entry_cluster_level: int,
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
    clvl = entry_cluster_level

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
                dataset_uuid=dataset_uuid,
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


# -----------------------------
# Public API
# -----------------------------
def fetch_austin_energy_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 16.1,
    fallback_radius_km: float = 40.2,
    max_zoom: int = 12,
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

    # ---- discovery: probe entry zoom only (anchors locked) ----
    t_dis0 = time.perf_counter()

    # Rotation fallback (no extra network calls beyond the same probe set):
    # Try primary state UUID, then cached last-known-good UUID (if different).
    state_candidates: List[str] = [AUSTIN_STATE_UUID]
    cached = _load_cached_state_uuid(debug=debug)
    if cached and cached != AUSTIN_STATE_UUID:
        state_candidates.append(cached)

    entry_zoom: Optional[int] = None
    state_uuid_used: Optional[str] = None

    last_err: Optional[Exception] = None
    for su in state_candidates:
        try:
            _dbg(debug, f"PROBE state_uuid candidate={su}")
            ez = _probe_entry_zoom(
                session=session,
                dataset_uuid=AUSTIN_DATASET_UUID,
                state_uuid=su,
                cluster_level=AUSTIN_ENTRY_CLUSTER_LEVEL,
                lat=lat,
                lon=lon,
                max_zoom=max_zoom,
                debug=debug,
            )
            entry_zoom = ez
            state_uuid_used = su
            break
        except Exception as e:
            last_err = e
            continue

    if entry_zoom is None or state_uuid_used is None:
        # Preserve the original controlled error surface
        raise _controlled_error(str(last_err) if last_err else "Could not discover entry zoom for Austin Energy cluster tiles")

    # Persist last-known-good state UUID for future rotation
    _save_cached_state_uuid(state_uuid_used, debug=debug)

    _dbg(
        debug,
        f"discovered dataset_uuid={AUSTIN_DATASET_UUID} state_uuid={state_uuid_used} cluster_level=cluster-{AUSTIN_ENTRY_CLUSTER_LEVEL}",
    )
    _dbg(debug, f"discovered shard_scheme=last3_rev entry_zoom={entry_zoom}")
    timers.discovery += time.perf_counter() - t_dis0

    # ---- fetch base tiles ----
    t_fetch0 = time.perf_counter()
    tiles_fetched, outs = _fetch_tiles(
        session=session,
        dataset_uuid=AUSTIN_DATASET_UUID,
        state_uuid=state_uuid_used,
        cluster_level=AUSTIN_ENTRY_CLUSTER_LEVEL,
        zoom=entry_zoom,
        lat=lat,
        lon=lon,
        neighbor_depth=neighbor_depth,
        debug=debug,
    )
    _dbg(debug, f"fetch counts: tiles_fetched={tiles_fetched} feature_count={len(outs)}")
    timers.fetch += time.perf_counter() - t_fetch0

    base_clusters = [o for o in outs if o.get("cluster") is True]
    base_incidents = [o for o in outs if not o.get("cluster")]

    # ---- drill clusters ----
    t_drill0 = time.perf_counter()
    drilled_incidents: List[Dict[str, Any]] = []
    if base_clusters and entry_zoom < max_zoom:
        drilled_incidents = _drill_clusters(
            session=session,
            dataset_uuid=AUSTIN_DATASET_UUID,
            state_uuid=state_uuid_used,
            entry_cluster_level=AUSTIN_ENTRY_CLUSTER_LEVEL,
            entry_zoom=entry_zoom,
            max_zoom=max_zoom,
            clusters=base_clusters,
            drill_neighbor_depth=drill_neighbor_depth,
            debug=debug,
        )
    timers.drill += time.perf_counter() - t_drill0

    # ---- normalize, distance, filter ----
    normalized = base_incidents + drilled_incidents

    with_dist: List[Dict[str, Any]] = []
    for o in normalized:
        if o.get("cluster"):
            continue  # strict non-cluster output
        if o.get("latitude") is None or o.get("longitude") is None:
            continue
        o["distance_km"] = _haversine_km(lat, lon, float(o["latitude"]), float(o["longitude"]))
        with_dist.append(o)

    within = [o for o in with_dist if o["distance_km"] is not None and o["distance_km"] <= max_radius_km]
    if not within and fallback_radius_km and fallback_radius_km > max_radius_km:
        within = [o for o in with_dist if o["distance_km"] is not None and o["distance_km"] <= fallback_radius_km]

    # Deduplicate (id + coordinates). Prefer the record with higher customers_out when duplicates exist.
    dedup: Dict[Tuple[str, float, float], Dict[str, Any]] = {}
    for o in within:
        try:
            key = (str(o.get("id") or "Unknown"), round(float(o.get("latitude") or 0.0), 6), round(float(o.get("longitude") or 0.0), 6))
        except Exception:
            continue
        cur = dedup.get(key)
        if cur is None:
            dedup[key] = o
        else:
            if (o.get("customers_out") or 0) > (cur.get("customers_out") or 0):
                dedup[key] = o
    within = list(dedup.values())

    within.sort(key=lambda x: (x["distance_km"] if x.get("distance_km") is not None else 1e9))
    nearest = within[0] if within else None

    if debug:
        _dbg(
            debug,
            f"timing summary: total={timers.total():.3f}s discovery={timers.discovery:.3f}s fetch={timers.fetch:.3f}s drill={timers.drill:.3f}s",
        )

    return {"nearest": nearest, "outages": within}


if __name__ == "__main__":
    # Austin downtown
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
