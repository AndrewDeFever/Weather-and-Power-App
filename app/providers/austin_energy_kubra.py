from __future__ import annotations

import json
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# ============================================================
# Austin Energy (Kubra) provider with sanity gate + full tiles
# ============================================================

# --- Locked anchors from working public tile URL ---
AUSTIN_DATASET_UUID = "20d293c6-08fd-41b3-b96b-d2f522c74990"
AUSTIN_LOCKED_STATE_UUID = "e32748cf-d34d-4844-8400-5340fba1a35b"
AUSTIN_ENTRY_CLUSTER_LEVEL = 1

# Cache for last-known-good state UUID (rotation fallback)
STATE_UUID_CACHE_FILE = os.getenv("AUSTIN_STATE_UUID_CACHE_FILE", "/tmp/austin_energy_state_uuid.json")

# Kubra config endpoint you provided (used for sanity gate)
CONFIG_URL = (
    "https://kubra.io/stormcenter/api/v1/stormcenters/"
    "dd9c446f-f6b8-43f9-8f80-83f5245c60a1/"
    "views/76446308-a901-4fa3-849c-3dd569933a51/"
    "configuration/53b6bbf9-126a-43cd-8eb5-eca49ade8eb4"
    "?preview=false"
)

DEFAULT_HEADERS = {
    "User-Agent": "NOC-AustinEnergyKubraProvider/2.0",
    "Accept": "application/json,text/plain,*/*",
}

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
def _load_cached_state_uuid() -> Optional[str]:
    try:
        with open(STATE_UUID_CACHE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        v = obj.get("state_uuid")
        return v if isinstance(v, str) and v.strip() else None
    except Exception:
        return None


def _save_cached_state_uuid(state_uuid: str) -> None:
    try:
        with open(STATE_UUID_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"state_uuid": state_uuid}, f)
    except Exception:
        pass


# --------------------------
# HTTP helpers
# --------------------------
def _http_get_json(session: requests.Session, url: str, debug: bool, timeout: float = 10.0) -> Optional[dict]:
    _dbg(debug, f"PROBE GET {url}")
    try:
        r = session.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
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
    cfg = _http_get_json(session, CONFIG_URL, debug=debug, timeout=10.0)
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
        blob = _http_get_json(session, url, debug=debug, timeout=10.0)
        if isinstance(blob, dict):
            return blob
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
    # bounded candidate list; common Kubra entry zooms
    candidates = [9, 10, 11, 12, 8, 13, 14]
    candidates = [z for z in candidates if 1 <= z <= max_zoom]

    for z in candidates:
        qkh = _quadkey_from_latlon(lat, lon, z)
        url = _tile_url(state_uuid, AUSTIN_ENTRY_CLUSTER_LEVEL, qkh)
        j = _http_get_json(session, url, debug=debug, timeout=8.0)
        if isinstance(j, dict):
            _dbg(debug, f"PROBE SUCCESS entry_zoom={z} url={url}")
            return z

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
        j = _http_get_json(session, url, debug=debug, timeout=8.0)
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


# --------------------------
# PUBLIC FUNCTION
# --------------------------
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

    # ---- sanity gate ----
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
    state_candidates: List[str] = [AUSTIN_LOCKED_STATE_UUID]
    cached = _load_cached_state_uuid()
    if cached and cached not in state_candidates:
        state_candidates.append(cached)

    # ---- discovery + fetch using first working state uuid ----
    chosen_state: Optional[str] = None
    entry_zoom: Optional[int] = None
    base_outs: List[Dict[str, Any]] = []

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

    # persist the working state UUID
    _save_cached_state_uuid(chosen_state)

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

    base_clusters = [o for o in outs if o.get("cluster") is True]
    base_incidents = [o for o in outs if not o.get("cluster")]

    # ---- drill clusters ----
    t_drill0 = time.perf_counter()
    drilled_incidents: List[Dict[str, Any]] = []
    if base_clusters and entry_zoom < max_zoom:
        drilled_incidents = _drill_clusters(
            session=session,
            state_uuid=chosen_state,
            entry_zoom=entry_zoom,
            max_zoom=max_zoom,
            clusters=base_clusters,
            drill_neighbor_depth=drill_neighbor_depth,
            debug=debug,
        )
    timers.drill += time.perf_counter() - t_drill0

    normalized = base_incidents + drilled_incidents

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

    # ---- dedupe (id + lat/lon) ----
    dedup: Dict[Tuple[str, int, int], Dict[str, Any]] = {}
    for o in within:
        oid = o.get("id") or "Unknown"
        lat_i = int(round(float(o["latitude"]) * 1_000_000))
        lon_i = int(round(float(o["longitude"]) * 1_000_000))
        key = (oid, lat_i, lon_i)

        if key not in dedup:
            dedup[key] = o
            continue

        # prefer higher customers_out if available
        cur = dedup[key]
        cur_c = cur.get("customers_out") or 0
        new_c = o.get("customers_out") or 0
        if isinstance(new_c, int) and isinstance(cur_c, int) and new_c > cur_c:
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
