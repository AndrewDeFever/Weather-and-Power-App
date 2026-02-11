# app/providers/evergy_kubra.py
"""
Evergy (Kansas) Kubra StormCenter outage fetcher.

Scope: Provider module only (no FastAPI/UI/weather).
Contract: Match OG&E / PSO outage object schema.

Confirmed Evergy behaviors (from captures):
- currentState:
    https://kubra.io/stormcenter/api/v1/stormcenters/<sc>/views/<view>/currentState?preview=false
- cluster template:
    currentState["data"]["cluster_interval_generation_data"]
    Example: cluster-data/{qkh}/<instance>/<generation>
- tile format:
    {"file_title": "...", "file_data": [ { desc:{...}, geom:{p:[encoded]} }, ... ]}
- geometry:
    geom.p[0] is an encoded polyline POINT (decode -> (lat, lon))
- cluster nodes:
    desc.cluster == true (mixed with non-cluster in same tile)
- qkh sharding is CONFIRMED:
    qkh = reverse(last3(quadkey))  (aka last3_rev)
- deployment config for cluster layers is 401:
    discover outage layers by validated probing
- IMPORTANT ENV NOTE:
    stormcenter.evergy.com may not resolve in some environments; DO NOT use it for discovery.
    Use outagemap.evergy.com as primary.

Primary function:
fetch_evergy_outages(lat, lon, max_radius_km=50.0, max_zoom=12,
                     neighbor_depth=1, drill_neighbor_depth=1, debug=False)

Return:
{ "nearest": <outage|null>, "outages": [<outage>...] }

Outage fields:
id, cluster, customers_out, n_out, etr, etr_confidence, cause, comments,
crew_status, start_time, latitude, longitude, distance_km
"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import requests

logger = logging.getLogger(__name__)

# -----------------------------
# Exceptions
# -----------------------------


class EvergyKubraError(RuntimeError):
    """Controlled error the router can catch."""
    pass


# -----------------------------
# HTTP config
# -----------------------------

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# -----------------------------
# Evergy search radius policy
# -----------------------------
# Default behavior (NOC): search ~10 miles first, then fall back to ~25 miles if none found.
PRIMARY_RADIUS_KM = 16.0934   # 10 miles
FALLBACK_RADIUS_KM = 40.2335  # 25 miles

# -----------------------------
# Simple in-process cache
# -----------------------------
# Evergy/Kubra discovery can be expensive (many probe requests), and Kubra may 404 on empty tiles.
# Cache the discovered IDs, cluster template, and tile scheme to keep requests fast.
CACHE_TTL_SECONDS = 600  # 10 minutes
_EVERGY_CACHE = {
    "ids": None,           # (stormcenter_id, view_id)
    "cluster_template": None,
    "scheme": None,        # TileScheme
    "ts": 0.0,
}


def _cache_get(key: str):
    now = time.time()
    if now - float(_EVERGY_CACHE.get("ts", 0.0)) > CACHE_TTL_SECONDS:
        return None
    return _EVERGY_CACHE.get(key)


def _cache_set(**kwargs):
    _EVERGY_CACHE.update(kwargs)
    _EVERGY_CACHE["ts"] = time.time()


REQUEST_TIMEOUT = (3.0, 5.0)   # (connect, read) seconds per HTTP call
TOTAL_BUDGET_SECONDS = 12.0    # overall provider budget to fit router 15s


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": UA,
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return s


def _get_text(s: requests.Session, url: str, debug: bool = False) -> str:
    if debug:
        print(f"GET {url}")
    r = s.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


def _get_json(s: requests.Session, url: str, debug: bool = False) -> Any:
    if debug:
        print(f"GET {url}")
    r = s.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Quadkey + geo utils
# -----------------------------


def _clip(n: float, lo: float, hi: float) -> float:
    return min(max(n, lo), hi)


def _map_size(zoom: int) -> int:
    return 256 << zoom


def _latlon_to_pixel_xy(lat: float, lon: float, zoom: int) -> Tuple[int, int]:
    lat = _clip(lat, -85.05112878, 85.05112878)
    lon = _clip(lon, -180.0, 180.0)

    x = (lon + 180.0) / 360.0
    sin_lat = math.sin(lat * math.pi / 180.0)
    y = 0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)

    size = _map_size(zoom)
    px = int(_clip(x * size + 0.5, 0, size - 1))
    py = int(_clip(y * size + 0.5, 0, size - 1))
    return px, py


def _pixel_xy_to_tile_xy(px: int, py: int) -> Tuple[int, int]:
    return px // 256, py // 256


def _tile_xy_to_quadkey(tx: int, ty: int, zoom: int) -> str:
    q = []
    for i in range(zoom, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if tx & mask:
            digit += 1
        if ty & mask:
            digit += 2
        q.append(str(digit))
    return "".join(q)


def latlon_to_quadkey(lat: float, lon: float, zoom: int) -> str:
    px, py = _latlon_to_pixel_xy(lat, lon, zoom)
    tx, ty = _pixel_xy_to_tile_xy(px, py)
    return _tile_xy_to_quadkey(tx, ty, zoom)


def quadkey_to_tile_xy(qk: str) -> Tuple[int, int]:
    tx = 0
    ty = 0
    zoom = len(qk)
    for i, ch in enumerate(qk):
        bit = zoom - i - 1
        mask = 1 << bit
        d = int(ch)
        if d & 1:
            tx |= mask
        if d & 2:
            ty |= mask
    return tx, ty


def quadkey_children(qk: str) -> List[str]:
    return [qk + d for d in ("0", "1", "2", "3")]


def quadkey_neighbors(qk: str, depth: int = 1) -> Set[str]:
    if depth <= 0:
        return {qk}
    z = len(qk)
    tx, ty = quadkey_to_tile_xy(qk)
    out: Set[str] = set()
    for dx in range(-depth, depth + 1):
        for dy in range(-depth, depth + 1):
            out.add(_tile_xy_to_quadkey(tx + dx, ty + dy, z))
    return out


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# -----------------------------
# Polyline decoding (Evergy geom.p)
# -----------------------------


def decode_polyline(encoded: str, precision: int = 5) -> List[Tuple[float, float]]:
    """
    Decodes a Google-encoded polyline string into (lat, lon) pairs.
    Evergy geom.p[0] is typically an encoded POINT -> list length 1.
    """
    if not encoded:
        return []
    idx = 0
    lat = 0
    lon = 0
    coords: List[Tuple[float, float]] = []
    factor = 10**precision

    while idx < len(encoded):
        # latitude
        shift = 0
        result = 0
        while True:
            if idx >= len(encoded):
                return coords
            b = ord(encoded[idx]) - 63
            idx += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        # longitude
        shift = 0
        result = 0
        while True:
            if idx >= len(encoded):
                return coords
            b = ord(encoded[idx]) - 63
            idx += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlon = ~(result >> 1) if (result & 1) else (result >> 1)
        lon += dlon

        coords.append((lat / factor, lon / factor))

    return coords


# -----------------------------
# Generic helpers
# -----------------------------


def _safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    return v if isinstance(v, str) else str(v)


def normalize_iso8601(ts: Optional[str]) -> Optional[str]:
    """Normalize timestamps to ISO-8601 with seconds (UTC, Z suffix).

    Handles values like:
      - 2025-12-19T19:29Z
      - 2025-12-19T19:29:47Z
      - ETR-EXP / ETR-NULL (passed through)
    """
    if not ts:
        return None
    ts = str(ts)
    if ts.startswith("ETR-"):
        return ts
    try:
        raw = ts[:-1] if ts.endswith("Z") else ts
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ts


def _localize_maybe(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        for k in ("EN-US", "en-US", "en", "EN"):
            vv = v.get(k)
            if isinstance(vv, str):
                return vv
        for vv in v.values():
            if isinstance(vv, str):
                return vv
    return _safe_str(v)


def _nested_get(d: Dict[str, Any], path: Sequence[str]) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


# -----------------------------
# Discovery: StormCenter/View IDs
# -----------------------------

# NOTE: Only use outagemap.evergy.com and friendly variants; do NOT use stormcenter.evergy.com.
DEFAULT_HOST_CANDIDATES = [
    "https://outagemap.evergy.com",
]

# Known-good Evergy StormCenter IDs (fallback when HTML/JS discovery fails)
DEFAULT_STORMCENTER_ID = "b1493825-4ee3-4706-a986-99a763a733db"
DEFAULT_VIEW_ID = "c1062d22-2919-487c-9000-e21b72b62278"

_STORMCENTER_VIEW_RE = re.compile(
    r"/stormcenter/api/v1/stormcenters/([0-9a-fA-F-]{36})/views/([0-9a-fA-F-]{36})/currentState",
    re.IGNORECASE,
)


def discover_stormcenter_and_view(debug: bool = False) -> Tuple[str, str]:
    """
    Scrape Evergy map pages/JS bundles to find currentState endpoint pattern.

    If this breaks due to UI changes, caller can pass stormcenter_id/view_id overrides
    to fetch_evergy_outages().
    """
    last_err: Optional[Exception] = None

    # IMPORTANT: Use the same session helper as the rest of the module.
    # This guarantees `s` is defined and has headers + timeouts.
    s = _session()
    try:
        for host in DEFAULT_HOST_CANDIDATES:
            for path in ("/", "/outages", "/outage-map"):
                url = host.rstrip("/") + path
                try:
                    html = _get_text(s, url, debug=debug)
                except Exception as e:
                    last_err = e
                    continue

                m = _STORMCENTER_VIEW_RE.search(html)
                if m:
                    if debug:
                        print(f"DISCOVERED IDs via {url}")
                    return m.group(1), m.group(2)

                script_srcs = re.findall(
                    r'<script[^>]+src="([^"]+)"', html, flags=re.IGNORECASE
                )
                bundle_like = [
                    src
                    for src in script_srcs
                    if any(k in src.lower() for k in ("main", "bundle", "app", "runtime"))
                ]
                for src in (bundle_like + script_srcs)[:6]:
                    js_url = src
                    if js_url.startswith("/"):
                        js_url = host.rstrip("/") + js_url
                    elif js_url.startswith("//"):
                        js_url = "https:" + js_url
                    elif not js_url.startswith("http"):
                        js_url = host.rstrip("/") + "/" + js_url.lstrip("/")

                    try:
                        js = _get_text(s, js_url, debug=debug)
                    except Exception as e:
                        last_err = e
                        continue

                    m2 = _STORMCENTER_VIEW_RE.search(js)
                    if m2:
                        if debug:
                            print(f"DISCOVERED IDs via JS {js_url}")
                        return m2.group(1), m2.group(2)

        raise EvergyKubraError(
            f"Unable to discover stormcenter/view IDs. Last error: {last_err}"
        )
    finally:
        try:
            s.close()
        except Exception:
            pass


def fetch_current_state(
    s: requests.Session, stormcenter_id: str, view_id: str, debug: bool = False
) -> Dict[str, Any]:
    url = (
        f"https://kubra.io/stormcenter/api/v1/stormcenters/{stormcenter_id}"
        f"/views/{view_id}/currentState?preview=false"
    )
    data = _get_json(s, url, debug=debug)
    if not isinstance(data, dict):
        raise EvergyKubraError("currentState response was not a JSON object")
    return data


def extract_cluster_template(current_state: Dict[str, Any]) -> str:
    """
    Evergy confirmed location: current_state['data']['cluster_interval_generation_data']
    """
    template = _nested_get(
        current_state, ["data", "cluster_interval_generation_data"]
    ) or current_state.get("cluster_interval_generation_data")
    if not template or not isinstance(template, str):
        blob = json.dumps(current_state)
        m = re.search(r"cluster-data/[^\"']+", blob)
        if m:
            template = m.group(0)
        else:
            raise EvergyKubraError(
                "Unable to extract cluster_interval_generation_data template from currentState"
            )
    return template.strip("/")


# -----------------------------
# qkh + URL building
# -----------------------------


def _qkh_last3_rev(qk: str) -> str:
    last3 = qk[-3:] if len(qk) >= 3 else qk
    return last3[::-1]


def build_tile_url(cluster_template: str, qk: str, layer: str, layout: str) -> str:
    """
    Evergy confirmed: {qkh} == reverse(last3(quadkey))  (last3_rev).
    Support both {qkh} and (qkh) placeholders for portability.
    """
    shard = _qkh_last3_rev(qk)
    base = (
        cluster_template.replace("{qkh}", shard)
        .replace("(qkh)", shard)
        .strip("/")
    )
    if layout == "flat":
        return f"https://kubra.io/{base}/public/{layer}/{qk}.json"
    if layout == "split2":
        p2 = qk[:2] if len(qk) >= 2 else qk
        return f"https://kubra.io/{base}/public/{layer}/{p2}/{qk}.json"
    raise ValueError(f"Unknown layout: {layout}")


@dataclass(frozen=True)
class TileScheme:
    layer: str
    layout: str
    zoom: int


def _looks_like_evergy_tile(j: Any) -> bool:
    return isinstance(j, dict) and isinstance(j.get("file_data"), list)


def discover_tile_scheme(
    s: requests.Session,
    cluster_template: str,
    qks_to_try: Sequence[str],
    zoom: int,
    debug: bool = False,
) -> TileScheme:
    """
    Evergy deployment config for outage layers is protected (401),
    so auto-discover by validated probing.

    Discover:
    - which layer(s) exist (cluster-1..cluster-N)
    - which layout is used (flat vs split2)

    IMPORTANT:
    Some Kubra deployments only materialize tiles where outages exist.
    If the caller's center tile is empty, it may 404 even though the map has outages
    nearby. Therefore we probe a small set of tiles (center + neighbors and/or
    known territory points) until we find ANY 200 tile with file_data.
    """
    layers = [f"cluster-{i}" for i in range(1, 21)]
    layouts = ["flat", "split2"]
    last_status: Optional[int] = None

    for qk in qks_to_try:
        for layer in layers:
            for layout in layouts:
                url = build_tile_url(cluster_template, qk, layer, layout)
                if debug:
                    print(
                        f"PROBE layer={layer} zoom={zoom} qkh=last3_rev "
                        f"layout={layout} qk={qk}"
                    )
                    print(f"   {url}")
                try:
                    r = s.get(url, timeout=REQUEST_TIMEOUT)
                    last_status = r.status_code
                    if r.status_code != 200:
                        continue
                    j = r.json()
                    if _looks_like_evergy_tile(j):
                        # Emit a single-line marker via logging so it reliably shows in CloudWatch.
                        logger.warning(
                            "EVERGY_SCHEME_SUCCESS layer=%s zoom=%s qkh=last3_rev layout=%s url=%s qk=%s",
                            layer,
                            zoom,
                            layout,
                            url,
                            qk,
                        )
                        if debug:
                            print(f"PROBE SUCCESS: {url}")
                        return TileScheme(layer=layer, layout=layout, zoom=zoom)
                except Exception:
                    continue

    raise EvergyKubraError(
        f"Unable to discover a working tile scheme (last HTTP status: {last_status})"
    )


def _decode_geom_point(geom: Any) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(geom, dict):
        return None, None
    p = geom.get("p")
    if not (isinstance(p, list) and p and isinstance(p[0], str)):
        return None, None
    coords = decode_polyline(p[0], precision=5)
    if not coords:
        return None, None
    lat, lon = coords[0]
    return float(lat), float(lon)


def _normalize_record(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    desc = rec.get("desc")
    if not isinstance(desc, dict):
        return None

    lat, lon = _decode_geom_point(rec.get("geom"))
    if lat is None or lon is None:
        return None

    is_cluster = bool(desc.get("cluster")) is True

    customers_out: Optional[int] = None
    cust_a = desc.get("cust_a")
    if isinstance(cust_a, dict):
        val = cust_a.get("val")
        if isinstance(val, (int, float)):
            customers_out = int(val)

    n_out: Optional[int] = None
    v = desc.get("n_out")
    if isinstance(v, (int, float)):
        n_out = int(v)

    inc_id = desc.get("inc_id")

    start_time = normalize_iso8601(_safe_str(desc.get("start_time")))

    # Evergy: incident IDs are not always assigned/published.
    # Policy for NOC: Only show a true Evergy/Kubra incident ID when provided; otherwise mark Unknown.
    outage_id = str(inc_id) if inc_id else "Unknown"

    return {
        "id": outage_id,
        "cluster": bool(is_cluster),
        "customers_out": customers_out,
        "n_out": n_out,
        "etr": normalize_iso8601(_safe_str(desc.get("etr"))),  # ISO string OR "ETR-EXP"
        "etr_confidence": _safe_str(desc.get("etr_confidence")),
        "cause": _localize_maybe(desc.get("cause")),
        "comments": _localize_maybe(desc.get("comments")),
        "crew_status": _localize_maybe(desc.get("crew_status")),
        "start_time": _safe_str(start_time),
        "latitude": lat,
        "longitude": lon,
        # distance_km added later
    }


def fetch_tile_records(
    s: requests.Session,
    cluster_template: str,
    scheme: TileScheme,
    qk: str,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    url = build_tile_url(cluster_template, qk, scheme.layer, scheme.layout)
    if debug:
        print(f"FETCH {url}")
    try:
        r = s.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return []
        j = r.json()
        if not _looks_like_evergy_tile(j):
            return []
    except Exception:
        return []

    recs = j.get("file_data", [])
    if debug:
        print(f"   records={len(recs)}")
    out: List[Dict[str, Any]] = []
    for rec in recs:
        if isinstance(rec, dict):
            o = _normalize_record(rec)
            if o:
                out.append(o)
    return out


# -----------------------------
# Main entrypoint
# -----------------------------


def fetch_evergy_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 50.0,
    max_zoom: int = 12,
    neighbor_depth: int = 1,
    drill_neighbor_depth: int = 1,
    debug: bool = False,
    # Optional overrides (recommended for reliability in NOC tooling)
    stormcenter_id: Optional[str] = None,
    view_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch outages near (lat, lon) from Evergy Kubra StormCenter.

    Fail behavior:
      - Raises EvergyKubraError for controlled failures (router can catch).
      - Returns {"nearest": None, "outages": []} ONLY if everything succeeds but nothing is found.
    """
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise EvergyKubraError("Invalid lat/lon")

    # Single session per invocation (reduces handshake overhead) + hard runtime budget.
    s = _session()
    t0 = time.time()

    def _time_left() -> float:
        return TOTAL_BUDGET_SECONDS - (time.time() - t0)

    try:
        # 1) Discover IDs unless provided (Option A), with a hard fallback (Option B)
        cached_ids = _cache_get("ids")
        if (not stormcenter_id or not view_id) and cached_ids:
            stormcenter_id, view_id = cached_ids
        if not stormcenter_id or not view_id:
            try:
                stormcenter_id, view_id = discover_stormcenter_and_view(debug=debug)
            except EvergyKubraError as e:
                # Operational fallback: use known IDs if discovery fails in this environment
                if debug:
                    print(f"DISCOVERY FAILED: {e}")
                    print("FALLBACK to DEFAULT_STORMCENTER_ID/DEFAULT_VIEW_ID")
                stormcenter_id, view_id = DEFAULT_STORMCENTER_ID, DEFAULT_VIEW_ID

        _cache_set(ids=(stormcenter_id, view_id))
        if debug:
            print(f"stormcenter={stormcenter_id} view={view_id}")

        # 2) currentState -> cluster template (instance + generation included)
        cs = fetch_current_state(s, stormcenter_id, view_id, debug=debug)
        cluster_template = extract_cluster_template(cs)
        _cache_set(cluster_template=cluster_template)
        if debug:
            print(f"cluster_template={cluster_template}")

        # 3) Select an entry zoom and discover tile scheme (layer + layout)
        zoom_candidates = [10, 11, 12, 13, 14]
        scheme: Optional[TileScheme] = None
        last_err: Optional[Exception] = None

        for z in zoom_candidates:
            if _time_left() <= 1.0:
                raise EvergyKubraError("Timeout after 12s")
            center_qk = latlon_to_quadkey(lat, lon, z)

            qks_to_try = sorted(list(quadkey_neighbors(center_qk, depth=max(1, neighbor_depth))))

            territory_points = [
                (38.9822, -94.6708),  # KC / Overland Park
                (39.0558, -95.6890),  # Topeka
                (37.6872, -97.3301),  # Wichita
            ]
            for tlat, tlon in territory_points:
                qks_to_try.append(latlon_to_quadkey(tlat, tlon, z))

            # De-dupe while preserving order
            seen: Set[str] = set()
            qks_to_try = [q for q in qks_to_try if not (q in seen or seen.add(q))]

            cached_scheme = _cache_get("scheme")
            if cached_scheme:
                scheme = cached_scheme
                break

            try:
                scheme = discover_tile_scheme(s, cluster_template, qks_to_try, z, debug=debug)
                _cache_set(scheme=scheme)
                break
            except Exception as e:
                last_err = e
                continue

        if not scheme:
            raise EvergyKubraError(f"Failed to discover tile scheme. Last error: {last_err}")

        entry_zoom = scheme.zoom
        center_qk = latlon_to_quadkey(lat, lon, entry_zoom)

        if debug:
            print(f"ENTRY zoom={entry_zoom} qk={center_qk}")
            print(f"USING layer={scheme.layer} layout={scheme.layout}")

        # 4) Fetch entry neighborhood
        visited_tiles: Set[str] = set()
        queue_tiles: List[str] = sorted(list(quadkey_neighbors(center_qk, depth=neighbor_depth)))

        all_outs: List[Dict[str, Any]] = []
        drill_queue: List[Tuple[float, float, int]] = []  # clusters to drill; not returned

        for qk in queue_tiles:
            if _time_left() <= 1.0:
                break
            if qk in visited_tiles:
                continue
            visited_tiles.add(qk)

            new_outs = fetch_tile_records(s, cluster_template, scheme, qk, debug=debug)
            for o in new_outs:
                if o.get("cluster") is True:
                    drill_queue.append((o["latitude"], o["longitude"], entry_zoom))
                else:
                    all_outs.append(o)

        # 5) Drill clusters down to max_zoom
        drill_ops = 0
        drill_cap = 120
        drilled_tile_keys: Set[Tuple[int, str]] = set()  # (zoom, quadkey) to avoid repeats

        while drill_queue:
            if _time_left() <= 1.0:
                if debug:
                    print("TIME BUDGET LOW; stopping drill.")
                break
            clat, clon, z = drill_queue.pop(0)
            if z >= max_zoom:
                continue
            if drill_ops >= drill_cap:
                if debug:
                    print("DRILL CAP reached; stopping.")
                break
            drill_ops += 1

            qk_cluster = latlon_to_quadkey(clat, clon, z)
            next_z = z + 1

            tiles_next: Set[str] = set()
            for child in quadkey_children(qk_cluster):
                tiles_next |= quadkey_neighbors(child, drill_neighbor_depth)

            if debug:
                print(f"DRILL cluster z={z} -> z={next_z} tiles={len(tiles_next)}")

            for tqk in tiles_next:
                key = (next_z, tqk)
                if key in drilled_tile_keys:
                    continue
                drilled_tile_keys.add(key)

                new_outs = fetch_tile_records(s, cluster_template, scheme, tqk, debug=debug)
                for o in new_outs:
                    if o.get("cluster") is True:
                        if next_z < max_zoom:
                            drill_queue.append((o["latitude"], o["longitude"], next_z))
                    else:
                        all_outs.append(o)

        # 6) Distance filter + dedupe
        primary_radius_km = min(PRIMARY_RADIUS_KM, float(max_radius_km))
        fallback_radius_km = min(FALLBACK_RADIUS_KM, float(max_radius_km))

        def _filter_and_dedup(radius_km: float) -> List[Dict[str, Any]]:
            dedup: Set[str] = set()
            final_outs: List[Dict[str, Any]] = []
            for o in all_outs:
                dist = haversine_km(lat, lon, o["latitude"], o["longitude"])
                o["distance_km"] = dist

                if dist > radius_km:
                    continue

                if o.get("cluster") is True:
                    dkey = f"cluster:{o['latitude']:.6f},{o['longitude']:.6f}:{o.get('n_out')}:{o.get('customers_out')}"
                else:
                    dkey = f"inc:{o.get('id')}:{o['latitude']:.6f},{o['longitude']:.6f}:{o.get('start_time')}"

                if dkey in dedup:
                    continue
                dedup.add(dkey)
                final_outs.append(o)
            return final_outs

        final_outs = _filter_and_dedup(primary_radius_km)

        if not final_outs and fallback_radius_km > primary_radius_km:
            if debug:
                print(f"RADIUS FALLBACK: {primary_radius_km:.1f} km -> {fallback_radius_km:.1f} km")
            final_outs = _filter_and_dedup(fallback_radius_km)

        final_outs.sort(key=lambda x: x.get("distance_km", float("inf")))

        non_clusters = [o for o in final_outs if o.get("cluster") is False]
        candidates = non_clusters if non_clusters else final_outs
        nearest = min(candidates, key=lambda x: x.get("distance_km", float("inf"))) if candidates else None

        return {"nearest": nearest, "outages": final_outs}

    finally:
        try:
            s.close()
        except Exception:
            pass


# -----------------------------
# Self-test
# -----------------------------


def _summarize(o: Optional[Dict[str, Any]]) -> str:
    if not o:
        return "None"
    return (
        f"id={o.get('id')} cluster={o.get('cluster')} "
        f"cust={o.get('customers_out')} n_out={o.get('n_out')} "
        f"etr={o.get('etr')} crew={o.get('crew_status')} "
        f"dist_km={o.get('distance_km', 0):.2f}"
    )


if __name__ == "__main__":
    # KC metro / Overland Park test point (Evergy territory)
    test_lat = 38.9822
    test_lon = -94.6708

    print("Testing Evergy outage fetch (debug on, max_zoom=12)...")
    try:
        res = fetch_evergy_outages(
            test_lat,
            test_lon,
            max_radius_km=50.0,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            debug=True,
            # Deterministic test IDs (you captured these):
            stormcenter_id="b1493825-4ee3-4706-a986-99a763a733db",
            view_id="c1062d22-2919-487c-9000-e21b72b62278",
        )
        outs = res.get("outages", [])
        print(f"\nTOTAL OUTAGES (filtered): {len(outs)}")
        print(f"NEAREST: {_summarize(res.get('nearest'))}")
        for i, o in enumerate(outs[:10]):
            print(f"  #{i+1}: {_summarize(o)}")
    except EvergyKubraError as e:
        print(f"EvergyKubraError: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
