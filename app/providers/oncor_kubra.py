# app/providers/oncor_kubra.py
"""
Oncor (Texas) — Kubra StormCenter provider module
=================================================

Provider-only module. No FastAPI/UI/weather.

Primary entrypoint:
    fetch_oncor_outages(
        lat, lon,
        max_radius_km=50.0,
        max_zoom=12,
        neighbor_depth=0,
        drill_neighbor_depth=1,
        debug=False,
        fast=False,
        stop_after=10,
        max_tile_fetches=None,
    )

Return:
    { "nearest": <outage|null>, "outages": [<outage>...] }

Outage fields (must match OG&E/PSO/Evergy):
    id, cluster, customers_out, n_out, etr, etr_confidence, cause, comments,
    crew_status, start_time, latitude, longitude, distance_km

Key behaviors:
- Auto-discover layer/qkh/layout/entry zoom using currentState + bounded probes.
- Decode geom.p (encoded polyline) to coordinates.
- Drill clusters down to max_zoom.
- Never return cluster rows in final outages list.
- Normalize localized dicts (prefer EN-US, ignore 'orig').
- Normalize timestamps to strict ISO-8601 UTC (seconds + Z).
- Performance hardening: cache state+scheme, cap drill, distance-gate cluster drilling.
- FAST mode (Option 1): nearest-first / early-stop collection to fit tight router timeouts.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests


# ----------------------------
# Controlled exception
# ----------------------------

class OncorKubraError(RuntimeError):
    """Controlled error for Oncor Kubra provider failures."""


# ----------------------------
# Known (confirmed) IDs
# ----------------------------

KUBRA_BASE = "https://kubra.io"
ONCOR_MAP_URL = "https://stormcenter.oncor.com/"

STORMCENTER_ID = "560abba3-7881-4741-b538-ca416b58ba1e"
VIEW_ID = "ca124b24-9a06-4b19-aeb3-1841a9c962e1"

CURRENT_STATE_URL = (
    f"{KUBRA_BASE}/stormcenter/api/v1/stormcenters/{STORMCENTER_ID}"
    f"/views/{VIEW_ID}/currentState?preview=false"
)


# ----------------------------
# Discovery knobs (bounded)
# ----------------------------

DISCOVERY_ZOOMS: Sequence[int] = (8, 9, 10, 11, 12, 13, 14)
QKH_STRATEGIES: Sequence[str] = ("last3_rev", "last3", "first3", "first3_rev", "last4_rev")
LAYOUTS: Sequence[str] = ("flat", "split2")

# Fast-path scheme guesses (do not remove the full discovery loop below).
#
# We have observed live Oncor tile URLs using:
#   - layer: cluster-3
#   - qkh: the *reverse of the last 3 digits* of the quadkey ("last3_rev")
#   - layout: flat
#
# Example (from browser devtools):
#   .../cluster-data/312/.../public/cluster-3/0231123213.json
# Here: quadkey ends with "213" -> reversed "312".
QUICK_SCHEME_CANDIDATES: Sequence[Tuple[str, int, str, str]] = (
    ("cluster-3", 10, "last3_rev", "flat"),
    ("cluster-3", 11, "last3_rev", "flat"),
    ("cluster-3", 12, "last3_rev", "flat"),
)

# Bounded set; Oncor observed as cluster-3 but we do not hardcode.
LAYER_CANDIDATES: Sequence[str] = ("cluster-1", "cluster-2", "cluster-3", "cluster-4", "cluster-5")

TEXAS_PROBE_POINTS: Sequence[Tuple[str, float, float]] = (
    ("Dallas", 32.7767, -96.7970),
    ("Fort Worth", 32.7555, -97.3308),
    ("Arlington", 32.7357, -97.1081),
    ("Midland", 31.9973, -102.0779),
    ("Tyler", 32.3513, -95.3011),
)


# ----------------------------
# Performance controls
# ----------------------------

STATE_TTL_SEC = 300       # 5 minutes
SCHEME_TTL_SEC = 900      # 15 minutes

# Default hard caps per request (can be overridden via max_tile_fetches in fast mode)
MAX_TILE_FETCHES = 90
MAX_LEAF_OUTAGES = 200

# Skip drilling clusters if their decoded centroid is outside radius + buffer
CLUSTER_DRILL_BUFFER_KM = 15.0


# ----------------------------
# In-process caches (per worker)
# ----------------------------

_STATE_CACHE: Optional["OncorState"] = None
_STATE_CACHE_TS: float = 0.0

_SCHEME_CACHE: Optional["Scheme"] = None
_SCHEME_CACHE_TS: float = 0.0


# ----------------------------
# HTTP helpers
# ----------------------------

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (OncorKubra provider module)",
            "Accept": "application/json, text/plain, */*",
            "Origin": ONCOR_MAP_URL.rstrip("/"),
            "Referer": ONCOR_MAP_URL,
        }
    )
    return s


def _http_get_json(
    session: requests.Session,
    url: str,
    *,
    timeout: float = 12.0,
    debug: bool = False,
) -> Any:
    try:
        resp = session.get(url, timeout=timeout)
    except requests.RequestException as e:
        raise OncorKubraError(f"Request failed: {url}: {e}") from e

    if resp.status_code != 200:
        if debug:
            print(f"PROBE FAIL status={resp.status_code} url={url}")
        return None

    if debug:
        print(f"PROBE SUCCESS {url}")

    try:
        return resp.json()
    except Exception as e:
        raise OncorKubraError(f"Non-JSON response from {url}: {e}") from e


# ----------------------------
# General utilities
# ----------------------------

def _iter_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_dicts(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _iter_dicts(it)


def _find_first_key(obj: Any, key: str) -> Optional[Any]:
    for d in _iter_dicts(obj):
        if key in d:
            return d.get(key)
    return None


def _pick_localized(value: Any) -> Any:
    """Prefer EN-US if localized dict; ignore 'orig'."""
    if isinstance(value, dict) and value:
        v = value.get("EN-US")
        if isinstance(v, str) and v.strip():
            return v
        for k, vv in value.items():
            if k == "orig":
                continue
            if isinstance(vv, str) and vv.strip():
                return vv
        return None
    return value


def _val_field(x: Any) -> Any:
    """Many Kubra desc fields are { 'val': ... }."""
    if isinstance(x, dict) and "val" in x:
        return x.get("val")
    return x


def _iso8601_utc(ts: Any) -> Optional[str]:
    """Strict ISO-8601 UTC (seconds + Z). Reject Kubra placeholders (ETR-*)."""
    if ts is None:
        return None

    if isinstance(ts, str):
        s = ts.strip()
        if not s or s.startswith("ETR-"):
            return None
        try:
            if s.endswith("Z"):
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc).replace(microsecond=0)
            return dt.isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    if isinstance(ts, (int, float)):
        v = float(ts)
        sec = v / 1000.0 if v > 1e12 else v
        try:
            dt = datetime.fromtimestamp(sec, tz=timezone.utc).replace(microsecond=0)
            return dt.isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    return None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# ----------------------------
# Quadkey math
# ----------------------------

def _latlon_to_tile_xy(lat: float, lon: float, zoom: int) -> Tuple[int, int]:
    lat = max(min(lat, 85.05112878), -85.05112878)
    lon = ((lon + 180.0) % 360.0) - 180.0

    x = (lon + 180.0) / 360.0
    sin_lat = math.sin(math.radians(lat))
    y = 0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)

    n = 1 << zoom
    tx = int(min(max(x * n, 0), n - 1))
    ty = int(min(max(y * n, 0), n - 1))
    return tx, ty


def _tile_xy_to_quadkey(tx: int, ty: int, zoom: int) -> str:
    out = []
    for i in range(zoom, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if tx & mask:
            digit += 1
        if ty & mask:
            digit += 2
        out.append(str(digit))
    return "".join(out)


def _quadkey_to_tile_xy(qk: str) -> Tuple[int, int, int]:
    tx = 0
    ty = 0
    z = len(qk)
    for i, c in enumerate(qk):
        mask = 1 << (z - i - 1)
        d = ord(c) - ord("0")
        if d & 1:
            tx |= mask
        if d & 2:
            ty |= mask
    return tx, ty, z


def _neighbors_tile_xy(tx: int, ty: int, zoom: int, depth: int) -> List[Tuple[int, int]]:
    if depth <= 0:
        return [(tx, ty)]
    n = 1 << zoom
    out: List[Tuple[int, int]] = []
    for dy in range(-depth, depth + 1):
        for dx in range(-depth, depth + 1):
            nx, ny = tx + dx, ty + dy
            if 0 <= nx < n and 0 <= ny < n:
                out.append((nx, ny))
    return out


def _children_quadkeys(qk: str) -> List[str]:
    return [qk + "0", qk + "1", qk + "2", qk + "3"]


# ----------------------------
# qkh + layout helpers
# ----------------------------

def _qkh_from_quadkey(qk: str, strategy: str) -> str:
    if not qk:
        return ""
    if strategy == "last3":
        return qk[-3:] if len(qk) >= 3 else qk
    if strategy == "last3_rev":
        s = qk[-3:] if len(qk) >= 3 else qk
        return s[::-1]
    if strategy == "first3":
        return qk[:3] if len(qk) >= 3 else qk
    if strategy == "first3_rev":
        s = qk[:3] if len(qk) >= 3 else qk
        return s[::-1]
    if strategy == "last4_rev":
        s = qk[-4:] if len(qk) >= 4 else qk
        return s[::-1]
    s = qk[-3:] if len(qk) >= 3 else qk
    return s[::-1]


def _layout_path(qk: str, layout: str) -> str:
    if layout == "flat":
        return f"{qk}.json"
    if layout == "split2":
        prefix = qk[:2] if len(qk) >= 2 else qk
        return f"{prefix}/{qk}.json"
    return f"{qk}.json"


# ----------------------------
# Geometry token decoding
# ----------------------------

def _decode_encoded_polyline_point(token: str) -> Optional[Tuple[float, float]]:
    """
    Decode Google/Mapbox encoded polyline string and return the first (lat, lon) point.
    Kubra uses this for geom.p points.
    """
    if not token or not isinstance(token, str):
        return None

    try:
        index = 0
        lat = 0
        lon = 0
        length = len(token)
        coords: List[Tuple[float, float]] = []

        while index < length:
            # lat delta
            shift = 0
            result = 0
            while True:
                if index >= length:
                    return None
                b = ord(token[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            dlat = ~(result >> 1) if (result & 1) else (result >> 1)
            lat += dlat

            # lon delta
            shift = 0
            result = 0
            while True:
                if index >= length:
                    return None
                b = ord(token[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            dlon = ~(result >> 1) if (result & 1) else (result >> 1)
            lon += dlon

            coords.append((lat / 1e5, lon / 1e5))

        if not coords:
            return None

        la, lo = coords[0]
        if not (-90.0 <= la <= 90.0 and -180.0 <= lo <= 180.0):
            return None
        return coords[0]

    except Exception:
        return None


def _resolve_coords_from_geom_tokens(row: Dict[str, Any]) -> bool:
    tokens: List[str] = row.get("_geom_tokens") or []
    if not tokens:
        return False
    pt = _decode_encoded_polyline_point(tokens[0])
    if not pt:
        return False
    row["latitude"], row["longitude"] = pt
    return True


# ----------------------------
# CurrentState extraction
# ----------------------------

@dataclass(frozen=True)
class OncorState:
    deployment_id: str
    generation_id: str
    cluster_prefix_template: str  # e.g. "cluster-data/{qkh}/<deploy>/<gen>"


def _extract_state(current_state: Any, debug: bool) -> OncorState:
    deployment_id = _find_first_key(current_state, "stormcenterDeploymentId")
    if not isinstance(deployment_id, str) or not deployment_id.strip():
        raise OncorKubraError("currentState missing stormcenterDeploymentId.")

    data = current_state.get("data") if isinstance(current_state, dict) else None
    if not isinstance(data, dict):
        raise OncorKubraError("currentState missing 'data' object.")

    cluster_template = data.get("cluster_interval_generation_data")
    if not isinstance(cluster_template, str) or "cluster-data/" not in cluster_template:
        raise OncorKubraError("currentState missing cluster_interval_generation_data template.")

    generation_id = cluster_template.split("/")[-1]
    if not generation_id or generation_id.count("-") < 4:
        raise OncorKubraError("Unable to parse generation id from cluster_interval_generation_data.")

    if debug:
        print(f"stormcenterDeploymentId={deployment_id}")
        print(f"generation_id={generation_id}")
        print(f"cluster_prefix_template={cluster_template}")

    return OncorState(
        deployment_id=deployment_id,
        generation_id=generation_id,
        cluster_prefix_template=cluster_template,
    )


# ----------------------------
# Tile fetch + parsing
# ----------------------------

def _cluster_tile_url(state: OncorState, *, qkh: str, layer: str, qk: str, layout: str) -> str:
    prefix = state.cluster_prefix_template.replace("{qkh}", qkh).strip("/")
    if not prefix.startswith("http"):
        prefix = f"{KUBRA_BASE}/{prefix}"
    return f"{prefix}/public/{layer}/{_layout_path(qk, layout)}"


def _extract_file_data(tile_payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(tile_payload, dict):
        return []
    fd = tile_payload.get("file_data")
    if isinstance(fd, list):
        return [x for x in fd if isinstance(x, dict)]
    return []


def _clean_str(x: Any) -> Optional[str]:
    if not isinstance(x, str):
        return None
    s = x.strip()
    return s if s else None


def _normalize_outage_row(row: Dict[str, Any]) -> Dict[str, Any]:
    desc = row.get("desc") if isinstance(row, dict) else None
    if not isinstance(desc, dict):
        desc = {}

    cluster = bool(desc.get("cluster")) if "cluster" in desc else False

    inc_id = _val_field(desc.get("inc_id"))
    outage_id = inc_id.strip() if isinstance(inc_id, str) and inc_id.strip() else "Unknown"

    cust_a = _val_field(desc.get("cust_a"))
    customers_out = None
    if cust_a is not None:
        try:
            customers_out = int(cust_a)
        except Exception:
            customers_out = None

    n_out = _val_field(desc.get("n_out"))
    n_out_i = None
    if n_out is not None:
        try:
            n_out_i = int(n_out)
        except Exception:
            n_out_i = None

    etr_raw = _pick_localized(_val_field(desc.get("etr")))
    etr = _iso8601_utc(etr_raw)

    etr_conf_raw = _pick_localized(_val_field(desc.get("etr_confidence")))
    etr_confidence = _clean_str(etr_conf_raw) if isinstance(etr_conf_raw, str) and not etr_conf_raw.startswith("ETR-") else None

    cause_raw = _pick_localized(_val_field(desc.get("cause")))
    cause = _clean_str(cause_raw)

    comments_raw = _pick_localized(_val_field(desc.get("comments")))
    comments = _clean_str(comments_raw)

    crew_raw = _pick_localized(_val_field(desc.get("crew_status")))
    crew_status = _clean_str(crew_raw)

    start_raw = _pick_localized(_val_field(desc.get("start_time")))
    start_time = _iso8601_utc(start_raw)

    geom = row.get("geom") if isinstance(row, dict) else None
    point_tokens: List[str] = []
    if isinstance(geom, dict):
        p = geom.get("p")
        if isinstance(p, list):
            point_tokens = [t for t in p if isinstance(t, str)]

    normalized: Dict[str, Any] = {
        "id": outage_id,
        "cluster": cluster,
        "customers_out": customers_out,
        "n_out": n_out_i,
        "etr": etr,
        "etr_confidence": etr_confidence,
        "cause": cause,
        "comments": comments,
        "crew_status": crew_status,
        "start_time": start_time,
        "latitude": None,
        "longitude": None,
        "distance_km": None,
        "_geom_tokens": point_tokens,  # internal only
    }

    _resolve_coords_from_geom_tokens(normalized)
    return normalized


# ----------------------------
# Scheme discovery
# ----------------------------

@dataclass(frozen=True)
class Scheme:
    layer: str
    zoom: int
    qkh_strategy: str
    layout: str


def _probe_scheme(
    session: requests.Session,
    state: OncorState,
    *,
    layer: str,
    zoom: int,
    qkh_strategy: str,
    layout: str,
    lat: float,
    lon: float,
    debug: bool,
) -> int:
    tx, ty = _latlon_to_tile_xy(lat, lon, zoom)
    qk = _tile_xy_to_quadkey(tx, ty, zoom)
    qkh = _qkh_from_quadkey(qk, qkh_strategy)
    url = _cluster_tile_url(state, qkh=qkh, layer=layer, qk=qk, layout=layout)

    if debug:
        print(f"PROBE layer={layer} zoom={zoom} qkh={qkh_strategy} layout={layout}")

    # Keep probes tight; scheme discovery must not dominate the caller's time budget.
    payload = _http_get_json(session, url, timeout=4.0, debug=debug)
    rows = _extract_file_data(payload)
    return len(rows)


def _discover_scheme(session: requests.Session, state: OncorState, debug: bool) -> Scheme:
    # Fast path: try the most likely candidates first.
    for layer, zoom, qkh_strategy, layout in QUICK_SCHEME_CANDIDATES:
        for _, plat, plon in TEXAS_PROBE_POINTS[:1]:  # Dallas first
            nrows = _probe_scheme(
                session,
                state,
                layer=layer,
                zoom=zoom,
                qkh_strategy=qkh_strategy,
                layout=layout,
                lat=plat,
                lon=plon,
                debug=debug,
            )
            if nrows > 0:
                if debug:
                    print(
                        "discovered "
                        f"layer_name={layer} entry_zoom={zoom} "
                        f"cluster_data_path={state.cluster_prefix_template} "
                        f"qkh/layout={qkh_strategy}/{layout} (quick)"
                    )
                return Scheme(layer=layer, zoom=zoom, qkh_strategy=qkh_strategy, layout=layout)

    for layer in LAYER_CANDIDATES:
        for zoom in DISCOVERY_ZOOMS:
            for qkh_strategy in QKH_STRATEGIES:
                for layout in LAYOUTS:
                    for _, plat, plon in TEXAS_PROBE_POINTS:
                        nrows = _probe_scheme(
                            session,
                            state,
                            layer=layer,
                            zoom=zoom,
                            qkh_strategy=qkh_strategy,
                            layout=layout,
                            lat=plat,
                            lon=plon,
                            debug=debug,
                        )
                        if nrows > 0:
                            if debug:
                                print(
                                    "discovered "
                                    f"layer_name={layer} entry_zoom={zoom} "
                                    f"cluster_data_path={state.cluster_prefix_template} "
                                    f"qkh/layout={qkh_strategy}/{layout}"
                                )
                            return Scheme(layer=layer, zoom=zoom, qkh_strategy=qkh_strategy, layout=layout)

    raise OncorKubraError("Unable to auto-discover a working cluster tile scheme for Oncor.")


# ----------------------------
# Collection + drill
# ----------------------------

def _fetch_tile_rows(
    session: requests.Session,
    state: OncorState,
    scheme: Scheme,
    *,
    qk: str,
    debug: bool,
) -> List[Dict[str, Any]]:
    qkh = _qkh_from_quadkey(qk, scheme.qkh_strategy)
    url = _cluster_tile_url(state, qkh=qkh, layer=scheme.layer, qk=qk, layout=scheme.layout)

    if debug:
        print(f"FETCH layer={scheme.layer} zoom={len(qk)} qkh={scheme.qkh_strategy} layout={scheme.layout}")

    payload = _http_get_json(session, url, timeout=6.0, debug=debug)
    raw_rows = _extract_file_data(payload)
    return [_normalize_outage_row(r) for r in raw_rows]


def _collect_outages(
    session: requests.Session,
    state: OncorState,
    scheme: Scheme,
    *,
    origin_lat: float,
    origin_lon: float,
    max_radius_km: float,
    max_zoom: int,
    neighbor_depth: int,
    drill_neighbor_depth: int,
    debug: bool,
    fast: bool = False,
    stop_after: int = 10,
    max_tile_fetches: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Collect outages near origin. If fast=True, early-stop after stop_after leaf outages are found.
    """
    start_zoom = scheme.zoom
    tx, ty = _latlon_to_tile_xy(origin_lat, origin_lon, start_zoom)

    # IMPORTANT: Oncor frequently places leaf outages just across tile boundaries.
    # If we drop neighbor expansion in fast mode, the UI will "miss" obvious nearby dots.
    #
    # In fast mode, still honor neighbor_depth but clamp to 1 to keep work bounded.
    if fast:
        eff_neighbor_depth = min(1, max(0, int(neighbor_depth)))
    else:
        eff_neighbor_depth = max(0, int(neighbor_depth))

    start_tiles = _neighbors_tile_xy(tx, ty, start_zoom, eff_neighbor_depth)
    start_qks = [_tile_xy_to_quadkey(x, y, start_zoom) for x, y in start_tiles]

    visited: set = set()
    leaf_seen: set = set()
    out: List[Dict[str, Any]] = []

    tile_fetches = 0
    row_total = 0
    done_fast = False

    effective_tile_cap = (
        int(max_tile_fetches)
        if isinstance(max_tile_fetches, int) and max_tile_fetches > 0
        else MAX_TILE_FETCHES
    )
    effective_stop_after = max(1, int(stop_after)) if fast else MAX_LEAF_OUTAGES

    stack: List[str] = list(start_qks)

    while stack:
        if tile_fetches >= effective_tile_cap:
            if debug:
                print(f"STOP: max tile fetches reached ({effective_tile_cap})")
            break
        if len(out) >= effective_stop_after:
            if debug and fast:
                print(f"STOP (fast): collected {len(out)} leaf outages (stop_after={effective_stop_after})")
            break
        if len(out) >= MAX_LEAF_OUTAGES:
            if debug:
                print(f"STOP: max leaf outages reached ({MAX_LEAF_OUTAGES})")
            break

        qk = stack.pop()
        z = len(qk)
        key = (scheme.layer, z, qk)
        if key in visited:
            continue
        visited.add(key)

        tile_fetches += 1
        rows = _fetch_tile_rows(session, state, scheme, qk=qk, debug=debug)
        if not rows:
            continue

        row_total += len(rows)

        clusters = [r for r in rows if r.get("cluster")]
        leaves = [r for r in rows if not r.get("cluster")]

        # Drill clusters (bounded by max_zoom), but distance-gate using decoded centroid if available.
        if clusters and z < max_zoom:
            next_z = z + 1

            drill_this_tile = True
            for c in clusters:
                clat, clon = c.get("latitude"), c.get("longitude")
                if clat is None or clon is None:
                    continue
                dist = _haversine_km(origin_lat, origin_lon, float(clat), float(clon))
                if dist > (max_radius_km + CLUSTER_DRILL_BUFFER_KM):
                    drill_this_tile = False
                else:
                    drill_this_tile = True
                    break

            if drill_this_tile:
                if debug:
                    print(f"DRILL cluster z={z} -> z={next_z} (clusters={len(clusters)})")

                # Same rationale as eff_neighbor_depth: we must still honor drill neighbor expansion
                # or we can miss leaf outages that render near the cluster boundary.
                if fast:
                    eff_drill_depth = min(1, max(0, int(drill_neighbor_depth)))
                else:
                    eff_drill_depth = max(0, int(drill_neighbor_depth))
                for child in _children_quadkeys(qk):
                    cx, cy, _ = _quadkey_to_tile_xy(child)
                    neigh = _neighbors_tile_xy(cx, cy, next_z, eff_drill_depth)
                    for nx, ny in neigh:
                        stack.append(_tile_xy_to_quadkey(nx, ny, next_z))
            else:
                if debug:
                    print(f"SKIP DRILL (cluster centroid outside radius+buffer) z={z} clusters={len(clusters)}")

        # Add leaves only (never return cluster rows)
        for r in leaves:
            lat = r.get("latitude")
            lon = r.get("longitude")
            if lat is None or lon is None:
                continue

            dist = _haversine_km(origin_lat, origin_lon, float(lat), float(lon))
            if dist > max_radius_km:
                continue

            r["distance_km"] = dist

            # Oncor inc_id often null; dedupe by coordinates.
            dedupe_key = (round(float(lat), 6), round(float(lon), 6))
            if dedupe_key in leaf_seen:
                continue
            leaf_seen.add(dedupe_key)

            r.pop("_geom_tokens", None)
            out.append(r)

            if fast and len(out) >= effective_stop_after:
                done_fast = True
                if debug:
                    print(f"STOP (fast): stop_after reached inside tile loop ({effective_stop_after})")
                break

        if done_fast:
            break

    if debug:
        print(f"tile fetch counts={tile_fetches} total_rows={row_total} leaf_outages={len(out)}")

    return out


# ----------------------------
# Public entrypoint
# ----------------------------

def fetch_oncor_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 50.0,
    max_zoom: int = 12,
    neighbor_depth: int = 0,
    drill_neighbor_depth: int = 1,
    debug: bool = False,
    fast: bool = False,
    stop_after: int = 10,
    max_tile_fetches: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Fetch Oncor outages near (lat, lon), drilling cluster tiles to max_zoom.

    fast=True enables nearest-first / early-stop collection to fit tight timeouts.
    """
    t0 = time.time()
    session = _make_session()
    now = time.time()

    global _STATE_CACHE, _STATE_CACHE_TS, _SCHEME_CACHE, _SCHEME_CACHE_TS

    # ---- currentState cache ----
    state_hit = True
    if _STATE_CACHE is None or (now - _STATE_CACHE_TS) > STATE_TTL_SEC:
        state_hit = False
        current_state = _http_get_json(session, CURRENT_STATE_URL, timeout=6.0, debug=debug)
        if not current_state:
            raise OncorKubraError("Failed to fetch Oncor currentState.")
        _STATE_CACHE = _extract_state(current_state, debug=debug)
        _STATE_CACHE_TS = now

    state = _STATE_CACHE

    # ---- scheme cache ----
    scheme_hit = True
    if _SCHEME_CACHE is None or (now - _SCHEME_CACHE_TS) > SCHEME_TTL_SEC:
        scheme_hit = False
        _SCHEME_CACHE = _discover_scheme(session, state, debug=debug)
        _SCHEME_CACHE_TS = now

    scheme = _SCHEME_CACHE

    if debug:
        print(f"CACHE state={'HIT' if state_hit else 'MISS'} scheme={'HIT' if scheme_hit else 'MISS'}")

    outages = _collect_outages(
        session,
        state,
        scheme,
        origin_lat=lat,
        origin_lon=lon,
        max_radius_km=max_radius_km,
        max_zoom=max_zoom,
        neighbor_depth=neighbor_depth,
        drill_neighbor_depth=drill_neighbor_depth,
        debug=debug,
        fast=fast,
        stop_after=stop_after,
        max_tile_fetches=max_tile_fetches,
    )

    nearest = min(outages, key=lambda o: o.get("distance_km", 1e18)) if outages else None

    if debug:
        print(f"TOTAL seconds={round(time.time() - t0, 3)} outages={len(outages)}")

    return {"nearest": nearest, "outages": outages}


# ----------------------------
# __main__ self-test
# ----------------------------

def _summarize_outage(o: Optional[Dict[str, Any]]) -> str:
    if not o:
        return "None"
    return (
        f"id={o.get('id')} cust_out={o.get('customers_out')} n_out={o.get('n_out')} "
        f"crew={o.get('crew_status')} etr={o.get('etr')} "
        f"dist_km={None if o.get('distance_km') is None else round(float(o['distance_km']), 2)} "
        f"lat={o.get('latitude')} lon={o.get('longitude')}"
    )


if __name__ == "__main__":
    test_lat, test_lon = 32.7767, -96.7970  # Dallas
    try:
        res = fetch_oncor_outages(
            test_lat,
            test_lon,
            max_radius_km=50.0,
            max_zoom=12,
            neighbor_depth=0,
            drill_neighbor_depth=1,
            debug=True,
            fast=True,
            stop_after=5,
            max_tile_fetches=40,
        )
        outs = res.get("outages", [])
        near = res.get("nearest")
        print("\n=== ONCOR SELF-TEST RESULTS ===")
        print(f"total outage count: {len(outs)}")
        print(f"nearest outage: {_summarize_outage(near)}")
    except OncorKubraError as e:
        print(f"\nOncorKubraError: {e}")
