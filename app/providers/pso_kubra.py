"""
PSO KUBRA Storm Center outage integration

Primary function:
  fetch_pso_outages(lat, lon, max_radius_km=50.0, max_zoom=12,
                    neighbor_depth=1, drill_neighbor_depth=1, debug=False)

Returns:
  { "nearest": <outage|null>, "outages": [<outage>...] }

Outage object fields (consistent with OG&E):
  id
  cluster (bool)
  customers_out
  n_out
  etr
  etr_confidence
  cause
  comments
  crew_status
  start_time
  latitude
  longitude
  distance_km
"""

import logging
import math
import os
import re
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional, Tuple, Set

_log = logging.getLogger("wnp.pso_kubra")

import requests

from app.netguard import limited_get
import mercantile
import polyline


# -------------------------- PSO HOSTS / ENTRYPOINTS --------------------------

OUTAGEMAP_BASE = "https://outagemap.psoklahoma.com"
KUBRA_API_BASE = "https://kubra.io/stormcenter/api/v1"
KUBRA_TILE_BASE = "https://kubra.io"

CHI_TZ = ZoneInfo("America/Chicago")
UTC_TZ = ZoneInfo("UTC")

# Known-good IDs (fallback only — from current DevTools inspection)
FALLBACK_STORMCENTER_ID = "4bb3b3bc-e1c4-448b-b806-e4fc85c3b640"
FALLBACK_VIEW_ID = "e2356e43-c76f-4772-bf85-31240a2cc504"

# Discovery cache to avoid repeated expensive discovery per call
_DISCOVERY_CACHE: Dict[str, Tuple[str, str]] = {}

# Background refresh state
_REFRESH_STOP_EVENT: threading.Event = threading.Event()
_REFRESH_THREAD: Optional[threading.Thread] = None
_REFRESH_LOCK: threading.Lock = threading.Lock()
REFRESH_INTERVAL_S: int = 10800  # 3 hours

# Cache discovered tile scheme keyed by cluster template.
# Value: (layer_name, qkh_strategy, layout_name, entry_zoom, ts)
_SCHEME_CACHE: Dict[str, Tuple[str, str, str, int, float]] = {}
SCHEME_CACHE_TTL_S = 600

# PSO-territory probe points
PROBE_POINTS: List[Tuple[float, float]] = [
    (36.15398, -95.99277),  # Tulsa
    (36.05260, -95.79082),  # Broken Arrow
    (36.13981, -96.10889),  # Sand Springs
    (35.42702, -99.39026),  # Elk City-ish (west OK)
]

ZOOM_CANDIDATES = [10, 11, 12, 13, 14]
QKH_STRATEGIES = ["last3_rev", "last3", "first3", "first3_rev", "last4_rev"]

# Fast probe first (cold-start latency control), then exhaustive fallback.
FAST_PROBE_POINTS: List[Tuple[float, float]] = [PROBE_POINTS[0]]
FAST_ZOOM_CANDIDATES = [12, 11, 10]
FAST_QKH_STRATEGIES = ["last3_rev", "last3"]
PROBE_REQUEST_TIMEOUT_S = 2.0
PROBE_SCHEME_BUDGET_S = 12.0

# Known-good quick candidates observed in production traffic.
KNOWN_SCHEME_CANDIDATES: List[Tuple[str, str, str, int]] = [
    ("cluster-1", "last3_rev", "flat", 10),
]
KNOWN_SCHEME_TIMEOUT_S = 6.0

# Keep PSO responses fast/stable in production. Dynamic discovery fallback can be
# re-enabled for troubleshooting by setting PSO_DYNAMIC_DISCOVERY_FALLBACK=true.
PSO_DYNAMIC_DISCOVERY_FALLBACK = (
    os.getenv("PSO_DYNAMIC_DISCOVERY_FALLBACK", "false").strip().lower() in {"1", "true", "yes", "on"}
)

URL_LAYOUTS = [
    ("flat",   lambda base, layer, qk: f"{KUBRA_TILE_BASE}/{base}/public/{layer}/{qk}.json"),
    ("split2", lambda base, layer, qk: f"{KUBRA_TILE_BASE}/{base}/public/{layer}/{qk[:2]}/{qk}.json"),
]

FALLBACK_LAYER_CANDIDATES = [f"cluster-{i}" for i in range(1, 9)]


# -------------------------- CONTROLLED EXCEPTIONS --------------------------

class PSOKubraError(Exception):
    pass

class PSOKubraDiscoveryError(PSOKubraError):
    pass

class PSOKubraFetchError(PSOKubraError):
    pass


# -------------------------- HELPERS --------------------------

def _dbg(debug: bool, *args) -> None:
    if debug:
        print(*args)

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "WeatherPower/1.0 (PSO Kubra integration)",
        "Accept": "application/json, text/plain, */*",
    })
    return s

def _get_text(s: requests.Session, url: str, timeout: float = 3.0) -> Optional[str]:
    r = limited_get(s, url, timeout=timeout)
    if r.status_code != 200:
        return None
    return r.text

def _get_json(s: requests.Session, url: str, timeout: float = 3.0) -> Optional[Dict[str, Any]]:
    r = limited_get(s, url, timeout=timeout)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def _expand_quadkeys(base_quadkey: str, depth: int) -> List[str]:
    if depth <= 0:
        return [base_quadkey]
    t = mercantile.quadkey_to_tile(base_quadkey)
    keys = []
    for dx in range(-depth, depth + 1):
        for dy in range(-depth, depth + 1):
            keys.append(mercantile.quadkey(mercantile.Tile(t.x + dx, t.y + dy, t.z)))
    return list(set(keys))


# -------------------------- TIME NORMALIZATION --------------------------

def _parse_iso(dt_str: Any) -> Optional[datetime]:
    if not isinstance(dt_str, str) or not dt_str:
        return None
    s = dt_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None

def _to_chicago_iso(dt_str: Any) -> Optional[str]:
    if not isinstance(dt_str, str) or not dt_str:
        return None
    dt = _parse_iso(dt_str)
    if not dt:
        return dt_str
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC_TZ)
    return dt.astimezone(CHI_TZ).isoformat()


# -------------------------- qkh shard helpers --------------------------

def _qkh_from_quadkey(qk: str, strategy: str) -> str:
    if not qk:
        return "000"
    if strategy == "last3":
        return qk[-3:].rjust(3, "0")
    if strategy == "last3_rev":
        return qk[-3:].rjust(3, "0")[::-1]
    if strategy == "first3":
        return qk[:3].ljust(3, "0")
    if strategy == "first3_rev":
        return qk[:3].ljust(3, "0")[::-1]
    if strategy == "last4_rev":
        return qk[-4:].rjust(4, "0")[::-1]
    return qk[-3:].rjust(3, "0")[::-1]


# ======================== DISCOVERY CACHE ========================

def _cache_discovery(key: str, value: Tuple[str, str]) -> None:
    """Cache discovery result (stormcenter_id, view_id) tuple."""
    _DISCOVERY_CACHE[key] = value

def _get_cached_discovery(key: str) -> Optional[Tuple[str, str]]:
    """Retrieve cached discovery result if available."""
    return _DISCOVERY_CACHE.get(key)


def _cache_tile_scheme(cluster_template: str, layer_name: str, qkh_strategy: str, layout_name: str, entry_zoom: int) -> None:
    _SCHEME_CACHE[cluster_template] = (layer_name, qkh_strategy, layout_name, entry_zoom, time.time())


def _get_cached_tile_scheme(cluster_template: str) -> Optional[Tuple[str, str, str, int]]:
    rec = _SCHEME_CACHE.get(cluster_template)
    if not rec:
        return None
    layer_name, qkh_strategy, layout_name, entry_zoom, ts = rec
    if (time.time() - ts) > SCHEME_CACHE_TTL_S:
        _SCHEME_CACHE.pop(cluster_template, None)
        return None
    return (layer_name, qkh_strategy, layout_name, entry_zoom)


# -------------------------- DISCOVERY --------------------------

_UUID_RE = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"

def _discover_stormcenter_and_view(s: requests.Session, debug: bool) -> Tuple[str, str]:
    # Check cache first to avoid repeated discovery
    cached = _get_cached_discovery("stormcenter_view")
    if cached:
        _dbg(debug, f"DISCOVERY (cached): stormcenter_id={cached[0]} view_id={cached[1]}")
        return cached

    # Fast-path: known-good IDs are often stable for PSO and avoid expensive
    # asset scraping. If this works, keep it.
    try:
        js = _fetch_current_state(
            s,
            FALLBACK_STORMCENTER_ID,
            FALLBACK_VIEW_ID,
            debug,
            allow_retry=False,
        )
        if isinstance(js, dict):
            result = (FALLBACK_STORMCENTER_ID, FALLBACK_VIEW_ID)
            _cache_discovery("stormcenter_view", result)
            _dbg(debug, "DISCOVERY fast-path: using known IDs")
            return result
    except Exception:
        pass

    html = _get_text(s, f"{OUTAGEMAP_BASE}/")
    if not html:
        _dbg(debug, "PROBE: failed to load outagemap HTML, falling back to known IDs")
        result = (FALLBACK_STORMCENTER_ID, FALLBACK_VIEW_ID)
        _cache_discovery("stormcenter_view", result)
        return result

    pat = re.compile(r"/stormcenters/(" + _UUID_RE + r")/views/(" + _UUID_RE + r")/currentState")
    m = pat.search(html)
    if m:
        sc_id, view_id = m.group(1), m.group(2)
        _dbg(debug, f"DISCOVERY stormcenter_id={sc_id} view_id={view_id} (from HTML)")
        result = (sc_id, view_id)
        _cache_discovery("stormcenter_view", result)
        return result

    script_urls = re.findall(r'<script[^>]+src="([^"]+)"', html)
    script_urls = [u for u in script_urls if u.endswith(".js")]
    norm = []
    for u in script_urls[:5]:
        if u.startswith("http"):
            norm.append(u)
        else:
            norm.append(f"{OUTAGEMAP_BASE}{u if u.startswith('/') else '/' + u}")

    for u in norm:
        js = _get_text(s, u)
        if not js:
            continue
        m = pat.search(js)
        if m:
            sc_id, view_id = m.group(1), m.group(2)
            _dbg(debug, f"DISCOVERY stormcenter_id={sc_id} view_id={view_id} (from JS)")
            result = (sc_id, view_id)
            _cache_discovery("stormcenter_view", result)
            return result

    _dbg(debug, "DISCOVERY: could not find IDs in assets; using known DevTools IDs")
    result = (FALLBACK_STORMCENTER_ID, FALLBACK_VIEW_ID)
    _cache_discovery("stormcenter_view", result)
    return result

def _fetch_current_state(s: requests.Session, stormcenter_id: str, view_id: str, debug: bool, allow_retry: bool = True) -> Optional[Dict[str, Any]]:
    url = f"{KUBRA_API_BASE}/stormcenters/{stormcenter_id}/views/{view_id}/currentState?preview=false"
    js = _get_json(s, url)
    if not isinstance(js, dict):
        # If this fails and we were using cached IDs, clear cache to force fresh discovery
        if allow_retry and _get_cached_discovery("stormcenter_view") is not None:
            _dbg(debug, f"DISCOVERY: currentState fetch failed with cached IDs; clearing cache for retry")
            _DISCOVERY_CACHE.clear()
            return None  # Signal to retry
        raise PSOKubraDiscoveryError(f"Failed to fetch currentState from: {url}")
    _dbg(debug, "PROBE SUCCESS:", url)
    return js

def _extract_cluster_template_and_deployment(state: Dict[str, Any]) -> Tuple[str, str]:
    dep = state.get("stormcenterDeploymentId")
    if not isinstance(dep, str) or not dep:
        raise PSOKubraDiscoveryError("currentState missing stormcenterDeploymentId")

    data = state.get("data", {}) if isinstance(state.get("data"), dict) else {}
    templ = data.get("cluster_interval_generation_data")
    if not isinstance(templ, str) or "cluster-data" not in templ:
        raise PSOKubraDiscoveryError("currentState missing cluster_interval_generation_data template")

    templ = templ.lstrip("/")
    if "/public" in templ:
        templ = templ.split("/public")[0]

    return templ, dep

def _deep_collect(obj: Any, pred) -> List[Any]:
    out = []
    stack = [obj]
    while stack:
        cur = stack.pop()
        try:
            if pred(cur):
                out.append(cur)
        except Exception:
            pass
        if isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return out

def _extract_cluster_layers_from_config(config: Dict[str, Any]) -> List[str]:
    layers: List[str] = []

    def is_cluster_layer(x: Any) -> bool:
        return (
            isinstance(x, dict)
            and isinstance(x.get("type"), str)
            and x["type"].startswith("CLUSTER_LAYER")
            and isinstance(x.get("name"), str)
            and x["name"]
        )

    objs = _deep_collect(config, is_cluster_layer)
    for o in objs:
        name = o.get("name")
        if name and name not in layers:
            layers.append(name)

    return layers

def _fetch_deployment_or_configuration(s: requests.Session, deployment_id: str, debug: bool) -> Optional[Dict[str, Any]]:
    candidates = [
        f"{KUBRA_API_BASE}/deployments/{deployment_id}",
        f"{KUBRA_API_BASE}/deployments/{deployment_id}/configuration",
        f"{OUTAGEMAP_BASE}/configuration/{deployment_id}",
        f"{OUTAGEMAP_BASE}/configuration/{deployment_id}.json",
        f"{OUTAGEMAP_BASE}/public/configuration/{deployment_id}",
        f"{OUTAGEMAP_BASE}/public/configuration/{deployment_id}.json",
    ]

    for url in candidates:
        js = _get_json(s, url)
        if isinstance(js, dict):
            _dbg(debug, "PROBE SUCCESS:", url)
            return js
        _dbg(debug, "PROBE:", url)

    return None

def _render_cluster_base(template: str, qkh: str) -> str:
    base = template.replace("(qkh)", qkh).replace("{qkh}", qkh)
    return base.strip("/")


# -------------------------- TILE PARSE / NORMALIZATION --------------------------

def _is_cluster(feature: Dict[str, Any]) -> bool:
    desc = feature.get("desc", {}) or {}
    return bool(desc.get("cluster"))

def _extract_location(feature: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    geom = feature.get("geom", {}) or {}
    pts = geom.get("p", []) or []
    if not pts:
        return None
    try:
        return polyline.decode(pts[0])[0]
    except Exception:
        return None

def _coerce_localized_text(val: Any) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, str):
        return val.strip() or None
    if isinstance(val, dict):
        if "EN-US" in val and isinstance(val["EN-US"], str) and val["EN-US"].strip():
            return val["EN-US"].strip()
        for v in val.values():
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None

def _normalize_outage(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    desc = feature.get("desc", {}) or {}
    loc = _extract_location(feature)
    if not loc:
        return None

    inc_id = desc.get("inc_id")
    if not inc_id:
        inc_id = f"{loc}-{desc.get('start_time', 'unknown')}"

    cause = _coerce_localized_text(desc.get("cause"))
    comments = _coerce_localized_text(desc.get("comments"))
    crew_status = _coerce_localized_text(desc.get("crew_status"))

    customers_out = None
    cust_a = desc.get("cust_a")
    if isinstance(cust_a, dict) and "val" in cust_a:
        customers_out = cust_a.get("val")
    if customers_out is None:
        customers_out = desc.get("customers_out")

    try:
        customers_out = int(customers_out) if customers_out is not None else None
    except Exception:
        customers_out = None

    n_out = desc.get("n_out")
    try:
        n_out = int(n_out) if n_out is not None else None
    except Exception:
        n_out = None

    etr_local = _to_chicago_iso(desc.get("etr"))
    start_local = _to_chicago_iso(desc.get("start_time"))

    return {
        "id": str(inc_id),
        "cluster": bool(desc.get("cluster")),
        "customers_out": customers_out,
        "n_out": n_out,
        "etr": etr_local,
        "etr_confidence": desc.get("etr_confidence"),
        "cause": cause,
        "comments": comments,
        "crew_status": crew_status,
        "start_time": start_local,
        "latitude": float(loc[0]),
        "longitude": float(loc[1]),
    }

def _parse_tile(tile_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    fd = tile_json.get("file_data", []) or []
    if not isinstance(fd, list):
        return []
    outs: List[Dict[str, Any]] = []
    for feat in fd:
        if not isinstance(feat, dict):
            continue
        o = _normalize_outage(feat)
        if o:
            outs.append({"_raw": feat, **o})
    return outs


# -------------------------- CLIENT --------------------------

class PSOKubraClient:
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.s = _session()

        self.stormcenter_id: Optional[str] = None
        self.view_id: Optional[str] = None

        self.cluster_template: Optional[str] = None
        self.deployment_id: Optional[str] = None
        self.cluster_layers: List[str] = []

        self.entry_zoom: Optional[int] = None
        self.layer_name: Optional[str] = None
        self.qkh_strategy: Optional[str] = None
        self.layout_name: Optional[str] = None

    def discover(self) -> None:
        sc_id, view_id = _discover_stormcenter_and_view(self.s, self.debug)
        self.stormcenter_id = sc_id
        self.view_id = view_id

        state = _fetch_current_state(self.s, sc_id, view_id, self.debug)
        # If currentState fetch failed with cached IDs, retry discovery (IDs may have rotated)
        if state is None:
            _dbg(self.debug, "DISCOVERY: retrying with fresh discovery after cache clear")
            sc_id, view_id = _discover_stormcenter_and_view(self.s, self.debug)
            self.stormcenter_id = sc_id
            self.view_id = view_id
            state = _fetch_current_state(self.s, sc_id, view_id, self.debug, allow_retry=False)
        
        templ, dep = _extract_cluster_template_and_deployment(state)
        self.cluster_template = templ
        self.deployment_id = dep

        _dbg(self.debug, f"DISCOVERY stormcenterDeploymentId={dep}")
        _dbg(self.debug, f"DISCOVERY cluster_template={templ}")

        cfg = _fetch_deployment_or_configuration(self.s, dep, self.debug)
        if isinstance(cfg, dict):
            layers = _extract_cluster_layers_from_config(cfg)
            if layers:
                self.cluster_layers = layers

        if not self.cluster_layers:
            _dbg(self.debug, "DISCOVERY: no CLUSTER_LAYER list found; using fallback layer candidates for probing")
            self.cluster_layers = list(FALLBACK_LAYER_CANDIDATES)

    def _tile_url(self, cluster_base: str, layer: str, quadkey: str, layout_fn) -> str:
        return layout_fn(cluster_base, layer, quadkey)

    def probe_tile_scheme(self) -> Tuple[str, str, str, str, int]:
        if not self.cluster_template:
            raise PSOKubraDiscoveryError("Missing cluster template; did discover() run?")

        probe_started = time.time()

        def _budget_exceeded() -> bool:
            return (time.time() - probe_started) > PROBE_SCHEME_BUDGET_S

        cached = _get_cached_tile_scheme(self.cluster_template)
        if cached:
            layer, strat, layout_name, zoom = cached
            self.entry_zoom = zoom
            self.layer_name = layer
            self.qkh_strategy = strat
            self.layout_name = layout_name

            plat, plon = PROBE_POINTS[0]
            qk = mercantile.quadkey(mercantile.tile(plon, plat, zoom))
            qkh = _qkh_from_quadkey(qk, strat)
            base = _render_cluster_base(self.cluster_template, qkh)
            _dbg(self.debug, f"DISCOVERY (cached tile scheme): layer={layer} zoom={zoom} qkh={strat} layout={layout_name}")
            return base, layer, strat, layout_name, zoom

        # Try known-good scheme candidates with a longer single-request timeout
        # before broader probing.
        layout_map = dict(URL_LAYOUTS)
        for layer, strat, layout_name, zoom in KNOWN_SCHEME_CANDIDATES:
            if layout_name not in layout_map:
                continue
            layout_fn = layout_map[layout_name]

            for plat, plon in FAST_PROBE_POINTS:
                qk = mercantile.quadkey(mercantile.tile(plon, plat, zoom))
                qkh = _qkh_from_quadkey(qk, strat)
                base = _render_cluster_base(self.cluster_template, qkh)
                url = self._tile_url(base, layer, qk, layout_fn)

                _dbg(self.debug, f"KNOWN SCHEME PROBE layer={layer} zoom={zoom} qkh={strat} layout={layout_name}")
                _dbg(self.debug, "  ", url)

                js = _get_json(self.s, url, timeout=KNOWN_SCHEME_TIMEOUT_S)
                if isinstance(js, dict) and isinstance(js.get("file_data"), list):
                    _dbg(self.debug, "KNOWN SCHEME SUCCESS:", url)
                    self.entry_zoom = zoom
                    self.layer_name = layer
                    self.qkh_strategy = strat
                    self.layout_name = layout_name
                    _cache_tile_scheme(self.cluster_template, layer, strat, layout_name, zoom)
                    return base, layer, strat, layout_name, zoom

        # Fast-path: try a constrained candidate set first to keep first-call latency low.
        fast_layers: List[str] = []
        if "cluster-1" in self.cluster_layers:
            fast_layers.append("cluster-1")
        for name in self.cluster_layers:
            if name not in fast_layers:
                fast_layers.append(name)
        fast_layers = fast_layers[:3]

        for plat, plon in FAST_PROBE_POINTS:
            for z in FAST_ZOOM_CANDIDATES:
                qk = mercantile.quadkey(mercantile.tile(plon, plat, z))

                for layer in fast_layers:
                    for strat in FAST_QKH_STRATEGIES:
                        qkh = _qkh_from_quadkey(qk, strat)
                        base = _render_cluster_base(self.cluster_template, qkh)

                        for layout_name, layout_fn in URL_LAYOUTS:
                            if _budget_exceeded():
                                raise PSOKubraDiscoveryError(
                                    f"Tile scheme probe exceeded budget ({PROBE_SCHEME_BUDGET_S}s)"
                                )
                            url = self._tile_url(base, layer, qk, layout_fn)
                            _dbg(self.debug, f"FAST PROBE layer={layer} zoom={z} qkh={strat} layout={layout_name}")
                            _dbg(self.debug, "  ", url)

                            js = _get_json(self.s, url, timeout=PROBE_REQUEST_TIMEOUT_S)
                            if isinstance(js, dict) and isinstance(js.get("file_data"), list):
                                _dbg(self.debug, "FAST PROBE SUCCESS:", url)
                                _dbg(self.debug, f"DISCOVERED layer_name={layer} entry_zoom={z} cluster_data_path={base}")

                                self.entry_zoom = z
                                self.layer_name = layer
                                self.qkh_strategy = strat
                                self.layout_name = layout_name
                                _cache_tile_scheme(self.cluster_template, layer, strat, layout_name, z)
                                return base, layer, strat, layout_name, z

        for plat, plon in PROBE_POINTS:
            for z in ZOOM_CANDIDATES:
                qk = mercantile.quadkey(mercantile.tile(plon, plat, z))

                for layer in self.cluster_layers:
                    for strat in QKH_STRATEGIES:
                        qkh = _qkh_from_quadkey(qk, strat)
                        base = _render_cluster_base(self.cluster_template, qkh)

                        for layout_name, layout_fn in URL_LAYOUTS:
                            if _budget_exceeded():
                                raise PSOKubraDiscoveryError(
                                    f"Tile scheme probe exceeded budget ({PROBE_SCHEME_BUDGET_S}s)"
                                )
                            url = self._tile_url(base, layer, qk, layout_fn)
                            _dbg(self.debug, f"PROBE layer={layer} zoom={z} qkh={strat} layout={layout_name}")
                            _dbg(self.debug, "  ", url)

                            js = _get_json(self.s, url, timeout=PROBE_REQUEST_TIMEOUT_S)
                            if isinstance(js, dict) and isinstance(js.get("file_data"), list):
                                _dbg(self.debug, "PROBE SUCCESS:", url)
                                _dbg(self.debug, f"DISCOVERED layer_name={layer} entry_zoom={z} cluster_data_path={base}")

                                self.entry_zoom = z
                                self.layer_name = layer
                                self.qkh_strategy = strat
                                self.layout_name = layout_name
                                _cache_tile_scheme(self.cluster_template, layer, strat, layout_name, z)
                                return base, layer, strat, layout_name, z

        raise PSOKubraDiscoveryError("Failed to discover working tile combo (layer/zoom/qkh/layout).")

    def _fetch_tile_features(
        self,
        cluster_base: str,
        layer: str,
        quadkey: str,
        layout_fn,
        seen_urls: Set[str],
        seen_quadkeys: Set[Tuple[int, str]],
        zoom: int
    ) -> List[Dict[str, Any]]:
        key = (zoom, quadkey)
        if key in seen_quadkeys:
            return []
        seen_quadkeys.add(key)

        url = self._tile_url(cluster_base, layer, quadkey, layout_fn)
        if url in seen_urls:
            return []
        seen_urls.add(url)

        js = _get_json(self.s, url)
        if not isinstance(js, dict):
            return []

        outs = _parse_tile(js)
        _dbg(self.debug, f"FETCH quadkey={quadkey} layer={layer} features={len(outs)}")
        return outs

    def fetch_outages_near(
        self,
        lat: float,
        lon: float,
        max_radius_km: float,
        max_zoom: int,
        neighbor_depth: int,
        drill_neighbor_depth: int
    ) -> Dict[str, Any]:
        if self.entry_zoom is None or self.layer_name is None or self.layout_name is None:
            raise PSOKubraDiscoveryError("Tile scheme not discovered; did probe_tile_scheme() run?")

        layout_fn = dict(URL_LAYOUTS)[self.layout_name]

        qk0 = mercantile.quadkey(mercantile.tile(lon, lat, self.entry_zoom))
        seeds = _expand_quadkeys(qk0, neighbor_depth)

        outages_by_id: Dict[str, Dict[str, Any]] = {}
        seen_urls: Set[str] = set()
        seen_quadkeys: Set[Tuple[int, str]] = set()

        cluster_queue: List[Tuple[int, Dict[str, Any]]] = []

        # ✅ CRITICAL: compute qkh + base per seed tile (shard varies by quadkey)
        for q in seeds:
            qkh = _qkh_from_quadkey(q, self.qkh_strategy or "last3_rev")
            cluster_base = _render_cluster_base(self.cluster_template or "", qkh)

            raw = self._fetch_tile_features(
                cluster_base,
                self.layer_name,
                q,
                layout_fn,
                seen_urls,
                seen_quadkeys,
                self.entry_zoom
            )
            for item in raw:
                feat = item.get("_raw") or {}
                if _is_cluster(feat):
                    cluster_queue.append((self.entry_zoom, item))
                else:
                    oid = item.get("id")
                    if oid and oid not in outages_by_id:
                        outages_by_id[oid] = item

        _dbg(self.debug, f"ENTRY FETCH complete: seed_tiles={len(seeds)} tiles (entry_zoom={self.entry_zoom}, layer={self.layer_name}, qkh={self.qkh_strategy}, layout={self.layout_name})")

        while cluster_queue:
            z, item = cluster_queue.pop(0)
            if z >= max_zoom:
                continue

            clat = item["latitude"]
            clon = item["longitude"]
            next_z = z + 1

            _dbg(self.debug, f"DRILL cluster z={z} -> z={next_z} at ({clat:.5f},{clon:.5f})")

            child_qk = mercantile.quadkey(mercantile.tile(clon, clat, next_z))
            child_tiles = _expand_quadkeys(child_qk, drill_neighbor_depth)

            for cq in child_tiles:
                qkh = _qkh_from_quadkey(cq, self.qkh_strategy or "last3_rev")
                cb = _render_cluster_base(self.cluster_template or "", qkh)

                raw2 = self._fetch_tile_features(cb, self.layer_name, cq, layout_fn, seen_urls, seen_quadkeys, next_z)
                for item2 in raw2:
                    feat2 = item2.get("_raw") or {}
                    if _is_cluster(feat2):
                        cluster_queue.append((next_z, item2))
                    else:
                        oid2 = item2.get("id")
                        if oid2 and oid2 not in outages_by_id:
                            outages_by_id[oid2] = item2

        outages: List[Dict[str, Any]] = []
        for o in outages_by_id.values():
            dkm = _haversine_km(lat, lon, o["latitude"], o["longitude"])
            if dkm <= max_radius_km:
                o2 = {k: v for k, v in o.items() if k != "_raw"}
                o2["distance_km"] = dkm
                outages.append(o2)

        outages.sort(key=lambda x: x.get("distance_km", 1e9))
        nearest = outages[0] if outages else None

        return {"nearest": nearest, "outages": outages}


# -------------------------- BACKGROUND REFRESH --------------------------

def refresh_pso_discovery(debug: bool = False) -> None:
    """Force-refresh the PSO stormcenter/view ID discovery cache.

    Clears any cached IDs and re-runs discovery from scratch so the cache is
    always up-to-date before a real query arrives. Swallows all exceptions and
    logs a warning — never raises.
    """
    try:
        _DISCOVERY_CACHE.clear()
        s = _session()
        sc_id, view_id = _discover_stormcenter_and_view(s, debug)
        _log.info("PSO discovery refresh ok: stormcenter_id=%s view_id=%s", sc_id, view_id)
    except Exception as exc:
        _log.warning("PSO discovery refresh failed: %s: %s", type(exc).__name__, exc)


def start_pso_background_refresh(interval_s: int = REFRESH_INTERVAL_S) -> None:
    """Start a daemon thread that periodically refreshes the PSO discovery cache.

    Idempotent — calling this more than once has no effect if the thread is
    already running. The thread is a daemon and will not prevent process exit.
    """
    global _REFRESH_THREAD

    with _REFRESH_LOCK:
        if _REFRESH_THREAD is not None and _REFRESH_THREAD.is_alive():
            return

        _REFRESH_STOP_EVENT.clear()

        def _loop() -> None:
            while not _REFRESH_STOP_EVENT.wait(timeout=interval_s):
                refresh_pso_discovery()

        _REFRESH_THREAD = threading.Thread(target=_loop, name="pso-kubra-refresh", daemon=True)
        _REFRESH_THREAD.start()
        _log.info("PSO background refresh thread started (interval=%ds)", interval_s)


def stop_pso_background_refresh() -> None:
    """Signal the background refresh thread to stop. Returns immediately."""
    _REFRESH_STOP_EVENT.set()
    _log.info("PSO background refresh thread stop signalled")


# -------------------------- PUBLIC FUNCTION --------------------------

def fetch_pso_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 50.0,
    max_zoom: int = 12,
    neighbor_depth: int = 1,
    drill_neighbor_depth: int = 1,
    debug: bool = False,
) -> Dict[str, Any]:
    client = PSOKubraClient(debug=debug)
    client.discover()

    # Fast primary path: known-good static scheme for PSO.
    client.entry_zoom = 10
    client.layer_name = "cluster-1"
    client.qkh_strategy = "last3_rev"
    client.layout_name = "flat"
    _dbg(debug, "FAST PATH: using static tile scheme cluster-1/last3_rev/flat/z10")

    fast_result = client.fetch_outages_near(
        lat=lat,
        lon=lon,
        max_radius_km=max_radius_km,
        max_zoom=min(max_zoom, 11),
        neighbor_depth=neighbor_depth,
        drill_neighbor_depth=0,
    )

    if fast_result.get("nearest") or fast_result.get("outages"):
        return fast_result

    if not PSO_DYNAMIC_DISCOVERY_FALLBACK:
        return fast_result

    # If fast path found no data, attempt dynamic discovery probe as fallback.
    try:
        client.probe_tile_scheme()
        return client.fetch_outages_near(
            lat=lat,
            lon=lon,
            max_radius_km=max_radius_km,
            max_zoom=min(max_zoom, 11),
            neighbor_depth=neighbor_depth,
            drill_neighbor_depth=0,
        )
    except PSOKubraDiscoveryError:
        return fast_result


# -------------------------- SELF TEST --------------------------

if __name__ == "__main__":
    test_lat, test_lon = 36.15398, -95.99277
    print("Testing PSO outage fetch (debug on, max_zoom=12)...")

    try:
        res = fetch_pso_outages(test_lat, test_lon, debug=True)
        print("\nRESULT SUMMARY")
        print("Outages returned:", len(res["outages"]))
        if res["nearest"]:
            n = res["nearest"]
            print("Nearest id:", n.get("id"))
            print("Nearest customers_out:", n.get("customers_out"))
            print("Nearest crew_status:", n.get("crew_status"))
            print("Nearest start_time (CT):", n.get("start_time"))
            print("Nearest etr (CT):", n.get("etr"))
            print("Nearest distance_km:", round(n.get("distance_km", 0.0), 2))
        else:
            print("Nearest outage: null")

    except PSOKubraDiscoveryError as e:
        print("PSOKubraDiscoveryError:", str(e))
        print({"nearest": None, "outages": []})
    except Exception as e:
        print("UNEXPECTED ERROR:", str(e))
        print({"nearest": None, "outages": []})
