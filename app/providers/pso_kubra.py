"""
PSO KUBRA Storm Center outage integration

Matches OG&E provider contract:
  fetch_pso_outages(lat, lon, max_radius_km=50.0, max_zoom=12,
                    neighbor_depth=1, drill_neighbor_depth=1, debug=False)

Returns:
  { "nearest": <outage|null>, "outages": [<outage>...] }

Outage fields (consistent with OG&E):
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
  distance_km (added before return)

NOTE:
- This module normalizes etr/start_time into America/Chicago ISO strings
  (with offset) to support UI display labeled as CT.
"""

import math
import re
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional, Tuple, Set

import requests
import mercantile
import polyline


# -------------------------- DISCOVERY/TILE-SCHEME CACHE --------------------------
# PSO discovery + tile-scheme probing is relatively expensive (multiple HTTP calls).
# Cache derived parameters for a short TTL so most requests only do tile fetches.
_CACHE_TTL_S = 300  # 5 minutes
_cache_lock = threading.Lock()
_cached_profile: Optional[Dict[str, Any]] = None  # {ts: float, ...fields...}


# -------------------------- PSO HOSTS / ENTRYPOINTS --------------------------

OUTAGEMAP_BASE = "https://outagemap.psoklahoma.com"
KUBRA_API_BASE = "https://kubra.io/stormcenter/api/v1"
KUBRA_TILE_BASE = "https://kubra.io"

CHI_TZ = ZoneInfo("America/Chicago")
UTC_TZ = ZoneInfo("UTC")

# Known-good IDs you captured (fallback only).
# These are NOT "guesses" — they are taken from your DevTools capture.
FALLBACK_STORMCENTER_ID = "4bb3b3bc-e1c4-448b-b806-e4fc85c3b640"
FALLBACK_VIEW_ID = "e2356e43-c76f-4772-bf85-31240a2cc504"

# PSO-territory probe points (per requirement)
PROBE_POINTS: List[Tuple[float, float]] = [
    (36.15398, -95.99277),  # Tulsa
    (36.05260, -95.79082),  # Broken Arrow
    (36.13981, -96.10889),  # Sand Springs
    (35.42702, -99.39026),  # Elk City-ish (corrected west OK longitude)
]

# User-required zoom candidates; we include 10 as well because a proven tile quadkey is length 10.
ZOOM_CANDIDATES = [10, 11, 12, 13, 14]

QKH_STRATEGIES = ["last3_rev", "last3", "first3", "first3_rev", "last4_rev"]

URL_LAYOUTS = [
    ("flat", lambda base, layer, qk: f"{KUBRA_TILE_BASE}/{base}/public/{layer}/{qk}.json"),
    ("split2", lambda base, layer, qk: f"{KUBRA_TILE_BASE}/{base}/public/{layer}/{qk[:2]}/{qk}.json"),
]

# If config parsing fails, we probe these layer candidates without hardcoding a single one.
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
    s.headers.update(
        {
            "User-Agent": "NOCTriage/1.0 (PSO Kubra integration)",
            "Accept": "application/json, text/plain, */*",
        }
    )
    return s


def _get_text(s: requests.Session, url: str, timeout: float = 15.0) -> Optional[str]:
    r = s.get(url, timeout=timeout)
    if r.status_code != 200:
        return None
    return r.text


def _get_json(s: requests.Session, url: str, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
    r = s.get(url, timeout=timeout)
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
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


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

    # Handle UTC "Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _to_chicago_iso(dt_str: Any) -> Optional[str]:
    """
    Convert ISO timestamps to America/Chicago and return ISO string with offset.
    If parsing fails, return original string if it was a string; else None.
    """
    if not isinstance(dt_str, str) or not dt_str:
        return None
    dt = _parse_iso(dt_str)
    if not dt:
        return dt_str  # preserve original string defensively

    if dt.tzinfo is None:
        # Treat naive as UTC (defensive)
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


# -------------------------- DISCOVERY --------------------------

_UUID_RE = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"


def _discover_stormcenter_and_view(s: requests.Session, debug: bool) -> Tuple[str, str]:
    """
    Preferred: scrape outagemap HTML/JS for the pattern:
      /stormcenters/<uuid>/views/<uuid>/currentState
    Fallback: use known-good IDs from DevTools capture.
    """
    html = _get_text(s, f"{OUTAGEMAP_BASE}/")
    if not html:
        _dbg(debug, "PROBE: failed to load outagemap HTML, falling back to known IDs")
        return FALLBACK_STORMCENTER_ID, FALLBACK_VIEW_ID

    pat = re.compile(r"/stormcenters/(" + _UUID_RE + r")/views/(" + _UUID_RE + r")/currentState")
    m = pat.search(html)
    if m:
        sc_id, view_id = m.group(1), m.group(2)
        _dbg(debug, f"DISCOVERY stormcenter_id={sc_id} view_id={view_id} (from HTML)")
        return sc_id, view_id

    # Scan up to 5 JS bundles for the same pattern
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
            return sc_id, view_id

    _dbg(debug, "DISCOVERY: could not find IDs in assets; using known DevTools IDs")
    return FALLBACK_STORMCENTER_ID, FALLBACK_VIEW_ID


def _fetch_current_state(s: requests.Session, stormcenter_id: str, view_id: str, debug: bool) -> Dict[str, Any]:
    """
    PSO uses Kubra API currentState (confirmed by DevTools capture).
    """
    url = f"{KUBRA_API_BASE}/stormcenters/{stormcenter_id}/views/{view_id}/currentState?preview=false"
    js = _get_json(s, url)
    if not isinstance(js, dict):
        raise PSOKubraDiscoveryError(f"Failed to fetch currentState from: {url}")
    _dbg(debug, "PROBE SUCCESS:", url)
    return js


def _extract_cluster_template_and_deployment(state: Dict[str, Any]) -> Tuple[str, str]:
    """
    Extract:
      stormcenterDeploymentId
      cluster_interval_generation_data (template)
    """
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
    """
    Try multiple likely endpoints to obtain layer configuration.
    Kept flexible across Kubra deployments.
    """
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


# -------------------------- TILE PARSE / NORMALIZATION (OG&E-compatible) --------------------------

def _is_cluster(feature: Dict[str, Any]) -> bool:
    desc = feature.get("desc", {}) or {}
    return bool(desc.get("cluster"))


def _extract_location(feature: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """
    Same as OG&E: geom.p[0] is an encoded polyline. First decoded point is the location.
    """
    geom = feature.get("geom", {}) or {}
    pts = geom.get("p", []) or []
    if not pts:
        return None
    try:
        return polyline.decode(pts[0])[0]  # (lat, lon)
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

        for plat, plon in PROBE_POINTS:
            for z in ZOOM_CANDIDATES:
                qk = mercantile.quadkey(mercantile.tile(plon, plat, z))

                for layer in self.cluster_layers:
                    for strat in QKH_STRATEGIES:
                        qkh = _qkh_from_quadkey(qk, strat)
                        base = _render_cluster_base(self.cluster_template, qkh)

                        for layout_name, layout_fn in URL_LAYOUTS:
                            url = self._tile_url(base, layer, qk, layout_fn)
                            _dbg(self.debug, f"PROBE layer={layer} zoom={z} qkh={strat} layout={layout_name}")
                            _dbg(self.debug, "  ", url)

                            js = _get_json(self.s, url)
                            if isinstance(js, dict) and isinstance(js.get("file_data"), list):
                                _dbg(self.debug, "PROBE SUCCESS:", url)
                                _dbg(self.debug, f"DISCOVERED layer_name={layer} entry_zoom={z} cluster_data_path={base}")

                                self.entry_zoom = z
                                self.layer_name = layer
                                self.qkh_strategy = strat
                                self.layout_name = layout_name
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
        zoom: int,
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
        drill_neighbor_depth: int,
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

        # recompute qkh + cluster_base per neighbor seed tile (Kubra shard varies by quadkey)
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
                self.entry_zoom,
            )
            for item in raw:
                feat = item.get("_raw") or {}
                if _is_cluster(feat):
                    cluster_queue.append((self.entry_zoom, item))
                else:
                    oid = item.get("id")
                    if oid and oid not in outages_by_id:
                        outages_by_id[oid] = item

        _dbg(
            self.debug,
            f"ENTRY FETCH complete: seed_tiles={len(seeds)} "
            f"(entry_zoom={self.entry_zoom}, layer={self.layer_name}, qkh={self.qkh_strategy}, layout={self.layout_name})",
        )

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


# -------------------------- CACHE HELPERS --------------------------

def _get_cached_profile() -> Optional[Dict[str, Any]]:
    global _cached_profile
    with _cache_lock:
        if not _cached_profile:
            return None
        age = time.time() - float(_cached_profile.get("ts", 0.0))
        if age > _CACHE_TTL_S:
            _cached_profile = None
            return None
        return dict(_cached_profile)


def _set_cached_profile(profile: Dict[str, Any]) -> None:
    global _cached_profile
    with _cache_lock:
        _cached_profile = dict(profile)


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
    """
    Primary provider function (contract-compatible with OG&E).
    Uses cached discovery/tile-scheme to avoid re-probing every request.
    """
    client = PSOKubraClient(debug=debug)

    prof = _get_cached_profile() if not debug else None  # debug forces fresh discovery
    if prof:
        client.stormcenter_id = prof.get("stormcenter_id")
        client.view_id = prof.get("view_id")
        client.cluster_template = prof.get("cluster_template")
        client.deployment_id = prof.get("deployment_id")
        client.cluster_layers = prof.get("cluster_layers") or []
        client.entry_zoom = prof.get("entry_zoom")
        client.layer_name = prof.get("layer_name")
        client.qkh_strategy = prof.get("qkh_strategy")
        client.layout_name = prof.get("layout_name")
    else:
        client.discover()
        _base, layer, strat, layout_name, z = client.probe_tile_scheme()

        _set_cached_profile(
            {
                "ts": time.time(),
                "stormcenter_id": client.stormcenter_id,
                "view_id": client.view_id,
                "cluster_template": client.cluster_template,
                "deployment_id": client.deployment_id,
                "cluster_layers": list(client.cluster_layers),
                "entry_zoom": z,
                "layer_name": layer,
                "qkh_strategy": strat,
                "layout_name": layout_name,
            }
        )

    return client.fetch_outages_near(
        lat=lat,
        lon=lon,
        max_radius_km=max_radius_km,
        max_zoom=max_zoom,
        neighbor_depth=neighbor_depth,
        drill_neighbor_depth=drill_neighbor_depth,
    )
