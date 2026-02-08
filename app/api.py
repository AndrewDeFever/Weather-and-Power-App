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

# -------------------------- DISCOVERY CACHE --------------------------
# PSO requires a relatively expensive discovery + tile-scheme probe (multiple HTTP calls).
# Cache those derived parameters for a short TTL to keep API latency predictable.
_CACHE_TTL_S = 300  # 5 minutes
_cache_lock = threading.Lock()
_cached_profile: Optional[Dict[str, Any]] = None  # {"ts": float, ...fields...}


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


CT = ZoneInfo("America/Chicago")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def to_iso_ct(dt_str: Optional[str]) -> Optional[str]:
    if not dt_str:
        return None
    try:
        # Many Kubra endpoints return ISO-ish strings. Best-effort parse.
        # Example: "2024-01-01T12:34:56Z" or "...-06:00"
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(CT).isoformat()
    except Exception:
        return dt_str


class PSOKubraClient:
    """
    Client that discovers:
    - stormcenter_id + view_id from landing HTML/JS
    - currentState -> deploymentId, clusterTemplate
    - config -> cluster layer options
    - tile scheme (layer name, zoom, qkh strategy, layout)
    """

    BASE = "https://pso.uat.kubra.io"  # note: may redirect in real env; discovery handles it
    LANDING = "https://outagemap.psoklahoma.com"  # PSO outage map entry point

    def __init__(self, debug: bool = False):
        self.debug = debug
        self.s = requests.Session()
        self.s.headers.update(
            {
                "User-Agent": "NOCTriage/1.0 (weather-power-status)",
                "Accept": "*/*",
            }
        )

        # Discovered
        self.stormcenter_id: Optional[str] = None
        self.view_id: Optional[str] = None
        self.cluster_template: Optional[str] = None
        self.deployment_id: Optional[str] = None
        self.cluster_layers: List[str] = []

        # Tile scheme
        self.entry_zoom: Optional[int] = None
        self.layer_name: Optional[str] = None
        self.qkh_strategy: Optional[str] = None
        self.layout_name: Optional[str] = None

    def log(self, *args: Any) -> None:
        if self.debug:
            print("[PSO]", *args)

    def discover(self) -> None:
        """
        Scrape stormcenter_id & view_id, then resolve currentState to obtain deploymentId + clusterTemplate,
        then fetch configuration to list possible cluster layers.
        """
        self.log("Discover: landing", self.LANDING)
        r = self.s.get(self.LANDING, timeout=10)
        r.raise_for_status()
        html = r.text

        # Find IDs in JS/config snippets
        m_sc = re.search(r"stormcenterId\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"", html)
        m_view = re.search(r"viewId\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"", html)

        if not m_sc or not m_view:
            # Try alternate patterns
            m_sc = re.search(r"stormcenterId\\s*[:=]\\s*\\\"([^\\\"]+)\\\"", html)
            m_view = re.search(r"viewId\\s*[:=]\\s*\\\"([^\\\"]+)\\\"", html)

        if not m_sc or not m_view:
            raise RuntimeError("Unable to discover stormcenterId/viewId from landing page")

        self.stormcenter_id = m_sc.group(1)
        self.view_id = m_view.group(1)
        self.log("stormcenter_id", self.stormcenter_id, "view_id", self.view_id)

        # currentState endpoint (common Kubra pattern)
        state_url = f"https://stormcenter.pso.uat.kubra.io/stormcenter/{self.stormcenter_id}/views/{self.view_id}/currentState"
        self.log("Discover: currentState", state_url)
        sr = self.s.get(state_url, timeout=10)
        sr.raise_for_status()
        state = sr.json()

        self.deployment_id = state.get("deploymentId") or state.get("deploymentID")
        self.cluster_template = state.get("clusterTemplate") or state.get("cluster_template")
        self.log("deployment_id", self.deployment_id, "cluster_template", self.cluster_template)

        # Config endpoint to list layers
        cfg_url = f"https://stormcenter.pso.uat.kubra.io/stormcenter/{self.stormcenter_id}/views/{self.view_id}/configuration"
        self.log("Discover: configuration", cfg_url)
        cr = self.s.get(cfg_url, timeout=10)
        cr.raise_for_status()
        cfg = cr.json()

        # Best-effort layer list extraction
        layers = []
        try:
            layers = cfg.get("layers") or []
        except Exception:
            layers = []

        self.cluster_layers = []
        for lyr in layers:
            name = lyr.get("name") if isinstance(lyr, dict) else None
            if name and "cluster" in name.lower():
                self.cluster_layers.append(name)

        self.log("cluster_layers", self.cluster_layers)

    def probe_tile_scheme(self) -> Tuple[str, str, str, str, int]:
        """
        Determine which layer/zoom/qkh strategy/layout responds for this provider.

        Returns:
          (base_url, layer_name, qkh_strategy, layout_name, entry_zoom)
        """
        if not self.stormcenter_id or not self.view_id:
            raise RuntimeError("discover() must be called first")

        # Base map tile endpoint tends to be under stormcenter.* domain.
        base_url = f"https://stormcenter.pso.uat.kubra.io/stormcenter/{self.stormcenter_id}/views/{self.view_id}/tiles"

        # Candidate strategies / layouts - can evolve by provider.
        qkh_strategies = ["qkh", "quadkeyhash", "quadkey"]
        layouts = ["public", "default", "layout"]
        layer_candidates = self.cluster_layers or ["cluster", "clusters"]

        # Start at a reasonable zoom and test downward if needed.
        zoom_candidates = list(range(12, 6, -1))

        lat_test, lon_test = 36.15398, -95.99277  # Tulsa-ish as probe point
        t = mercantile.tile(lon_test, lat_test, 10)

        for z in zoom_candidates:
            t = mercantile.tile(lon_test, lat_test, z)
            for layer_name in layer_candidates:
                for strat in qkh_strategies:
                    for layout in layouts:
                        url = f"{base_url}/{layout}/{layer_name}/{z}/{t.x}/{t.y}.json"
                        try:
                            rr = self.s.get(url, timeout=5)
                            if rr.status_code == 200 and rr.headers.get("content-type", "").startswith("application/json"):
                                self.entry_zoom = z
                                self.layer_name = layer_name
                                self.qkh_strategy = strat
                                self.layout_name = layout
                                self.log("Tile scheme OK:", url)
                                return (base_url, layer_name, strat, layout, z)
                        except Exception:
                            continue

        raise RuntimeError("Unable to probe PSO tile scheme")

    def _tile_url(self, z: int, x: int, y: int) -> str:
        if not self.stormcenter_id or not self.view_id:
            raise RuntimeError("discover() must be called first")
        if self.layout_name is None or self.layer_name is None:
            raise RuntimeError("probe_tile_scheme() must be called first")

        base_url = f"https://stormcenter.pso.uat.kubra.io/stormcenter/{self.stormcenter_id}/views/{self.view_id}/tiles"
        return f"{base_url}/{self.layout_name}/{self.layer_name}/{z}/{x}/{y}.json"

    def _fetch_tile(self, z: int, x: int, y: int) -> List[Dict[str, Any]]:
        url = self._tile_url(z, x, y)
        rr = self.s.get(url, timeout=8)
        if rr.status_code != 200:
            return []
        try:
            data = rr.json()
        except Exception:
            return []

        # Kubra tile payloads vary. We normalize to a list of "outage-ish" dicts.
        if isinstance(data, dict) and "features" in data:
            feats = data.get("features") or []
            out = []
            for f in feats:
                props = f.get("properties") or {}
                geom = f.get("geometry") or {}
                coords = (geom.get("coordinates") or [None, None])
                if isinstance(coords, list) and len(coords) >= 2:
                    lon, lat = coords[0], coords[1]
                else:
                    lon, lat = None, None
                props["longitude"] = props.get("longitude", lon)
                props["latitude"] = props.get("latitude", lat)
                out.append(props)
            return out

        if isinstance(data, list):
            return data

        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            return data["data"]

        return []

    def fetch_outages_near(
        self,
        lat: float,
        lon: float,
        max_radius_km: float = 50.0,
        max_zoom: int = 12,
        neighbor_depth: int = 1,
        drill_neighbor_depth: int = 1,
    ) -> Dict[str, Any]:
        """
        Fetch tiles around the point and return outages plus nearest outage.

        neighbor_depth: number of tiles around main tile to fetch (square neighborhood).
        drill_neighbor_depth: optional second-level neighborhood around any found clusters (future enhancement).
        """
        if self.entry_zoom is None:
            raise RuntimeError("probe_tile_scheme() must be called first")

        z = min(max_zoom, int(self.entry_zoom))

        t = mercantile.tile(lon, lat, z)

        tiles: Set[Tuple[int, int, int]] = set()
        for dx in range(-neighbor_depth, neighbor_depth + 1):
            for dy in range(-neighbor_depth, neighbor_depth + 1):
                tiles.add((z, t.x + dx, t.y + dy))

        outages: List[Dict[str, Any]] = []
        for (tz, tx, ty) in tiles:
            outages.extend(self._fetch_tile(tz, tx, ty))

        # Normalize + compute nearest
        nearest = None
        best_d = None

        normalized: List[Dict[str, Any]] = []
        for o in outages:
            try:
                olat = float(o.get("latitude"))
                olon = float(o.get("longitude"))
            except Exception:
                continue

            d = haversine_km(lat, lon, olat, olon)
            if d > max_radius_km:
                continue

            norm = {
                "id": o.get("id") or o.get("ID") or o.get("outageId") or o.get("outage_id"),
                "cluster": bool(o.get("cluster") or o.get("isCluster") or False),
                "customers_out": o.get("customersOut") or o.get("customers_out") or o.get("custOut"),
                "n_out": o.get("nOut") or o.get("n_out"),
                "etr": to_iso_ct(o.get("etr") or o.get("estimatedRestoration")),
                "etr_confidence": o.get("etrConfidence") or o.get("etr_confidence"),
                "cause": o.get("cause"),
                "comments": o.get("comments"),
                "crew_status": o.get("crewStatus") or o.get("crew_status"),
                "start_time": to_iso_ct(o.get("startTime") or o.get("start_time")),
                "latitude": olat,
                "longitude": olon,
                "distance_km": d,
            }
            normalized.append(norm)

            if best_d is None or d < best_d:
                best_d = d
                nearest = norm

        normalized.sort(key=lambda x: x.get("distance_km") or 1e9)

        return {"nearest": nearest, "outages": normalized}


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

    Optimization:
    - Cache the discovery + tile-scheme probe outputs for a short TTL so most requests only do tile fetches.
    - If debug=True, cache is bypassed to make troubleshooting deterministic.
    """
    client = PSOKubraClient(debug=debug)

    prof = _get_cached_profile() if not debug else None
    if prof:
        # Inject cached discovery/probe results
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
        _base_url, layer_name, qkh_strategy, layout_name, entry_zoom = client.probe_tile_scheme()

        _set_cached_profile(
            {
                "ts": time.time(),
                "stormcenter_id": client.stormcenter_id,
                "view_id": client.view_id,
                "cluster_template": client.cluster_template,
                "deployment_id": client.deployment_id,
                "cluster_layers": list(client.cluster_layers),
                "entry_zoom": entry_zoom,
                "layer_name": layer_name,
                "qkh_strategy": qkh_strategy,
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
            print("Nearest distance_km:", n.get("distance_km"))
        else:
            print("No nearest outage found within radius.")
    except Exception as e:
        print("ERROR:", repr(e))
