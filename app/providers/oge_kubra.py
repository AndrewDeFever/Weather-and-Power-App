"""
OG&E KUBRA Storm Center outage integration

HARD-CODED SCHEME (production-stable):
- layer_name: cluster-1
- entry_zoom: 11
- qkh: last3_rev (quadkey[-3:][::-1])
- layout: simple (..../public/<layer>/<quadkey>.json)

FAST-PATH (NOC) MODE:
- Prioritize "nearest outage" and a small set of leaf outages
- Hard caps and time budgets to keep request under router/origin constraints

Kubra high-level:
- currentState => cluster_interval_generation_data template (includes {qkh} sharding)
- tiles => file_data entries containing either clusters or leaf outage points
"""

from __future__ import annotations

import math
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import mercantile
import polyline
import requests

# -------------------------- OG&E IDENTIFIERS --------------------------

BASE_URL = "https://kubra.io/"
INSTANCE_ID = "dc85f79f-59f9-4e9e-9557-b3a9bee7e0ce"
VIEW_ID = "8fe9d356-96bc-41f1-b353-6720eb408936"

# -------------------------- HARD-CODED TILE SCHEME --------------------------

LAYER_NAME = "cluster-1"
ENTRY_ZOOM = 11


def _qkh_last3_rev(quadkey: str) -> str:
    return quadkey[-3:][::-1]


def _url_simple(cluster_data_path: str, layer: str, quadkey: str) -> str:
    # cluster_data_path already includes the sharded base (format-applied with qkh)
    return f"{BASE_URL}{cluster_data_path}/public/{layer}/{quadkey}.json"


# -------------------------- PERFORMANCE BUDGETS --------------------------
# Keep provider work below router timeout so we can return cleanly.
PROVIDER_TOTAL_BUDGET_S = 11.0

# Per-request timeouts
META_TIMEOUT_S = 4  # currentState
TILE_TIMEOUT_S = 3  # tile fetches

# Fast-mode hard caps (tuned for "nearest only")
MAX_TILE_FETCHES = 80
MAX_CLUSTER_DRILLS = 40
STOP_AFTER_LEAF = 25  # stop collecting after this many leaf outages

# -------------------------- NEIGHBORHOOD / DRILLING --------------------------

NEIGHBOR_DEPTH = 1
DEFAULT_MAX_ZOOM = 12
DEFAULT_DRILL_NEIGHBOR_DEPTH = 1


def _env_truthy(name: str) -> bool:
    v = os.getenv(name, "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


class OgeKubraClient:
    def __init__(self, debug: bool = False) -> None:
        self.session = requests.Session()
        self.debug = bool(debug) or _env_truthy("OGE_DEBUG")

        state = self._get_current_state()
        self.cluster_data_path_template = state["data"]["cluster_interval_generation_data"]

        # Apply hard-coded scheme
        self.layer_name = LAYER_NAME
        self.entry_zoom = ENTRY_ZOOM
        self._qkh_func = _qkh_last3_rev
        self._url_builder = _url_simple

        if self.debug:
            print("OGE HARD-CODED SCHEME ENABLED", flush=True)
            print("  layer_name:", self.layer_name, flush=True)
            print("  entry_zoom:", self.entry_zoom, flush=True)
            print("  qkh:", "last3_rev", flush=True)
            print("  layout:", "simple", flush=True)
            print("  cluster_data_path_template:", self.cluster_data_path_template, flush=True)

    def fetch_outages_for_point(
        self,
        lat: float,
        lon: float,
        max_radius_km: float = 50.0,
        max_zoom: int = DEFAULT_MAX_ZOOM,
        neighbor_depth: int = NEIGHBOR_DEPTH,
        drill_neighbor_depth: int = DEFAULT_DRILL_NEIGHBOR_DEPTH,
        *,
        fast: bool = True,
        stop_after_leaf: int = STOP_AFTER_LEAF,
        max_tile_fetches: int = MAX_TILE_FETCHES,
        max_cluster_drills: int = MAX_CLUSTER_DRILLS,
        time_budget_s: float = PROVIDER_TOTAL_BUDGET_S,
    ) -> Dict[str, Any]:
        if max_zoom < self.entry_zoom:
            raise ValueError(f"max_zoom ({max_zoom}) must be >= entry_zoom ({self.entry_zoom}).")

        t0 = time.time()

        def time_left() -> float:
            return time_budget_s - (time.time() - t0)

        base_tile = mercantile.tile(lon, lat, self.entry_zoom)
        base_q = mercantile.quadkey(base_tile)

        if self.debug:
            print(f"Base tile: {base_tile}", flush=True)
            print(f"Base quadkey (z={self.entry_zoom}): {base_q}", flush=True)

        seeds = self._expand_quadkeys(base_q, depth=neighbor_depth)

        outages_by_id: Dict[str, Dict[str, Any]] = {}
        seen_urls: Set[str] = set()
        seen_quadkeys: Set[Tuple[int, str]] = set()

        tile_fetches = 0
        cluster_drills = 0
        cluster_queue: List[Tuple[int, Dict[str, Any]]] = []  # (zoom, raw feature)

        def add_leaf(feat: Dict[str, Any]) -> None:
            o = self._normalize_outage(feat)
            if o:
                outages_by_id[o["id"]] = o

        # Seed crawl
        for q in seeds:
            if time_left() <= 0.5:
                break
            if tile_fetches >= max_tile_fetches:
                break

            raw_features, did_fetch = self._fetch_tile_features(q, self.entry_zoom, seen_urls, seen_quadkeys)
            if did_fetch:
                tile_fetches += 1

            for feat in raw_features:
                if self._is_cluster(feat):
                    cluster_queue.append((self.entry_zoom, feat))
                else:
                    add_leaf(feat)

            if fast and len(outages_by_id) >= stop_after_leaf:
                break

        # Drill clusters (bounded)
        while cluster_queue:
            if time_left() <= 0.5:
                break
            if tile_fetches >= max_tile_fetches:
                break
            if cluster_drills >= max_cluster_drills:
                break

            z, cluster_feat = cluster_queue.pop(0)

            if z >= max_zoom:
                add_leaf(cluster_feat)
                if fast and len(outages_by_id) >= stop_after_leaf:
                    break
                continue

            loc = self._extract_location(cluster_feat)
            if not loc:
                add_leaf(cluster_feat)
                if fast and len(outages_by_id) >= stop_after_leaf:
                    break
                continue

            clat, clon = loc
            child_z = z + 1
            child_tile = mercantile.tile(clon, clat, child_z)
            child_q = mercantile.quadkey(child_tile)

            child_keys = self._expand_quadkeys(child_q, depth=drill_neighbor_depth)
            cluster_drills += 1

            if self.debug:
                print(
                    f"DRILL cluster z={z} -> z={child_z}, center_q={child_q}, neighborhood={len(child_keys)} tiles",
                    flush=True,
                )

            for cq in child_keys:
                if time_left() <= 0.5:
                    break
                if tile_fetches >= max_tile_fetches:
                    break

                raw_features, did_fetch = self._fetch_tile_features(cq, child_z, seen_urls, seen_quadkeys)
                if did_fetch:
                    tile_fetches += 1

                for feat in raw_features:
                    if self._is_cluster(feat):
                        cluster_queue.append((child_z, feat))
                    else:
                        add_leaf(feat)

                if fast and len(outages_by_id) >= stop_after_leaf:
                    break

            if fast and len(outages_by_id) >= stop_after_leaf:
                break

        if not outages_by_id:
            return {"nearest": None, "outages": []}

        # Attach distance and sort
        enriched: List[Dict[str, Any]] = []
        for o in outages_by_id.values():
            d = haversine_km(lat, lon, o["latitude"], o["longitude"])
            oo = dict(o)
            oo["distance_km"] = d
            enriched.append(oo)

        enriched.sort(key=lambda x: x["distance_km"])

        nearest = enriched[0] if enriched else None
        within = [o for o in enriched if o["distance_km"] <= max_radius_km]

        if nearest and nearest["distance_km"] > max_radius_km:
            nearest = None

        if fast:
            within = within[:stop_after_leaf]

        return {"nearest": nearest, "outages": within}

    def _fetch_tile_features(
        self,
        quadkey: str,
        zoom: int,
        seen_urls: Set[str],
        seen_quadkeys: Set[Tuple[int, str]],
    ) -> Tuple[List[Dict[str, Any]], bool]:
        key = (zoom, quadkey)
        if key in seen_quadkeys:
            return [], False
        seen_quadkeys.add(key)

        qkh = self._qkh_func(quadkey)
        cluster_data_path = self.cluster_data_path_template.format(qkh=qkh)
        url = self._url_builder(cluster_data_path, self.layer_name, quadkey)

        if url in seen_urls:
            return [], False
        seen_urls.add(url)

        if self.debug:
            print(f"FETCH z={zoom} q={quadkey} -> {url}", flush=True)

        try:
            r = self.session.get(url, timeout=TILE_TIMEOUT_S)
        except requests.RequestException:
            return [], True

        if r.status_code != 200:
            return [], True

        try:
            tile = r.json()
        except ValueError:
            return [], True

        feats = tile.get("file_data", [])
        if self.debug:
            print(f"  -> features: {len(feats)}", flush=True)
        return feats, True

    @staticmethod
    def _is_cluster(feature: Dict[str, Any]) -> bool:
        desc = feature.get("desc", {}) or {}
        return bool(desc.get("cluster"))

    def _extract_location(self, feature: Dict[str, Any]) -> Optional[Tuple[float, float]]:
        geom = feature.get("geom", {}) or {}
        pts = geom.get("p", []) or []
        if not pts:
            return None
        try:
            return polyline.decode(pts[0])[0]
        except Exception:
            return None

    def _normalize_outage(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        desc = feature.get("desc", {}) or {}
        loc = self._extract_location(feature)
        if not loc:
            return None

        lat, lon = loc[0], loc[1]

        inc_id = desc.get("inc_id")
        outage_id = inc_id if inc_id else f"{loc}-{desc.get('start_time', 'unknown')}"
        canonical_id = inc_id if inc_id else f"{round(lat, 5)}|{round(lon, 5)}"

        cause = desc.get("cause")
        if isinstance(cause, dict):
            cause = cause.get("EN-US") or next(iter(cause.values()), None)

        customers_out = None
        cust_a = desc.get("cust_a")
        if isinstance(cust_a, dict):
            customers_out = cust_a.get("val")

        return {
            "id": outage_id,
            "canonical_id": canonical_id,
            "cluster": bool(desc.get("cluster")),
            "customers_out": customers_out,
            "n_out": desc.get("n_out"),
            "etr": desc.get("etr"),
            "etr_confidence": desc.get("etr_confidence"),
            "cause": cause,
            "comments": desc.get("comments"),
            "crew_status": desc.get("crew_status"),
            "start_time": desc.get("start_time"),
            "latitude": lat,
            "longitude": lon,
        }

    def _expand_quadkeys(self, base_quadkey: str, depth: int) -> List[str]:
        t = mercantile.quadkey_to_tile(base_quadkey)
        out: List[str] = []
        seen: Set[str] = set()
        for dx in range(-depth, depth + 1):
            for dy in range(-depth, depth + 1):
                q = mercantile.quadkey(mercantile.Tile(t.x + dx, t.y + dy, t.z))
                if q not in seen:
                    seen.add(q)
                    out.append(q)
        return out

    def _get_current_state(self) -> Dict[str, Any]:
        url = (
            f"{BASE_URL}stormcenter/api/v1/stormcenters/"
            f"{INSTANCE_ID}/views/{VIEW_ID}/currentState?preview=false"
        )
        r = self.session.get(url, timeout=META_TIMEOUT_S)
        r.raise_for_status()
        return r.json()


# -------------------------- UTIL --------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# -------------------------- CLIENT CACHE --------------------------

_CLIENT: Optional["OgeKubraClient"] = None
_CLIENT_TS: float = 0.0
_CLIENT_TTL_S: int = 600  # 10 minutes


def _get_client(debug: bool = False) -> "OgeKubraClient":
    global _CLIENT, _CLIENT_TS
    now = time.time()
    debug_effective = bool(debug) or _env_truthy("OGE_DEBUG")

    if _CLIENT is None or (now - _CLIENT_TS) > _CLIENT_TTL_S:
        _CLIENT = OgeKubraClient(debug=debug_effective)
        _CLIENT_TS = now
    else:
        if debug_effective:
            _CLIENT.debug = True
    return _CLIENT


def fetch_oge_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 50.0,
    max_zoom: int = DEFAULT_MAX_ZOOM,
    neighbor_depth: int = NEIGHBOR_DEPTH,
    drill_neighbor_depth: int = DEFAULT_DRILL_NEIGHBOR_DEPTH,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    NOC default: fast=True (nearest-focused).
    """
    client = _get_client(debug=debug)
    return client.fetch_outages_for_point(
        lat,
        lon,
        max_radius_km=max_radius_km,
        max_zoom=max_zoom,
        neighbor_depth=neighbor_depth,
        drill_neighbor_depth=drill_neighbor_depth,
        fast=True,
        stop_after_leaf=STOP_AFTER_LEAF,
        max_tile_fetches=MAX_TILE_FETCHES,
        max_cluster_drills=MAX_CLUSTER_DRILLS,
        time_budget_s=PROVIDER_TOTAL_BUDGET_S,
    )


if __name__ == "__main__":
    print("Testing OG&E outage fetch (debug on, fast mode)...")
    os.environ["OGE_DEBUG"] = "1"
    try:
        res = fetch_oge_outages(
            35.4676,
            -97.5164,
            max_radius_km=50.0,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            debug=True,
        )
        print("NEAREST:", res["nearest"])
        print("COUNT (within radius):", len(res["outages"]))
        for o in res["outages"][:5]:
            print(" -", o["id"], "cust_out=", o["customers_out"], "km=", round(o["distance_km"], 3))
    except Exception as e:
        print("ERROR:", e)
